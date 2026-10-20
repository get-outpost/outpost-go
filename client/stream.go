package client

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"sync"
	"time"

	"github.com/coder/websocket"
	"github.com/get-outpost/outpost-go"
)

const (
	minReconnectDelay = 200 * time.Millisecond
	maxReconnectDelay = 5 * time.Second
)

// Stream is a resumable WebSocket subscription. Its lifetime is bounded by the
// context passed to Client.Stream or by Close. At most one caller may call Read
// at a time; Close may be called concurrently with Read.
type Stream struct {
	client *Client
	ctx    context.Context
	cancel context.CancelFunc

	mu   sync.Mutex
	conn *websocket.Conn

	// Only Read owns the cursor, retry delay, and terminal handshake error.
	after    int64
	delay    time.Duration
	terminal error
}

// Stream opens a subscription after an exclusive account sequence cursor.
// after must be nonnegative. The initial connection attempt is bounded by
// RequestTimeout and returns connection errors directly. Once connected, Read
// reconnects automatically until ctx is canceled, Close is called, or the relay
// rejects a reconnect with HTTP 401 or 403.
func (c *Client) Stream(ctx context.Context, after int64) (*Stream, error) {
	if after < 0 {
		return nil, fmt.Errorf("after must be nonnegative")
	}
	streamCtx, cancel := context.WithCancel(ctx)
	s := &Stream{client: c, ctx: streamCtx, cancel: cancel, after: after, delay: minReconnectDelay}
	// Cancellation must release an idle connection even when nobody is in Read.
	context.AfterFunc(streamCtx, func() { _ = s.closeConn() })
	conn, err := c.dialStream(streamCtx, after)
	if err != nil {
		cancel()
		return nil, err
	}
	if !s.setConn(conn) {
		_ = conn.CloseNow()
		cancel()
		return nil, streamCtx.Err()
	}
	return s, nil
}

func (c *Client) dialStream(ctx context.Context, after int64) (*websocket.Conn, error) {
	u := c.endpoint("/v1/ws")
	if u.Scheme == "https" {
		u.Scheme = "wss"
	} else {
		u.Scheme = "ws"
	}
	u.RawQuery = url.Values{"after": {strconv.FormatInt(after, 10)}}.Encode()
	dialCtx, cancel := context.WithTimeout(ctx, RequestTimeout)
	defer cancel()
	conn, resp, err := websocket.Dial(dialCtx, u.String(), &websocket.DialOptions{
		HTTPClient: c.http, HTTPHeader: http.Header{"Authorization": {"Bearer " + c.token}},
	})
	if err != nil {
		if resp != nil {
			defer resp.Body.Close()
			return nil, c.responseError(resp)
		}
		return nil, err
	}
	// JSON can expand payload characters into six-byte escapes. The incoming
	// payload cap is not the encoded envelope's wire-size cap.
	conn.SetReadLimit(6 * (outpost.MaxPayloadSize + 16*1024))
	return conn, nil
}

// Read returns the next frame, preserving ready, ack, error, and unknown frame
// kinds. Only returned event frames with a nonnil Event advance the resume
// cursor; event sequences at or below that cursor are suppressed. Ready heads
// and acknowledgement sequences never advance it.
//
// Connection failures retry with a cancellation-aware exponential delay from
// 200ms to 5s, reset by a delivered event. HTTP 401 and 403 are terminal: this and
// subsequent calls return the APIError without further connection attempts.
// Malformed JSON and non-text messages return an error without retrying that
// Read. A relay error frame is returned as data, not converted to a Go error.
//
// Canceling ctx interrupts this call and returns its context error. A later Read
// may resume with another context, unless the stream's lifetime context was also
// canceled. Close interrupts blocked reads with context.Canceled.
func (s *Stream) Read(ctx context.Context) (outpost.Frame, error) {
	var zero outpost.Frame
	if err := s.contextError(ctx); err != nil {
		return zero, err
	}
	if s.terminal != nil {
		return zero, s.terminal
	}
	readCtx, cancel := context.WithCancel(ctx)
	stop := context.AfterFunc(s.ctx, cancel)
	defer stop()
	defer cancel()
	for {
		if err := s.contextError(readCtx); err != nil {
			return zero, err
		}
		conn := s.getConn()
		if conn == nil {
			if err := s.waitReconnect(readCtx); err != nil {
				return zero, err
			}
			var err error
			conn, err = s.client.dialStream(readCtx, s.after)
			if contextErr := s.contextError(readCtx); contextErr != nil {
				if conn != nil {
					_ = conn.CloseNow()
				}
				return zero, contextErr
			}
			if err != nil {
				var apiError *APIError
				if errors.As(err, &apiError) && (apiError.StatusCode == http.StatusUnauthorized || apiError.StatusCode == http.StatusForbidden) {
					s.terminal = err
					return zero, err
				}
				continue
			}
			if !s.setConn(conn) {
				_ = conn.CloseNow()
				return zero, s.ctx.Err()
			}
		}
		typ, data, err := conn.Read(readCtx)
		if err != nil {
			s.discardConn(conn)
			if contextErr := s.contextError(readCtx); contextErr != nil {
				return zero, contextErr
			}
			continue
		}
		if typ != websocket.MessageText {
			return zero, fmt.Errorf("expected JSON text frame")
		}
		var frame outpost.Frame
		if err := json.Unmarshal(data, &frame); err != nil {
			return zero, err
		}
		if frame.Kind == outpost.FrameEvent && frame.Event != nil {
			if frame.Event.Seq <= s.after {
				continue
			}
			s.after = frame.Event.Seq
			s.delay = minReconnectDelay
		}
		return frame, nil
	}
}

func (s *Stream) contextError(ctx context.Context) error {
	if err := s.ctx.Err(); err != nil {
		return err
	}
	return ctx.Err()
}

func (s *Stream) waitReconnect(ctx context.Context) error {
	timer := time.NewTimer(s.delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return s.contextError(ctx)
	case <-s.ctx.Done():
		return s.ctx.Err()
	case <-timer.C:
		s.delay = min(s.delay*2, maxReconnectDelay)
		return nil
	}
}

func (s *Stream) getConn() *websocket.Conn {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.conn
}

func (s *Stream) setConn(conn *websocket.Conn) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.ctx.Err() != nil {
		return false
	}
	s.conn = conn
	return true
}

func (s *Stream) discardConn(conn *websocket.Conn) {
	s.mu.Lock()
	if s.conn != conn {
		s.mu.Unlock()
		return
	}
	s.conn = nil
	s.mu.Unlock()
	_ = conn.CloseNow()
}

func (s *Stream) closeConn() error {
	s.mu.Lock()
	conn := s.conn
	s.conn = nil
	s.mu.Unlock()
	if conn != nil {
		return conn.CloseNow()
	}
	return nil
}

// Close cancels the subscription and promptly releases its current connection
// without waiting for a WebSocket close handshake. It is safe to call more than
// once or concurrently with Read; no reconnect is attempted after Close.
func (s *Stream) Close() error {
	s.cancel()
	return s.closeConn()
}
