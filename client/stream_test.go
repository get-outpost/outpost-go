package client_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/get-outpost/outpost-go"
	"github.com/get-outpost/outpost-go/client"
)

func writeFrame(t *testing.T, ctx context.Context, conn *websocket.Conn, frame outpost.Frame) bool {
	t.Helper()
	data, err := json.Marshal(frame)
	if err != nil {
		t.Error(err)
		return false
	}
	return conn.Write(ctx, websocket.MessageText, data) == nil
}

func TestStreamResumesDeliveredCursorAndPreservesFrames(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var calls atomic.Int32
	cursors := make(chan string, 4)
	head, laterHead := int64(99), int64(999)
	ackID := "publication"
	first := outpost.Envelope{ID: "six", Seq: 6, Type: "message.created", Payload: json.RawMessage(`{"text":"hello"}`)}
	duplicate := first
	duplicate.Seq = 5
	second := first
	second.ID, second.Seq = "seven", 7
	ack := first
	ack.Seq = 500
	firstFrames := []outpost.Frame{
		{Kind: outpost.FrameReady, HeadSeq: &head},
		{Kind: outpost.FrameAck, ID: &ackID, Event: &ack},
		{Kind: outpost.FrameError, Error: "publication rejected"},
		{Kind: "future_frame"},
		{Kind: outpost.FrameEvent}, // Missing events are data, not delivery cursors.
		{Kind: outpost.FrameEvent, Event: &first},
	}
	secondFrames := []outpost.Frame{
		{Kind: outpost.FrameReady, HeadSeq: &laterHead},
		{Kind: outpost.FrameEvent, Event: &first},
		{Kind: outpost.FrameEvent, Event: &duplicate},
		{Kind: outpost.FrameAck, ID: &ackID, Event: &ack},
		{Kind: outpost.FrameEvent, Event: &second},
	}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/relay/v1/ws" || r.Header.Get("Authorization") != "Bearer op_fixture" {
			t.Errorf("stream request = %s, authorization = %q", r.URL, r.Header.Get("Authorization"))
		}
		cursors <- r.URL.Query().Get("after")
		attempt := calls.Add(1)
		conn, err := websocket.Accept(w, r, nil)
		if err != nil {
			return
		}
		defer conn.CloseNow()
		frames := firstFrames
		if attempt > 1 {
			frames = secondFrames
		}
		for _, frame := range frames {
			if !writeFrame(t, ctx, conn, frame) {
				return
			}
		}
	}))
	defer srv.Close()
	stream, err := newClient(t, srv.URL+"/relay").Stream(ctx, 5)
	if err != nil {
		t.Fatal(err)
	}
	defer stream.Close()
	want := append(append([]outpost.Frame{}, firstFrames...), secondFrames[0], secondFrames[3], secondFrames[4])
	for i, expected := range want {
		frame, err := stream.Read(ctx)
		if err != nil || !reflect.DeepEqual(frame, expected) {
			t.Fatalf("Read[%d] = %#v, %v; want %#v", i, frame, err, expected)
		}
	}
	if calls.Load() != 2 {
		t.Fatalf("connection attempts = %d, want 2", calls.Load())
	}
	if firstCursor, secondCursor := <-cursors, <-cursors; firstCursor != "5" || secondCursor != "6" {
		t.Fatalf("resume cursors = %q, %q; want 5, 6", firstCursor, secondCursor)
	}
}

func TestStreamRetriesTransientHandshakeFailure(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var calls atomic.Int32
	cursors := make(chan string, 4)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		cursors <- r.URL.Query().Get("after")
		attempt := calls.Add(1)
		if attempt == 2 {
			w.WriteHeader(http.StatusServiceUnavailable)
			_, _ = io.WriteString(w, `{"error":"restarting"}`)
			return
		}
		conn, err := websocket.Accept(w, r, nil)
		if err != nil {
			return
		}
		defer conn.CloseNow()
		if attempt == 1 {
			head := int64(900)
			writeFrame(t, ctx, conn, outpost.Frame{Kind: outpost.FrameReady, HeadSeq: &head})
			return
		}
		writeFrame(t, ctx, conn, outpost.Frame{Kind: outpost.FrameEvent, Event: &outpost.Envelope{Seq: 1}})
	}))
	defer srv.Close()
	stream, err := newClient(t, srv.URL).Stream(ctx, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer stream.Close()
	if frame, err := stream.Read(ctx); err != nil || frame.Kind != outpost.FrameReady {
		t.Fatalf("ready = %+v, %v", frame, err)
	}
	frame, err := stream.Read(ctx)
	if err != nil || frame.Event == nil || frame.Event.Seq != 1 || calls.Load() != 3 {
		t.Fatalf("recovered frame = %+v, %v, attempts=%d", frame, err, calls.Load())
	}
	for range 3 {
		if cursor := <-cursors; cursor != "0" {
			t.Fatalf("ready or failed connection advanced cursor to %q", cursor)
		}
	}
}

func TestStreamAuthenticationFailuresStop(t *testing.T) {
	for _, status := range []int{http.StatusUnauthorized, http.StatusForbidden} {
		for _, reconnect := range []bool{false, true} {
			t.Run(fmt.Sprintf("status_%d_reconnect_%v", status, reconnect), func(t *testing.T) {
				ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
				defer cancel()
				var calls atomic.Int32
				srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					attempt := calls.Add(1)
					if reconnect && attempt == 1 {
						conn, err := websocket.Accept(w, r, nil)
						if err != nil {
							return
						}
						defer conn.CloseNow()
						writeFrame(t, ctx, conn, outpost.Frame{Kind: outpost.FrameEvent, Event: &outpost.Envelope{Seq: 1}})
						return
					}
					w.WriteHeader(status)
					_, _ = io.WriteString(w, `{"error":"invalid op_fixture"}`)
				}))
				defer srv.Close()
				stream, err := newClient(t, srv.URL).Stream(ctx, 0)
				wantCalls := int32(1)
				if reconnect {
					if err != nil {
						t.Fatal(err)
					}
					defer stream.Close()
					if _, err := stream.Read(ctx); err != nil {
						t.Fatal(err)
					}
					_, err = stream.Read(ctx)
					wantCalls = 2
				}
				var apiError *client.APIError
				if !errors.As(err, &apiError) || apiError.StatusCode != status || apiError.Message != "invalid [redacted]" || calls.Load() != wantCalls {
					t.Fatalf("authentication error = %v, attempts=%d", err, calls.Load())
				}
				if reconnect {
					_, again := stream.Read(ctx)
					if again != err || calls.Load() != wantCalls {
						t.Fatalf("terminal failure retried: %v, attempts=%d", again, calls.Load())
					}
				}
			})
		}
	}
}

func TestReadCancellationCanResume(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	var calls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		attempt := calls.Add(1)
		if r.URL.Query().Get("after") != "0" {
			t.Errorf("canceled read advanced cursor: %s", r.URL)
		}
		conn, err := websocket.Accept(w, r, nil)
		if err != nil {
			return
		}
		defer conn.CloseNow()
		if attempt == 1 {
			_, _, _ = conn.Read(ctx)
			return
		}
		writeFrame(t, ctx, conn, outpost.Frame{Kind: outpost.FrameEvent, Event: &outpost.Envelope{Seq: 1}})
	}))
	defer srv.Close()
	stream, err := newClient(t, srv.URL).Stream(ctx, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer stream.Close()
	readCtx, stopRead := context.WithTimeout(ctx, 30*time.Millisecond)
	defer stopRead()
	if _, err := stream.Read(readCtx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("canceled Read = %v", err)
	}
	frame, err := stream.Read(ctx)
	if err != nil || frame.Event == nil || frame.Event.Seq != 1 || calls.Load() != 2 {
		t.Fatalf("resumed Read = %+v, %v, attempts=%d", frame, err, calls.Load())
	}
}

func TestStreamCancellationInterruptsBackoff(t *testing.T) {
	for _, lifetime := range []bool{false, true} {
		t.Run(fmt.Sprintf("lifetime_%v", lifetime), func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			streamCtx, cancelStream := context.WithCancel(ctx)
			defer cancelStream()
			var calls atomic.Int32
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls.Add(1)
				conn, err := websocket.Accept(w, r, nil)
				if err != nil {
					return
				}
				defer conn.CloseNow()
				writeFrame(t, ctx, conn, outpost.Frame{Kind: outpost.FrameReady})
			}))
			defer srv.Close()
			stream, err := newClient(t, srv.URL).Stream(streamCtx, 0)
			if err != nil {
				t.Fatal(err)
			}
			defer stream.Close()
			if _, err := stream.Read(ctx); err != nil {
				t.Fatal(err)
			}
			readCtx, cancelRead := context.WithCancel(ctx)
			defer cancelRead()
			cancelTarget := cancelRead
			if lifetime {
				cancelTarget = cancelStream
			}
			timer := time.AfterFunc(30*time.Millisecond, cancelTarget)
			defer timer.Stop()
			if _, err := stream.Read(readCtx); !errors.Is(err, context.Canceled) {
				t.Fatalf("canceled backoff returned %v", err)
			}
			if calls.Load() != 1 {
				t.Fatalf("cancellation allowed %d connection attempts", calls.Load())
			}
		})
	}
}

func TestStreamCloseInterruptsReadAndReconnectDial(t *testing.T) {
	for _, reconnect := range []bool{false, true} {
		t.Run(fmt.Sprintf("reconnect_%v", reconnect), func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			var calls atomic.Int32
			dialing := make(chan struct{}, 1)
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				attempt := calls.Add(1)
				if reconnect && attempt > 1 {
					dialing <- struct{}{}
					select {
					case <-r.Context().Done():
					case <-ctx.Done():
					}
					return
				}
				conn, err := websocket.Accept(w, r, nil)
				if err != nil {
					return
				}
				defer conn.CloseNow()
				if reconnect {
					writeFrame(t, ctx, conn, outpost.Frame{Kind: outpost.FrameReady})
					return
				}
				_, _, _ = conn.Read(ctx)
			}))
			defer srv.Close()
			stream, err := newClient(t, srv.URL).Stream(ctx, 0)
			if err != nil {
				t.Fatal(err)
			}
			defer stream.Close()
			if reconnect {
				if _, err := stream.Read(ctx); err != nil {
					t.Fatal(err)
				}
			}
			done := make(chan error, 1)
			go func() {
				_, err := stream.Read(context.Background())
				done <- err
			}()
			if reconnect {
				select {
				case <-dialing:
				case <-ctx.Done():
					t.Fatal("stream did not begin reconnecting")
				}
			}
			_ = stream.Close()
			select {
			case err := <-done:
				if !errors.Is(err, context.Canceled) {
					t.Fatalf("Read after Close = %v", err)
				}
			case <-time.After(500 * time.Millisecond):
				t.Fatal("Close did not interrupt Read promptly")
			}
			_ = stream.Close()
			if _, err := stream.Read(context.Background()); !errors.Is(err, context.Canceled) {
				t.Fatalf("future Read after Close = %v", err)
			}
			wantCalls := int32(1)
			if reconnect {
				wantCalls = 2
			}
			if calls.Load() != wantCalls {
				t.Fatalf("Close caused additional connections: %d", calls.Load())
			}
		})
	}
}

func TestStreamLifetimeCancellationClosesIdleSocket(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	streamCtx, cancelStream := context.WithCancel(ctx)
	defer cancelStream()
	closed := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := websocket.Accept(w, r, nil)
		if err != nil {
			return
		}
		defer conn.CloseNow()
		_, _, _ = conn.Read(ctx)
		close(closed)
	}))
	defer srv.Close()
	stream, err := newClient(t, srv.URL).Stream(streamCtx, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer stream.Close()
	cancelStream()
	select {
	case <-closed:
	case <-time.After(500 * time.Millisecond):
		t.Fatal("lifetime cancellation left idle socket open")
	}
	if _, err := stream.Read(context.Background()); !errors.Is(err, context.Canceled) {
		t.Fatalf("Read after lifetime cancellation = %v", err)
	}
}

func TestStreamReadsJSONEscapingExpansion(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	text := strings.Repeat("<", 250000)
	payload := json.RawMessage(`{"text":"` + text + `"}`)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := websocket.Accept(w, r, nil)
		if err != nil {
			return
		}
		defer conn.CloseNow()
		writeFrame(t, ctx, conn, outpost.Frame{Kind: outpost.FrameEvent, Event: &outpost.Envelope{Seq: 1, Payload: payload}})
	}))
	defer srv.Close()
	stream, err := newClient(t, srv.URL).Stream(ctx, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer stream.Close()
	frame, err := stream.Read(ctx)
	if err != nil || frame.Kind != outpost.FrameEvent || frame.Event == nil || frame.Event.Seq != 1 {
		t.Fatalf("expanded frame = %+v, %v", frame, err)
	}
	var decoded struct {
		Text string `json:"text"`
	}
	if err := json.Unmarshal(frame.Event.Payload, &decoded); err != nil {
		t.Fatal(err)
	}
	if decoded.Text != text {
		t.Fatal("legal payload was lost after wire JSON escaping expansion")
	}
}

func TestStreamProtocolErrorsRemainVisible(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	var calls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		conn, err := websocket.Accept(w, r, nil)
		if err != nil {
			return
		}
		defer conn.CloseNow()
		if conn.Write(ctx, websocket.MessageBinary, []byte(`{}`)) != nil {
			return
		}
		if conn.Write(ctx, websocket.MessageText, []byte(`{`)) != nil {
			return
		}
		writeFrame(t, ctx, conn, outpost.Frame{Kind: outpost.FrameReady})
	}))
	defer srv.Close()
	stream, err := newClient(t, srv.URL).Stream(ctx, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer stream.Close()
	if _, err := stream.Read(ctx); err == nil || !strings.Contains(err.Error(), "text frame") {
		t.Fatalf("binary frame error = %v", err)
	}
	if _, err := stream.Read(ctx); err == nil {
		t.Fatal("malformed JSON was hidden")
	}
	if frame, err := stream.Read(ctx); err != nil || frame.Kind != outpost.FrameReady || calls.Load() != 1 {
		t.Fatalf("Read after protocol errors = %+v, %v, calls=%d", frame, err, calls.Load())
	}
}

func TestStreamInitialValidationAndCancellation(t *testing.T) {
	var calls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		<-r.Context().Done()
	}))
	defer srv.Close()
	c := newClient(t, srv.URL)
	if _, err := c.Stream(context.Background(), -1); err == nil || calls.Load() != 0 {
		t.Fatalf("negative cursor = %v, calls=%d", err, calls.Load())
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	if _, err := c.Stream(ctx, 0); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("initial handshake cancellation = %v", err)
	}
}
