package client

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/get-outpost/outpost-go"
)

// RequestTimeout bounds an HTTP request or a WebSocket connection attempt.
// It does not limit the lifetime of an established stream.
const RequestTimeout = 3 * time.Second

// EventPage is a page of account events, ordered by sequence number.
type EventPage struct {
	Events    []outpost.Envelope `json:"events"`
	NextAfter int64              `json:"next_after"`
	HasMore   bool               `json:"has_more"`
}

// Snapshot contains the visible sessions and the account sequence at which
// their state was read.
type Snapshot struct {
	Sessions []outpost.Session `json:"sessions"`
	AsOfSeq  int64             `json:"as_of_seq"`
}

// APIError describes a non-success HTTP response, including a rejected
// WebSocket handshake. Message redacts the configured device token.
type APIError struct {
	StatusCode int
	Message    string
}

// Error returns the HTTP status and relay-provided message.
func (e *APIError) Error() string { return fmt.Sprintf("outpost HTTP %d: %s", e.StatusCode, e.Message) }

// Client performs authenticated requests for one device. A Client may be used
// concurrently. Redirects are never followed, so credentials cannot be forwarded
// to a redirect destination.
type Client struct {
	base  *url.URL
	token string
	http  *http.Client
}

// New validates cfg and constructs a client without making a network request.
func New(cfg Config) (*Client, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	base, _ := url.Parse(cfg.URL)
	return &Client{base: base, token: cfg.Token, http: &http.Client{
		CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
	}}, nil
}

func (c *Client) endpoint(path string) *url.URL { return c.base.JoinPath(path) }

// Me returns the account, device, and role associated with the credential.
func (c *Client) Me(ctx context.Context) (outpost.Principal, error) {
	var result outpost.Principal
	err := c.request(ctx, http.MethodGet, c.endpoint("/v1/me"), nil, &result)
	return result, err
}

// PostEvent publishes caller-owned event fields. The relay supplies routing
// identity, timestamps, and the account sequence number.
func (c *Client) PostEvent(ctx context.Context, input outpost.Event) (outpost.Envelope, error) {
	var result outpost.Envelope
	err := c.request(ctx, http.MethodPost, c.endpoint("/v1/events"), input, &result)
	return result, err
}

// ListEvents returns events after the supplied exclusive sequence cursor.
// The relay requires limit to be between 1 and 1000.
func (c *Client) ListEvents(ctx context.Context, after int64, limit int) (EventPage, error) {
	var result EventPage
	u := c.endpoint("/v1/events")
	u.RawQuery = url.Values{"after": {strconv.FormatInt(after, 10)}, "limit": {strconv.Itoa(limit)}}.Encode()
	err := c.request(ctx, http.MethodGet, u, nil, &result)
	return result, err
}

// ListSessions returns a consistent session snapshot. Set includeDeleted to
// include session tombstones.
func (c *Client) ListSessions(ctx context.Context, includeDeleted bool) (Snapshot, error) {
	var result Snapshot
	u := c.endpoint("/v1/sessions")
	if includeDeleted {
		u.RawQuery = "include_deleted=1"
	}
	err := c.request(ctx, http.MethodGet, u, nil, &result)
	return result, err
}

// GetSession returns one session, including its tombstone if it was deleted.
// sessionID must be nonempty and is escaped as a single URL path segment.
func (c *Client) GetSession(ctx context.Context, sessionID string) (outpost.Session, error) {
	var result outpost.Session
	if sessionID == "" {
		return result, fmt.Errorf("session ID is required")
	}
	err := c.request(ctx, http.MethodGet, c.sessionEndpoint(sessionID), nil, &result)
	return result, err
}

// PostCommand sends a command to sessionID. The ID is escaped as one URL path
// segment and overrides input.SessionID; only Type, Payload, and Enc are sent.
func (c *Client) PostCommand(ctx context.Context, sessionID string, input outpost.Event) (outpost.Envelope, error) {
	var result outpost.Envelope
	if sessionID == "" {
		return result, fmt.Errorf("session ID is required")
	}
	u := c.sessionEndpoint(sessionID)
	u.RawPath += "/commands"
	u.Path += "/commands"
	body := struct {
		Type    string          `json:"type"`
		Payload json.RawMessage `json:"payload,omitempty"`
		Enc     string          `json:"enc,omitempty"`
	}{input.Type, input.Payload, input.Enc}
	err := c.request(ctx, http.MethodPost, u, body, &result)
	return result, err
}

func (c *Client) sessionEndpoint(sessionID string) *url.URL {
	u := c.endpoint("/v1/sessions")
	// RawPath keeps slashes and reserved characters inside the ID's segment.
	u.RawPath = strings.TrimRight(u.EscapedPath(), "/") + "/" + url.PathEscape(sessionID)
	u.Path = strings.TrimRight(u.Path, "/") + "/" + sessionID
	return u
}

func (c *Client) request(ctx context.Context, method string, u *url.URL, input, result any) error {
	ctx, cancel := context.WithTimeout(ctx, RequestTimeout)
	defer cancel()
	var body io.Reader
	if input != nil {
		data, err := json.Marshal(input)
		if err != nil {
			return err
		}
		body = bytes.NewReader(data)
	}
	r, err := http.NewRequestWithContext(ctx, method, u.String(), body)
	if err != nil {
		return err
	}
	r.Header.Set("Authorization", "Bearer "+c.token)
	if input != nil {
		r.Header.Set("Content-Type", "application/json")
	}
	resp, err := c.http.Do(r)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return c.responseError(resp)
	}
	return json.NewDecoder(resp.Body).Decode(result)
}

func (c *Client) responseError(resp *http.Response) error {
	var problem struct {
		Error string `json:"error"`
	}
	_ = json.NewDecoder(io.LimitReader(resp.Body, 16*1024)).Decode(&problem)
	if problem.Error == "" {
		problem.Error = http.StatusText(resp.StatusCode)
	}
	problem.Error = strings.ReplaceAll(problem.Error, c.token, "[redacted]")
	return &APIError{StatusCode: resp.StatusCode, Message: problem.Error}
}
