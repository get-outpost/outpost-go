package client_test

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/get-outpost/outpost-go"
	"github.com/get-outpost/outpost-go/client"
)

func newClient(t *testing.T, serverURL string) *client.Client {
	t.Helper()
	c, err := client.New(client.Config{URL: serverURL, Token: "op_fixture"})
	if err != nil {
		t.Fatal(err)
	}
	return c
}

func TestHTTPMethods(t *testing.T) {
	ctx := context.Background()
	principal := outpost.Principal{AccountID: "account", DeviceID: "device", Role: outpost.RolePlugin}
	envelope := outpost.Envelope{V: 1, ID: "event", Seq: 3, Type: "message.created", SessionID: "s/a", Payload: json.RawMessage(`{"text":"hello"}`)}
	session := outpost.Session{ID: "s/a %?#", AccountID: "account", OwnerDeviceID: "device", UpdatedSeq: 3, PendingApprovals: []outpost.PendingApproval{{ID: "approval", RequestedSeq: 2}}}
	page := client.EventPage{Events: []outpost.Envelope{envelope}, NextAfter: 3, HasMore: true}
	snapshot := client.Snapshot{Sessions: []outpost.Session{session}, AsOfSeq: 3}
	input := outpost.Event{Type: "message.created", SessionID: "s/a", Payload: json.RawMessage(`{"text":"hello"}`), Enc: "opaque"}
	tests := []struct {
		name     string
		method   string
		path     string
		query    string
		body     string
		response any
		call     func(*client.Client) (any, error)
	}{
		{"me", http.MethodGet, "/relay/v1/me", "", "", principal, func(c *client.Client) (any, error) { return c.Me(ctx) }},
		{"post_event", http.MethodPost, "/relay/v1/events", "", `{"type":"message.created","session_id":"s/a","payload":{"text":"hello"},"enc":"opaque"}`, envelope, func(c *client.Client) (any, error) { return c.PostEvent(ctx, input) }},
		{"list_events", http.MethodGet, "/relay/v1/events", "after=2&limit=10", "", page, func(c *client.Client) (any, error) { return c.ListEvents(ctx, 2, 10) }},
		{"list_sessions", http.MethodGet, "/relay/v1/sessions", "", "", snapshot, func(c *client.Client) (any, error) { return c.ListSessions(ctx, false) }},
		{"list_deleted_sessions", http.MethodGet, "/relay/v1/sessions", "include_deleted=1", "", snapshot, func(c *client.Client) (any, error) { return c.ListSessions(ctx, true) }},
		{"get_session", http.MethodGet, "/relay/v1/sessions/s%2Fa%20%25%3F%23", "", "", session, func(c *client.Client) (any, error) { return c.GetSession(ctx, session.ID) }},
		{"post_command", http.MethodPost, "/relay/v1/sessions/s%2Fa%20%25%3F%23/commands", "", `{"type":"message.created","payload":{"text":"hello"},"enc":"opaque"}`, envelope, func(c *client.Client) (any, error) { return c.PostCommand(ctx, session.ID, input) }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var calls atomic.Int32
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls.Add(1)
				if r.Method != tt.method || r.URL.EscapedPath() != tt.path || r.URL.RawQuery != tt.query {
					t.Errorf("request = %s %s, want %s %s?%s", r.Method, r.URL.RequestURI(), tt.method, tt.path, tt.query)
				}
				if got := r.Header.Get("Authorization"); got != "Bearer op_fixture" {
					t.Errorf("authorization = %q", got)
				}
				body, err := io.ReadAll(r.Body)
				if err != nil {
					t.Error(err)
				}
				if tt.body == "" {
					if len(body) != 0 || r.Header.Get("Content-Type") != "" {
						t.Errorf("unexpected body or content type: %s, %q", body, r.Header.Get("Content-Type"))
					}
				} else {
					var got, want any
					if err := json.Unmarshal(body, &got); err != nil {
						t.Error(err)
					}
					if err := json.Unmarshal([]byte(tt.body), &want); err != nil {
						t.Error(err)
					}
					if !reflect.DeepEqual(got, want) || r.Header.Get("Content-Type") != "application/json" {
						t.Errorf("body = %s, content type = %q", body, r.Header.Get("Content-Type"))
					}
				}
				w.Header().Set("Content-Type", "application/json")
				if err := json.NewEncoder(w).Encode(tt.response); err != nil {
					t.Error(err)
				}
			}))
			defer srv.Close()
			got, err := tt.call(newClient(t, srv.URL+"/relay"))
			if err != nil || !reflect.DeepEqual(got, tt.response) || calls.Load() != 1 {
				t.Fatalf("response = %#v, %v; want %#v (calls=%d)", got, err, tt.response, calls.Load())
			}
		})
	}
}

func TestSessionIDValidationAndJSONEncoding(t *testing.T) {
	var calls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { calls.Add(1) }))
	defer srv.Close()
	c := newClient(t, srv.URL)
	if _, err := c.GetSession(context.Background(), ""); err == nil {
		t.Fatal("GetSession accepted an empty session ID")
	}
	if _, err := c.PostCommand(context.Background(), "", outpost.Event{}); err == nil {
		t.Fatal("PostCommand accepted an empty session ID")
	}
	if _, err := c.PostEvent(context.Background(), outpost.Event{Payload: json.RawMessage(`{`)}); err == nil {
		t.Fatal("PostEvent accepted invalid JSON")
	}
	if calls.Load() != 0 {
		t.Fatalf("invalid input made %d requests", calls.Load())
	}
}

func TestHTTPResponseErrors(t *testing.T) {
	tests := []struct {
		name    string
		status  int
		body    string
		message string
	}{
		{"relay_message", http.StatusConflict, `{"error":"credential op_fixture rejected"}`, "credential [redacted] rejected"},
		{"empty_message", http.StatusForbidden, `{"error":""}`, "Forbidden"},
		{"non_json", http.StatusBadGateway, "<html>unavailable</html>", "Bad Gateway"},
		{"bounded_body", http.StatusInternalServerError, `{"error":"` + strings.Repeat("x", 16*1024) + `"}`, "Internal Server Error"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(tt.status)
				_, _ = io.WriteString(w, tt.body)
			}))
			defer srv.Close()
			_, err := newClient(t, srv.URL).Me(context.Background())
			var apiError *client.APIError
			if !errors.As(err, &apiError) || apiError.StatusCode != tt.status || apiError.Message != tt.message {
				t.Fatalf("error = %v, want HTTP %d: %s", err, tt.status, tt.message)
			}
			if strings.Contains(err.Error(), "op_fixture") {
				t.Fatalf("credential leaked in error: %v", err)
			}
		})
	}
}

func TestRedirectDoesNotForwardCredentials(t *testing.T) {
	for _, stream := range []bool{false, true} {
		name := "http"
		if stream {
			name = "websocket"
		}
		t.Run(name, func(t *testing.T) {
			var called atomic.Bool
			destination := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { called.Store(true) }))
			defer destination.Close()
			origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { http.Redirect(w, r, destination.URL, http.StatusFound) }))
			defer origin.Close()
			c := newClient(t, origin.URL)
			var err error
			if stream {
				_, err = c.Stream(context.Background(), 0)
			} else {
				_, err = c.Me(context.Background())
			}
			var apiError *client.APIError
			if !errors.As(err, &apiError) || apiError.StatusCode != http.StatusFound || called.Load() {
				t.Fatalf("redirect followed or wrong error: %v, destination called=%v", err, called.Load())
			}
		})
	}
}

func TestHTTPCancellationAndMalformedResponse(t *testing.T) {
	t.Run("cancellation", func(t *testing.T) {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { <-r.Context().Done() }))
		defer srv.Close()
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
		defer cancel()
		_, err := newClient(t, srv.URL).Me(ctx)
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("Me error = %v, want context deadline", err)
		}
	})
	t.Run("malformed_response", func(t *testing.T) {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { _, _ = io.WriteString(w, `{`) }))
		defer srv.Close()
		if _, err := newClient(t, srv.URL).Me(context.Background()); err == nil {
			t.Fatal("Me accepted malformed JSON")
		}
	})
}
