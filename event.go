// Package outpost defines the shared Outpost wire protocol and event validation.
package outpost

import (
	"bytes"
	"encoding/json"
	"fmt"
	"regexp"
	"strings"
	"time"
)

const (
	// Version is the current event envelope version.
	Version = 1
	// MaxPayloadSize is the maximum event payload size in bytes.
	MaxPayloadSize = 1 << 20
)

var eventType = regexp.MustCompile(`^[a-z][a-z0-9_]*(\.[a-z][a-z0-9_]*)+$`)

// Envelope is a stored event, including its server-assigned routing identity and sequence.
type Envelope struct {
	V         int             `json:"v"`
	ID        string          `json:"id"`
	Seq       int64           `json:"seq"`
	AccountID string          `json:"account_id"`
	SessionID string          `json:"session_id,omitempty"`
	Type      string          `json:"type"`
	Source    string          `json:"source"`
	TS        time.Time       `json:"ts"`
	Payload   json.RawMessage `json:"payload"`
	Enc       string          `json:"enc,omitempty"`
}

// Event contains only caller-owned fields; routing identity comes from the token.
type Event struct {
	Type      string          `json:"type"`
	SessionID string          `json:"session_id,omitempty"`
	Payload   json.RawMessage `json:"payload,omitempty"`
	Enc       string          `json:"enc,omitempty"`
}

// Validate checks the caller-owned fields using the same rules as Envelope.Validate.
func (e Event) Validate() error {
	return (Envelope{Type: e.Type, SessionID: e.SessionID, Payload: e.Payload, Enc: e.Enc}).Validate()
}

// Validate checks routing syntax, payload shape, and known plaintext commands.
// Event projection payloads remain lenient; encrypted payloads remain opaque.
// Version, ID, sequence, account and timestamp are assigned by the store.
func (e Envelope) Validate() error {
	if !eventType.MatchString(e.Type) {
		return fmt.Errorf("invalid event type %q: expected a dotted lowercase name", e.Type)
	}
	if e.SessionID == "" && (strings.HasPrefix(e.Type, "session.") || IsCommand(e.Type) || e.Type == ApprovalRequested || e.Type == ApprovalResolved) {
		return fmt.Errorf("session_id is required for %s", e.Type)
	}
	if len(e.Payload) > MaxPayloadSize {
		return fmt.Errorf("payload exceeds %d bytes", MaxPayloadSize)
	}
	if len(e.Payload) != 0 && (!json.Valid(e.Payload) || bytes.TrimSpace(e.Payload)[0] != '{') {
		return fmt.Errorf("payload must be a valid JSON object or empty")
	}
	if e.Enc == "" {
		return e.validateCommandPayload()
	}
	return nil
}

func (e Envelope) validateCommandPayload() error {
	switch e.Type {
	case CommandApprove:
		var payload struct {
			ApprovalID string `json:"approval_id"`
			Decision   string `json:"decision"`
		}
		if len(e.Payload) > 0 {
			if err := json.Unmarshal(e.Payload, &payload); err != nil {
				return fmt.Errorf("invalid approve payload: %w", err)
			}
		}
		if payload.ApprovalID == "" {
			return fmt.Errorf("approval_id is required")
		}
		if payload.Decision != DecisionAllow && payload.Decision != DecisionDeny {
			return fmt.Errorf("decision must be allow or deny")
		}
	case CommandMessage:
		var payload struct {
			Text string `json:"text"`
		}
		if len(e.Payload) > 0 {
			if err := json.Unmarshal(e.Payload, &payload); err != nil {
				return fmt.Errorf("invalid message payload: %w", err)
			}
		}
		if strings.TrimSpace(payload.Text) == "" {
			return fmt.Errorf("text must not be empty")
		}
	}
	return nil
}
