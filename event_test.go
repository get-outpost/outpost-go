package outpost_test

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/get-outpost/outpost-go"
)

func TestEnvelopeValidate(t *testing.T) {
	atLimit := `{"data":"` + strings.Repeat("x", outpost.MaxPayloadSize-len(`{"data":""}`)) + `"}`
	tests := []struct {
		name, kind, payload string
		valid               bool
	}{
		{"empty payload", "session.started", "", true},
		{"object", "session.started", `{"nested":{"arbitrary":[1,null,true]}}`, true},
		{"whitespace around object", "session.started", " \n {} \t", true},
		{"type segments", "session_2.tool_call.done3", `{}`, true},
		{"at size limit", "session.started", atLimit, true},
		{"over size limit", "session.started", atLimit + " ", false},
		{"missing type", "", `{}`, false},
		{"undotted type", "session", `{}`, false},
		{"uppercase", "Session.started", `{}`, false},
		{"trailing dot", "session.started.", `{}`, false},
		{"empty segment", "session..started", `{}`, false},
		{"numeric segment", "session.1started", `{}`, false},
		{"hyphen", "session.tool-call", `{}`, false},
		{"newline", "session.started\n", `{}`, false},
		{"malformed object", "session.started", `{"x":}`, false},
		{"array", "session.started", `[]`, false},
		{"null", "session.started", `null`, false},
		{"string", "session.started", `"ciphertext"`, false},
		{"number", "session.started", `1`, false},
		{"bool", "session.started", `true`, false},
		{"whitespace only", "session.started", " \n ", false},
		{"multiple objects", "session.started", `{} {}`, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := (outpost.Envelope{SessionID: "session-1", Type: tt.kind, Payload: json.RawMessage(tt.payload)}).Validate()
			if (err == nil) != tt.valid {
				t.Fatalf("Validate() = %v, want valid=%v", err, tt.valid)
			}
		})
	}
}

func TestSessionIDRequired(t *testing.T) {
	for _, kind := range []string{outpost.SessionStarted, outpost.SessionUpdated, outpost.SessionEnded, outpost.SessionDeleted, outpost.ApprovalRequested, outpost.ApprovalResolved, "session.future", outpost.CommandApprove, outpost.CommandMessage, outpost.CommandInterrupt, "command.future"} {
		if err := (outpost.Envelope{Type: kind}).Validate(); err == nil {
			t.Fatalf("%s accepted without a session_id", kind)
		}
	}
	if err := (outpost.Envelope{Type: "message.created"}).Validate(); err != nil {
		t.Fatalf("log-only event needs no session_id: %v", err)
	}
}
