package outpost_test

import (
	"encoding/json"
	"testing"

	"github.com/get-outpost/outpost-go"
)

func TestCommandPayloadValidation(t *testing.T) {
	for _, tt := range []struct {
		name, kind, payload, enc string
		valid                    bool
	}{
		{"approve allow", outpost.CommandApprove, `{"approval_id":"p","decision":"allow"}`, "", true},
		{"approve deny", outpost.CommandApprove, `{"approval_id":"p","decision":"deny"}`, "", true},
		{"missing approval", outpost.CommandApprove, `{"decision":"allow"}`, "", false},
		{"wrong approval type", outpost.CommandApprove, `{"approval_id":1,"decision":"allow"}`, "", false},
		{"invalid decision", outpost.CommandApprove, `{"approval_id":"p","decision":"yes"}`, "", false},
		{"null decision", outpost.CommandApprove, `{"approval_id":"p","decision":null}`, "", false},
		{"empty approve", outpost.CommandApprove, "", "", false},
		{"message", outpost.CommandMessage, `{"text":"Please run tests"}`, "", true},
		{"empty message", outpost.CommandMessage, `{"text":""}`, "", false},
		{"blank message", outpost.CommandMessage, `{"text":" \n\t "}`, "", false},
		{"missing message", outpost.CommandMessage, `{}`, "", false},
		{"wrong message type", outpost.CommandMessage, `{"text":123}`, "", false},
		{"interrupt", outpost.CommandInterrupt, `{}`, "", true},
		{"empty interrupt", outpost.CommandInterrupt, "", "", true},
		{"encrypted approve", outpost.CommandApprove, `{"decision":123}`, "e2ee", true},
		{"encrypted message", outpost.CommandMessage, `{}`, "e2ee", true},
		{"encrypted empty approve", outpost.CommandApprove, "", "e2ee", true},
		{"encrypted still needs object", outpost.CommandApprove, `[]`, "e2ee", false},
	} {
		t.Run(tt.name, func(t *testing.T) {
			err := (outpost.Envelope{Type: tt.kind, SessionID: "s", Payload: json.RawMessage(tt.payload), Enc: tt.enc}).Validate()
			if (err == nil) != tt.valid {
				t.Fatalf("Validate = %v, want valid=%v", err, tt.valid)
			}
		})
	}
}
