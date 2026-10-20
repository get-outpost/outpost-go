package outpost_test

import (
	"encoding/json"
	"testing"

	"github.com/get-outpost/outpost-go"
)

func TestProjectionPayloadsRemainLenient(t *testing.T) {
	for _, kind := range []string{
		outpost.SessionStarted, outpost.SessionUpdated, outpost.SessionEnded,
		outpost.SessionDeleted, outpost.ApprovalRequested, outpost.ApprovalResolved,
		outpost.ToolCalled, outpost.ToolCompleted, outpost.MessageCreated,
		outpost.NotificationCreated, "custom.future", "command.future",
	} {
		t.Run(kind, func(t *testing.T) {
			for _, payload := range []string{"", `{}`, `{"provider":"unknown-herdr-kind","status":"unknown","approval_id":123,"decision":[],"title":null,"unknown":true}`} {
				e := outpost.Event{Type: kind, SessionID: "s", Payload: json.RawMessage(payload)}
				if err := e.Validate(); err != nil {
					t.Fatalf("projection payload %q rejected: %v", payload, err)
				}
			}
		})
	}
}

func TestFramePresenceEncoding(t *testing.T) {
	id := ""
	head := int64(0)
	for _, tt := range []struct {
		name  string
		frame outpost.Frame
		want  string
	}{
		{"absent", outpost.Frame{Kind: outpost.FrameReady}, `{"kind":"ready"}`},
		{"zero head", outpost.Frame{Kind: outpost.FrameReady, HeadSeq: &head}, `{"kind":"ready","head_seq":0}`},
		{"empty id", outpost.Frame{Kind: outpost.FrameError, ID: &id, Error: "invalid"}, `{"kind":"error","id":"","error":"invalid"}`},
	} {
		t.Run(tt.name, func(t *testing.T) {
			got, err := json.Marshal(tt.frame)
			if err != nil || string(got) != tt.want {
				t.Fatalf("Marshal() = %s, %v; want %s", got, err, tt.want)
			}
		})
	}
}
