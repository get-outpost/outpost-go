package outpost

const (
	// FrameEvent delivers a stored event from the event log.
	FrameEvent = "event"
	// FrameReady marks completion of the stream's initial backfill.
	FrameReady = "ready"
	// FrameAck acknowledges a published event.
	FrameAck = "ack"
	// FrameError reports a stream or publication error.
	FrameError = "error"
	// FramePublish submits a caller-owned event to the stream.
	FramePublish = "publish"
)

// Frame is a server-to-client WebSocket message. Pointer fields distinguish absent
// values from an explicitly empty correlation ID or a zero head sequence.
type Frame struct {
	Kind    string    `json:"kind"`
	ID      *string   `json:"id,omitempty"`
	Event   *Envelope `json:"event,omitempty"`
	HeadSeq *int64    `json:"head_seq,omitempty"`
	Error   string    `json:"error,omitempty"`
}

// PublishFrame is a client-to-server WebSocket publication with a correlation ID.
type PublishFrame struct {
	Kind  string `json:"kind"`
	ID    string `json:"id"`
	Event *Event `json:"event"`
}
