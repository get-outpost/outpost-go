# Outpost Go SDK

Go protocol types and an authenticated HTTP/WebSocket client for Outpost agent
adapters and applications. Requires Go 1.27. MIT licensed.

```sh
go get github.com/get-outpost/outpost-go@v0.1.0
```

## Packages

- `outpost`: event envelopes and caller-owned events, vocabulary constants,
  sessions and pending approvals, device identities, WebSocket frames, and pure
  validation. Payloads stay `json.RawMessage`; encrypted payloads stay opaque.
- `client`: token-authenticated HTTP requests and resumable WebSocket streams.
  The only external dependency is `github.com/coder/websocket`.

Canonical providers match herdr agent kinds: `ProviderClaude` (`claude`),
`ProviderCodex` (`codex`), `ProviderPi` (`pi`), `ProviderOMP` (`omp`), and
`ProviderOpenCode` (`opencode`). Unknown provider kinds pass through unchanged.

## Publish and consume

Use a `plugin` device token to publish telemetry; `client` devices publish
commands. Tokens and account/device provisioning belong to the application.
The SDK does not read login files or environment variables, persist credentials,
or depend on a server implementation.

```go
package main

import (
    "context"
    "encoding/json"
    "log"
    "os"

    "github.com/get-outpost/outpost-go"
    "github.com/get-outpost/outpost-go/client"
)

func main() {
    ctx := context.Background()
    c, err := client.New(client.Config{
        URL: "http://127.0.0.1:8080",
        Token: os.Getenv("OUTPOST_TOKEN"),
    })
    if err != nil { log.Fatal(err) }
    started, err := c.PostEvent(ctx, outpost.Event{
        Type: outpost.SessionStarted,
        SessionID: "example",
        Payload: json.RawMessage(`{"provider":"claude","title":"Example"}`),
    })
    if err != nil { log.Fatal(err) }
    stream, err := c.Stream(ctx, started.Seq)
    if err != nil { log.Fatal(err) }
    defer stream.Close()
    for {
        frame, err := stream.Read(ctx)
        if err != nil { log.Fatal(err) }
        if frame.Kind == outpost.FrameEvent && frame.Event != nil {
            log.Printf("%d %s", frame.Event.Seq, frame.Event.Type)
        }
    }
}
```

`Me` returns the authenticated account, device, and role. `PostEvent` publishes
caller-owned fields only. `ListEvents(ctx, after, limit)` returns an ordered page
with `NextAfter` and `HasMore`; pass `NextAfter` to the next call. `ListSessions`
returns sessions plus an `AsOfSeq` snapshot cursor; `GetSession` includes pending
approvals and tombstones. `PostCommand(ctx, sessionID, event)` targets the path
session and ignores the event's `SessionID` field.

Start a stream after the snapshot's `AsOfSeq` to avoid a snapshot/live gap.
`Stream` makes one initial connection attempt. After connection, `Read` reconnects
with 200 ms–5 s exponential backoff and deduplicates delivered event sequences.
Only an `event` frame advances the in-memory cursor: `ready.HeadSeq` and `ack`
envelopes never do. Persist your last successfully processed event sequence if
you need restart durability. A stream is a single-reader API; `Close` can interrupt
a blocked read. Cancel the stream context to end its lifetime, or the read context
to cancel a read. HTTP 401/403 stop retries; malformed JSON and non-text frames
return errors. Application-level `error` frames are returned to the caller.

HTTP calls and WebSocket handshakes have a three-second request budget, bounded
by the caller's context. Redirects are not followed, so a device token cannot be
forwarded to another endpoint. Non-success HTTP responses return `*client.APIError`
with a status code and a token-redacted message. Use HTTPS outside localhost.

`Envelope.Validate` and `Event.Validate` check dotted lowercase event names,
required session IDs, JSON object shape, the 1 MiB payload limit, and known
plaintext command fields. Projection payload fields and unknown dotted event
kinds remain lenient; provider identifiers are not a closed enum. Validation does
not grant authorization or check server-side command eligibility. Commands are
requests, not state transitions: the owning adapter publishes their results.

## Development

```sh
go vet ./...
go test -race ./...
```
