package outpost

import "strings"

const (
	// SessionStarted announces a session and its initial metadata.
	SessionStarted = "session.started"
	// SessionUpdated updates a subset of session metadata.
	SessionUpdated = "session.updated"
	// SessionEnded marks a session as done.
	SessionEnded = "session.ended"
	// SessionDeleted tombstones a session.
	SessionDeleted = "session.deleted"
	// ApprovalRequested announces an actionable approval request.
	ApprovalRequested = "approval.requested"
	// ApprovalResolved records an approval decision or expiration.
	ApprovalResolved = "approval.resolved"
	// ToolCalled records a tool invocation.
	ToolCalled = "tool.called"
	// ToolCompleted records completion of a tool invocation.
	ToolCompleted = "tool.completed"
	// MessageCreated records a timeline message.
	MessageCreated = "message.created"
	// NotificationCreated records a notification.
	NotificationCreated = "notification.created"
	// TerminalOutput records terminal output without changing projections.
	TerminalOutput = "terminal.output"
	// CommandApprove asks a plugin to allow or deny an approval.
	CommandApprove = "command.approve"
	// CommandMessage asks a plugin to deliver a message to the session.
	CommandMessage = "command.message"
	// CommandInterrupt asks a plugin to interrupt the session.
	CommandInterrupt = "command.interrupt"
)

const (
	// StatusWorking indicates that a session is working.
	StatusWorking = "working"
	// StatusIdle indicates that a session is idle.
	StatusIdle = "idle"
	// StatusBlocked indicates that a session is blocked.
	StatusBlocked = "blocked"
	// StatusDone indicates that a session has finished.
	StatusDone = "done"
	// DecisionAllow permits an approval request.
	DecisionAllow = "allow"
	// DecisionDeny rejects an approval request.
	DecisionDeny = "deny"
	// DecisionExpired indicates an approval request is no longer actionable.
	DecisionExpired = "expired"
)

// IsCommand reports whether kind belongs to the command namespace, including unknown commands.
func IsCommand(kind string) bool { return strings.HasPrefix(kind, "command.") }

// Canonical provider identifiers describe known harnesses. Unknown herdr kinds
// pass through unchanged; providers are not validated as a closed enumeration.
const (
	// ProviderClaude identifies Claude Code.
	ProviderClaude = "claude"
	// ProviderCodex identifies Codex.
	ProviderCodex = "codex"
	// ProviderPi identifies pi.
	ProviderPi = "pi"
	// ProviderOMP identifies omp.
	ProviderOMP = "omp"
	// ProviderOpenCode identifies OpenCode.
	ProviderOpenCode = "opencode"
)

// Role identifies the publishing permissions of an authenticated device.
type Role string

const (
	// RolePlugin publishes telemetry and consumes commands.
	RolePlugin Role = "plugin"
	// RoleClient publishes commands and consumes telemetry.
	RoleClient Role = "client"
)

// Valid reports whether r is a supported device role.
func (r Role) Valid() bool { return r == RolePlugin || r == RoleClient }

// Principal identifies the account, device, and role authenticated by a token.
type Principal struct {
	AccountID string `json:"account_id"`
	DeviceID  string `json:"device_id"`
	Role      Role   `json:"role"`
}
