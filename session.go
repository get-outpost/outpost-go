package outpost

import "time"

// PendingApproval is an actionable approval request projected from the event log.
type PendingApproval struct {
	ID           string `json:"id"`
	Tool         string `json:"tool"`
	Summary      string `json:"summary"`
	RequestedSeq int64  `json:"requested_seq"`
}

// Session is the current session projection at a snapshot sequence.
type Session struct {
	AccountID        string            `json:"account_id"`
	ID               string            `json:"id"`
	OwnerDeviceID    string            `json:"owner_device_id"`
	Provider         string            `json:"provider"`
	CWD              string            `json:"cwd"`
	Worktree         string            `json:"worktree"`
	Title            string            `json:"title"`
	Status           string            `json:"status"`
	CreatedSeq       int64             `json:"created_seq"`
	UpdatedSeq       int64             `json:"updated_seq"`
	CreatedAt        time.Time         `json:"created_at"`
	UpdatedAt        time.Time         `json:"updated_at"`
	EndedAt          *time.Time        `json:"ended_at"`
	DeletedAt        *time.Time        `json:"deleted_at"`
	PendingApprovals []PendingApproval `json:"pending_approvals"`
}
