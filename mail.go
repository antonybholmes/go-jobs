package jobs

import (
	"context"
)

type (
	// Item send on the mail queue to generate emails.
	// It is defined in jobs because one of its functions
	// is handling jobs, but it is a general-purpose mail queue
	// for all email types the system can send. The actual
	// sending logic is implemented in the go-mail package.
	// This was the only way to fix a circular dependency
	// where mail needs to reference jobs,
	// and jobs needs to reference mail.
	MailQueueItem struct {
		Payload *Payload `json:"payload,omitempty"`
		Job     *Job     `json:"job,omitempty"`
		// Name of the user, if applicable.
		UserID    string    `json:"userId,omitempty"`
		Name      string    `json:"name,omitempty"`
		To        string    `json:"to"`
		From      string    `json:"from"`
		EmailType EmailType `json:"type"`
		TTL       string    `json:"ttl,omitempty"`
		LinkUrl   string    `json:"linkUrl,omitempty"`
		Mode      string    `json:"mode,omitempty"`
	}

	// MailQueue defines the interface for sending mail items to a queue
	// This is implemented concretely in the go-mail package.
	MailQueue interface {
		SendMail(ctx context.Context, mail *MailQueueItem) error
	}

	EmailType string
)

const (
	EmailTypeJob          EmailType = "job"
	EmailTypeJobQueued    EmailType = "job-queued"
	EmailTypeJobStarted   EmailType = "job-started"
	EmailTypeJobError     EmailType = "job-error"
	EmailTypeJobCancelled EmailType = "job-cancelled"
	EmailTypeJobFailed    EmailType = "job-failed"
	EmailTypeJobCompleted EmailType = "job-completed"
)

func NewJobMailItem(job *Job) *MailQueueItem {
	return &MailQueueItem{
		UserID:    job.User.ID,
		Job:       job,
		To:        job.User.Email,
		EmailType: EmailTypeJob,
	}
}
