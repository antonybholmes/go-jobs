package jobs

import (
	"context"
)

type (

	// Sent on a queue to an email handler for processing.
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
	EmailTypeJobSubmitted EmailType = "job-submitted"
	EmailTypeJobQueued    EmailType = "job-queued"
	EmailTypeJobStarted   EmailType = "job-started"
	EmailTypeJobError     EmailType = "job-error"
	EmailTypeJobCancelled EmailType = "job-cancelled"
	EmailTypeJobFailed    EmailType = "job-failed"
	EmailTypeJobCompleted EmailType = "job-completed"
)

func NewJobSubmittedMailItem(job *Job) *MailQueueItem {
	return &MailQueueItem{
		UserID:    job.User.ID,
		Job:       job,
		To:        job.User.Email,
		EmailType: EmailTypeJobSubmitted,
	}
}

func NewJobStatusMailItem(jobID string, jobType string, userID string, email string, status JobStatus) *MailQueueItem {
	return &MailQueueItem{
		UserID:    userID,
		Job:       &Job{ID: jobID, Type: jobType},
		To:        email,
		EmailType: EmailType("job-" + string(status)),
	}
}
