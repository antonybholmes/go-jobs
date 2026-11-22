package jobs

import (
	"context"
	"io"
	"time"
)

type (
	JobUser struct {
		ID    string `json:"id"`
		Email string `json:"email,omitempty"`
		Name  string `json:"name,omitempty"`
	}

	// Represents a job, but without the specific details of the work to be processed.
	// This is more for recording job metadata such as sending an email where
	// details such as an S3 bucket or file key are not necessary.
	BaseJob struct {
		User      JobUser   `json:"user"`
		CreatedAt time.Time `json:"createdAt"`
		ID        string    `json:"id"`
		// Name of the job, if applicable.
		Name        string `json:"name,omitempty"`
		Type        string `json:"type"`
		Description string `json:"description,omitempty"`
	}

	// Job represents a unit of work to be processed.
	Job struct {
		BaseJob
		Bucket      string `json:"bucket"`
		Key         string `json:"key"`
		Filename    string `json:"filename"`
		ContentType string `json:"contentType"`
		Size        int64  `json:"size"`
	}

	// MailQueue defines the interface for sending mail items to a queue
	JobQueue interface {
		SendJob(ctx context.Context, job *Job) error
	}

	JobStore interface {
		CreateJob(ctx context.Context, job Job) error
	}

	ObjectStore interface {
		Upload(ctx context.Context, job *Job, body io.ReadSeeker) error
	}

	Payload struct {
		DataType string `json:"dataType"`
		Data     string `json:"data"`
	}

	// Sent on a queue to an email handler for processing.
	MailQueueItem struct {
		Payload *Payload `json:"payload,omitempty"`
		Job     *BaseJob `json:"job,omitempty"`
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

	JobStatus string
)

const (
	JobStatusSubmitted JobStatus = "submitted"
	JobStatusQueued    JobStatus = "queued"
	JobStatusStarted   JobStatus = "started"
	JobStatusError     JobStatus = "error"
	JobStatusCancelled JobStatus = "cancelled"
	JobStatusFailed    JobStatus = "failed"
	JobStatusCompleted JobStatus = "completed"

	EmailTypeJobSubmitted EmailType = "job-submitted"
	EmailTypeJobQueued    EmailType = "job-queued"
	EmailTypeJobStarted   EmailType = "job-started"
	EmailTypeJobError     EmailType = "job-error"
	EmailTypeJobCancelled EmailType = "job-cancelled"
	EmailTypeJobFailed    EmailType = "job-failed"
	EmailTypeJobCompleted EmailType = "job-completed"
)

func NewJobSubmittedMailItem(job *BaseJob) *MailQueueItem {
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
		Job:       &BaseJob{ID: jobID, Type: jobType},
		To:        email,
		EmailType: EmailType("job-" + string(status)),
	}
}
