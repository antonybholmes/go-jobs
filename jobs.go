package jobs

import (
	"context"
	"io"
	"time"
)

type (
	// Represents a job, but without the specific details of the work to be processed.
	// This is more for recording job metadata such as sending an email where
	// details such as an S3 bucket or file key are not necessary.
	BaseJob struct {
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

	// SubmitHandler handles the submission of jobs, including validating metadata,
	// uploading files to the object store, and sending jobs to the job queue.
	SubmitHandler struct {
		jobQueue    JobQueue
		objectStore ObjectStore
		bucket      string
		maxBody     int64
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

	MailItem struct {
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
	MailQueue interface {
		SendMail(ctx context.Context, mail *MailItem) error
	}

	EmailType string
)

const (
	EmailTypeJobSubmitted  EmailType = "job-submitted"
	EmailTypeJobQueued     EmailType = "job-queued"
	EmailTypeJobStarted    EmailType = "job-started"
	EmailTypeJobError      EmailType = "job-error"
	EmailTypeJobCancelled  EmailType = "job-cancelled"
	EmailTypeJobInProgress EmailType = "job-in-progress"
	EmailTypeJobFailed     EmailType = "job-failed"
	EmailTypeJobComplete   EmailType = "job-complete"
)

func NewJobSubmittedMailItem(job *BaseJob, userID, to string) *MailItem {
	return &MailItem{
		UserID:    userID,
		Job:       job,
		To:        to,
		EmailType: EmailTypeJobSubmitted,
	}
}
