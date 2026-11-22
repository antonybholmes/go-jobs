package jobs

import (
	"context"
	"io"
	"time"

	"github.com/google/uuid"
)

type (
	JobStatus   string
	JobFileType string

	JobUser struct {
		// Unique identifier for the user.
		ID string `json:"id"`
		// Email address of the user.
		Email string `json:"email"`
		// Name of the user. If not provided, email will be used as the display name.
		Name string `json:"name,omitempty"`
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

	JobFile struct {
		ID string `json:"id"`
		// Whether input or output file.
		Type        JobFileType `json:"type"`
		Bucket      string      `json:"bucket"`
		Key         string      `json:"key"`
		Filename    string      `json:"filename"`
		ContentType string      `json:"contentType"`
		Size        int64       `json:"size"`
		CreatedAt   time.Time   `json:"createdAt"`
		// The content of the file.
		Content io.ReadSeeker `json:"-"`
	}

	// Job represents a unit of work to be processed.
	Job struct {
		BaseJob
		Files []*JobFile `json:"files,omitempty"`
	}

	JobStatusQueueItem struct {
		// The ID of the job this status update pertains to.
		ID string `json:"jobId"`
		// The type of the job.
		Status JobStatus `json:"status"`
		// An optional message providing additional context about the job status.
		Message string `json:"message,omitempty"`
		// The files created in the s3 bucket as a result of this job.
		Files []*JobFile `json:"files,omitempty"`
		// The mode in which the job was executed, if applicable.
		UpdatedAt time.Time `json:"updatedAt"`
	}

	// MailQueue defines the interface for sending mail items to a queue
	JobQueue interface {
		SendJob(ctx context.Context, job *Job) error
	}

	JobStore interface {
		CreateJob(ctx context.Context, job Job) error
	}

	ObjectStore interface {
		Upload(ctx context.Context, job *Job) error
	}

	Payload struct {
		DataType string `json:"dataType"`
		Data     string `json:"data"`
	}
)

const (
	JobFileTypeInput  JobFileType = "input"
	JobFileTypeOutput JobFileType = "output"

	JobStatusSubmitted JobStatus = "submitted"
	JobStatusQueued    JobStatus = "queued"
	JobStatusStarted   JobStatus = "started"
	JobStatusError     JobStatus = "error"
	JobStatusCancelled JobStatus = "cancelled"
	JobStatusFailed    JobStatus = "failed"
	JobStatusCompleted JobStatus = "completed"
)

func NewJobStatusQueueItem(jobID string, status JobStatus, message string, files []*JobFile) *JobStatusQueueItem {
	return &JobStatusQueueItem{
		ID:        jobID,
		Status:    status,
		Message:   message,
		Files:     files,
		UpdatedAt: time.Now(),
	}
}

func NewJobFile(fileType JobFileType, bucket, key, filename, contentType string, size int64, content io.ReadSeeker) *JobFile {
	id, _ := uuid.NewV7()

	file := JobFile{
		ID:          id.String(),
		Type:        fileType,
		Bucket:      bucket,
		Key:         key,
		Filename:    filename,
		ContentType: contentType,
		Size:        size,
		CreatedAt:   time.Now(),
		Content:     content,
	}

	return &file
}

func NewJob(user JobUser, jobType string, files []*JobFile) *Job {
	id, _ := uuid.NewV7()

	return &Job{
		ID:        id.String(),
		User:      user,
		CreatedAt: time.Now(),
		Files:     files,
	}
}
