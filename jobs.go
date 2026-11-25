package jobs

import (
	"context"
	"io"
	"time"
	"uuid"
)

type (
	JobStatus        string
	JobFileType      string
	JobQueueItemType string

	JobUser struct {
		// Unique identifier for the user.
		ID string `json:"id"`
		// Email address of the user.
		Email string `json:"email"`
		// Name of the user. If not provided, email will be used as the display name.
		Name string `json:"name,omitempty"`
	}

	JobFile struct {
		//CreatedAt   time.Time     `json:"createdAt"`
		Content io.ReadSeeker `json:"-"`
		ID      string        `json:"id"`
		Type    JobFileType   `json:"type"`
		// For security never expose the actual bucket
		Bucket      string    `json:"-"`
		ObjectKey   string    `json:"objectKey"`
		Filename    string    `json:"filename"`
		ContentType string    `json:"contentType"`
		Size        int64     `json:"size"`
		CreatedAt   time.Time `json:"createdAt"`
		UpdatedAt   time.Time `json:"updatedAt"`
	}

	// Job represents a unit of work to be processed.
	Job struct {
		User        JobUser    `json:"user"`
		ID          string     `json:"id"`
		Name        string     `json:"name,omitempty"`
		Type        string     `json:"type,omitempty"`
		Description string     `json:"description,omitempty"`
		Status      JobStatus  `json:"status,omitempty"`
		CreatedAt   time.Time  `json:"createdAt"`
		UpdatedAt   time.Time  `json:"updatedAt"`
		Files       []*JobFile `json:"files,omitempty"`
	}

	// JobQueueItem struct {
	// 	// Why are we sending this item, e.g. submit new job
	// 	Type JobQueueItemType `json:"type"`

	// 	//UpdatedAt time.Time  `json:"updatedAt"`
	// 	//ID      string     `json:"jobId"`
	// 	//Status JobStatus `json:"status"`
	// 	//Message string     `json:"message,omitempty"`
	// 	//Files   []*JobFile `json:"files,omitempty"`
	// 	Job *Job `json:"job"`
	// }

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

	JobQueueItemTypeSubmit JobQueueItemType = "submit"
	JobQueueItemTypeStatus JobQueueItemType = "status"
)

// func NewJobStatusQueueItem(jobID string, status JobStatus, message string, files []*JobFile) *JobStatusQueueItem {
// 	return &JobStatusQueueItem{
// 		ID:      jobID,
// 		Status:  status,
// 		Message: message,
// 		Files:   files,
// 		//UpdatedAt: time.Now(),
// 	}
// }

func NewJobFile(fileType JobFileType, bucket, key, filename, contentType string, size int64, content io.ReadSeeker) *JobFile {
	id := uuid.NewV7()

	file := JobFile{
		ID:          id.String(),
		Type:        fileType,
		Bucket:      bucket,
		ObjectKey:   key,
		Filename:    filename,
		ContentType: contentType,
		Size:        size,
		//CreatedAt:   time.Now(),
		Content: content,
	}

	return &file
}

func NewJob(user JobUser, jobType string, files []*JobFile) *Job {
	id := uuid.NewV7()

	return &Job{
		ID:   id.String(),
		User: user,
		//CreatedAt: time.Now(),
		Files: files,
	}
}
