package jobs

import (
	"context"
	"io"
	"time"
	"uuid"
)

type (
	JobType          string
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
		CreatedAt time.Time     `json:"createdAt"`
		UpdatedAt time.Time     `json:"updatedAt"`
		Content   io.ReadSeeker `json:"-"`
		ID        string        `json:"id"`
		Type      JobFileType   `json:"type"`
		//Bucket      string     `json:"-"`
		ObjectKey   string `json:"objectKey"`
		Filename    string `json:"filename"`
		ContentType string `json:"contentType"`
		Size        int64  `json:"size"`
	}

	// Job represents a unit of work to be processed.
	Job struct {
		User        JobUser `json:"user"`
		ID          string  `json:"id"`
		Name        string  `json:"name,omitempty"`
		Type        JobType `json:"type,omitempty"`
		Description string  `json:"description,omitempty"`
		// The S3 bucket where job files are stored.
		// This is not exposed in the JSON representation since users
		// do not need to see the underlying storage details.
		Bucket    string     `json:"-"`
		Status    JobStatus  `json:"status,omitempty"`
		CreatedAt time.Time  `json:"createdAt"`
		UpdatedAt time.Time  `json:"updatedAt"`
		Files     []*JobFile `json:"files,omitempty"`
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
	JobTypeCrispr JobType = "crispr"
	JobTypeTest   JobType = "test"

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

func NewJobFile(fileType JobFileType, key, filename, contentType string, size int64, content io.ReadSeeker) *JobFile {
	id := uuid.NewV7()

	file := JobFile{
		ID:   id.String(),
		Type: fileType,

		ObjectKey:   key,
		Filename:    filename,
		ContentType: contentType,
		Size:        size,
		//CreatedAt:   time.Now(),
		Content: content,
	}

	return &file
}

func NewJob(user JobUser, jobType string, bucket string, files []*JobFile) *Job {
	id := uuid.NewV7()

	return &Job{
		ID:   id.String(),
		User: user,
		//CreatedAt: time.Now(),
		Bucket: bucket,
		Files:  files,
	}
}

// InputFiles returns all input files associated with the job.
func (j *Job) InputFiles() []*JobFile {
	var inputs []*JobFile
	for _, f := range j.Files {
		if f.Type == JobFileTypeInput {
			inputs = append(inputs, f)
		}
	}
	return inputs
}
