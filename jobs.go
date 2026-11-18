package jobs

import (
	"time"
)

type (
	// Represents a job, but without the specific details of the work to be processed.
	// This is more for recording job metadata such as sending an email where
	// details such as an S3 bucket or file key are not necessary.
	BaseJob struct {
		// A job such as this should get a unique id such as a UUID.
		ID string `json:"id"`
		// ID of the user who created the job.
		UserID string `json:"userId"`
		// Email of the user who created the job.
		Email string `json:"email"`
		// Type of job, e.g., "email", "file_upload", etc.
		Type string `json:"type"`
		// Optional description of the job.
		Description string `json:"description,omitempty"`
		// Time when the job was created.
		CreatedAt time.Time `json:"createdAt"`
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
)
