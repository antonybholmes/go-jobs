package jobs

import (
	"time"
)

type (
	Job struct {
		ID          string    `json:"id"`
		UserID      string    `json:"userId"`
		Email       string    `json:"email"`
		Type        string    `json:"type"`
		Bucket      string    `json:"bucket"`
		Key         string    `json:"key"`
		Filename    string    `json:"filename"`
		ContentType string    `json:"content_type"`
		Size        int64     `json:"size"`
		CreatedAt   time.Time `json:"created_at"`
	}
)
