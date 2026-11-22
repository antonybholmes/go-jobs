package aws

import (
	"context"
	"fmt"
	"io"

	"github.com/antonybholmes/go-jobs"
	awssys "github.com/antonybholmes/go-sys/aws"
)

type (
	S3JobStore struct {
		s3Store *awssys.S3Store
	}
)

func NewS3JobStore(s3Store *awssys.S3Store) *S3JobStore {
	return &S3JobStore{
		s3Store: s3Store,
	}
}

func (s *S3JobStore) Bucket() string {
	return s.s3Store.Bucket()
}

func (s *S3JobStore) Upload(ctx context.Context, job *jobs.Job) error {

	if len(job.Files) == 0 {
		return fmt.Errorf("no files associated with job")
	}

	for _, file := range job.Files {
		if err := s.UploadJobFile(ctx, file); err != nil {
			return fmt.Errorf("upload file %s to S3: %w", file.Key, err)
		}
	}

	return nil
}

func (s *S3JobStore) UploadJobFile(ctx context.Context, file *jobs.JobFile) error {

	if file == nil {
		return fmt.Errorf("no file provided")
	}

	if err := s.s3Store.Upload(ctx, file.Key, file.Content, file.ContentType); err != nil {
		return fmt.Errorf("upload file %s to S3: %w", file.Key, err)
	}

	return nil
}

func (s *S3JobStore) Download(ctx context.Context, key string) (io.ReadCloser, error) {
	return s.s3Store.Download(ctx, key)
}
