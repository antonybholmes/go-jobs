package aws

import (
	"context"
	"fmt"
	"io"
	"path/filepath"
	"uuid"

	"github.com/antonybholmes/go-jobs"
	"github.com/antonybholmes/go-sys"
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

func (s *S3JobStore) UploadFiles(ctx context.Context, files []*jobs.JobFile) error {

	if len(files) == 0 {
		return fmt.Errorf("no files associated with job")
	}

	for _, file := range files {
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

	if file.Key == "" {
		return fmt.Errorf("file key is empty")
	}

	if file.Content == nil {
		return fmt.Errorf("file content is empty")
	}

	if err := s.s3Store.Upload(ctx, file.Content, file.Key, file.ContentType); err != nil {
		return fmt.Errorf("upload file %s to S3: %w", file.Key, err)
	}

	return nil
}

func (s *S3JobStore) DownloadJobFile(ctx context.Context, file *jobs.JobFile) (io.ReadCloser, error) {
	return s.s3Store.Download(ctx, file.Key)
}

func (s *S3JobStore) DownloadJobFileLocal(ctx context.Context, file *jobs.JobFile, dir string) (string, error) {

	path := filepath.Join(dir, filepath.Base(file.Key))

	if _, err := s.s3Store.DownloadLocal(ctx, file.Key, path); err != nil {
		return "", fmt.Errorf("download file %s from S3: %w", file.Key, err)
	}

	return path, nil
}

func MakeUploadKey(job *jobs.Job, filename string) string {
	id := uuid.NewV7()
	//id := sys.NanoId()

	// Use approved file extension for the uploaded file
	ext := sys.DetectFileExtension(filename)

	//return "users/" + job.User.ID + "/jobs/" + job.Type + "/" + job.ID + "/uploads/" + filename
	return MakeS3JobKey(job, filename) + "/uploads/" + id.String() + "." + ext
}

// MakeS3JobFileOutputKey generates the S3 key for storing output files of a job.
// Files are given a unique identifier with a matching file extension.
func MakeS3JobFileOutputKey(job *jobs.Job, filename string) string {
	id := uuid.NewV7()
	//id := sys.NanoId()

	// Use approved file extension for the uploaded file
	ext := sys.DetectFileExtension(filename)
	return MakeS3JobKey(job, filename) + "/downloads/" + id.String() + "." + ext
}

// The base key for a user's job in S3 storage. We store all files related to the job under this key.
func MakeS3JobKey(job *jobs.Job, filename string) string {
	return "users/" + job.User.ID + "/jobs/" + job.Type + "/" + job.ID
}
