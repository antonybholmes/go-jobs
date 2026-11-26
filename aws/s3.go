package aws

import (
	"context"
	"fmt"
	"io"
	"path/filepath"
	"strings"
	"uuid"

	"github.com/antonybholmes/go-jobs"
	awssys "github.com/antonybholmes/go-sys/aws"
	sysfiles "github.com/antonybholmes/go-sys/files"
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
			return fmt.Errorf("upload file %s to S3: %w", file.ObjectKey, err)
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
			return fmt.Errorf("upload file %s to S3: %w", file.ObjectKey, err)
		}
	}

	return nil
}

func (s *S3JobStore) UploadJobFile(ctx context.Context, file *jobs.JobFile) error {

	if file == nil {
		return fmt.Errorf("no file provided")
	}

	if file.ObjectKey == "" {
		return fmt.Errorf("file key is empty")
	}

	if file.Content == nil {
		return fmt.Errorf("file content is empty")
	}

	if err := s.s3Store.Upload(ctx, file.Content, file.ObjectKey); err != nil {
		return fmt.Errorf("upload file %s to S3: %w", file.ObjectKey, err)
	}

	return nil
}

func (s *S3JobStore) DownloadJobFile(ctx context.Context, file *jobs.JobFile) (io.ReadCloser, error) {
	return s.s3Store.Download(ctx, file.ObjectKey)
}

func (s *S3JobStore) DownloadJobFileLocal(ctx context.Context, file *jobs.JobFile, path string) (string, error) {

	if strings.HasSuffix(path, "/") {
		path = filepath.Join(path, filepath.Base(file.ObjectKey))
	}

	if _, err := s.s3Store.DownloadLocal(ctx, file.ObjectKey, path); err != nil {
		return "", fmt.Errorf("download file %s from S3: %w", file.ObjectKey, err)
	}

	return path, nil
}

func MakeS3InputKey(job *jobs.Job, filename string) string {
	return MakeS3JobKey(job) + "/inputs/" + filename
}

func MakeS3RandInputKey(job *jobs.Job, filename string) string {
	id := uuid.NewV7()
	//id := sys.NanoId()

	// Use approved file extension for the uploaded file
	ext := sysfiles.DetectFileExtension(filename)

	return MakeS3JobKey(job) + "/inputs/" + id.String() + "." + ext
}

// MakeS3JobFileOutputKey generates the S3 key for storing output files of a job.

func MakeS3JobFileOutputKey(job *jobs.Job, filename string) string {
	return MakeS3JobKey(job) + "/outputs/" + filename
}

// Files are given a unique identifier with a matching file extension.
func MakeS3JobFileRandOutputKey(job *jobs.Job, filename string) string {
	id := uuid.NewV7()

	// Use approved file extension for the uploaded file
	ext := sysfiles.DetectFileExtension(filename)
	return MakeS3JobKey(job) + "/outputs/" + id.String() + "." + ext
}

// The base key for a user's job in S3 storage. We store all files related to the job under this key.
func MakeS3JobKey(job *jobs.Job) string {
	return "users/" + job.User.ID + "/jobs/" + string(job.Type) + "/" + job.ID
}
