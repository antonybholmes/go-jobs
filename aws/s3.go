package aws

import (
	"context"
	"fmt"
	"io"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"

	"github.com/antonybholmes/go-jobs"
)

type (
	S3Store struct {
		client *s3.Client
		bucket string
	}

	S3JobStore struct {
		s3Store *S3Store
	}
)

func NewS3Store(client *s3.Client, bucket string) *S3Store {
	return &S3Store{
		client: client,
		bucket: bucket,
	}
}

func (s *S3Store) Upload(ctx context.Context,
	key string,
	body io.ReadSeeker,
	contentType string) error {
	input := s3.PutObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(key),
		Body:   body,
	}

	if contentType != "" {
		input.ContentType = aws.String(contentType)
	}

	_, err := s.client.PutObject(ctx, &input)

	if err != nil {
		return fmt.Errorf("put S3 object: %w", err)
	}

	return nil
}

func NewS3JobStore(s3Store *S3Store) *S3JobStore {
	return &S3JobStore{
		s3Store: s3Store,
	}
}

func (s *S3JobStore) Upload(ctx context.Context, job *jobs.Job, body io.ReadSeeker) error {
	return s.s3Store.Upload(ctx, job.Key, body, job.ContentType)
}
