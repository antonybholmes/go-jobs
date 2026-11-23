package jobs

import (
	"context"
	_ "embed"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type (
	PostgresStore struct {
		emailQueue MailQueue
		pool       *pgxpool.Pool
	}

	MessageSender interface {
		Send(ctx context.Context, payload []byte) error
	}

	OutboxPublisher interface {
		PublishOne(context.Context, MessageSender) (bool, error)
	}
)

const (
	CreateJobSql = `INSERT INTO jobs
		(id, user_id, email, bucket, object_key, filename, content_type, size_bytes, created_at)
		VALUES (:id, :user_id, :email, :bucket, :object_key, :filename, :content_type, :size_bytes, :created_at)`

	UpdateJobStatusSql = `UPDATE jobs SET status = :status::job_status, updated_at = now() WHERE id = :id`

	InsertJobFilesSql = `INSERT INTO job_files 
		(id, 
		type, 
		bucket, 
		object_key, 
		filename, 
		content_type, 
		size_bytes, 
		created_at)
	VALUES (:id, 
		:type, 
		:bucket, 
		:object_key, 
		:filename, 
		:content_type, 
		:size_bytes, 
		:created_at)
	ON CONFLICT (id) DO UPDATE SET 
		type = :type, 
		bucket = :bucket, 
		object_key = :object_key, 
		filename = :filename, 
		content_type = :content_type, 
		size_bytes = :size_bytes, 
		created_at = :created_at`

	SelectJobUserInfoSql = `SELECT user_id, type, email FROM jobs WHERE id = :id`
)

//var schema string

func NewPostgresStore(pool *pgxpool.Pool, emailQueue MailQueue) *PostgresStore {
	return &PostgresStore{
		pool:       pool,
		emailQueue: emailQueue,
	}
}

func (s *PostgresStore) CreateJob(ctx context.Context, job *Job) error {
	//payload, err := json.Marshal(job)

	// if err != nil {
	// 	return fmt.Errorf("marshal job: %w", err)
	// }

	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin job transaction: %w", err)
	}

	defer tx.Rollback(context.Background())

	_, err = tx.Exec(ctx,
		CreateJobSql,
		map[string]any{
			"id":         job.ID,
			"user_id":    job.User.ID,
			"email":      job.User.Email,
			"created_at": job.CreatedAt,
		})

	if err != nil {
		return fmt.Errorf("insert job: %w", err)
	}

	err = insertJobFiles(ctx, tx, job.ID, job.Files)

	if err != nil {
		return fmt.Errorf("update job file: %w", err)
	}

	// _, err = tx.Exec(ctx, `INSERT INTO job_outbox (job_id, payload, created_at)
	// 	VALUES ($1, $2, $3)`, j.ID, payload, j.CreatedAt)
	// if err != nil {
	// 	return fmt.Errorf("insert outbox message: %w", err)
	// }

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit job transaction: %w", err)
	}

	mailItem := NewJobSubmittedMailItem(job)

	if err := s.emailQueue.SendMail(ctx, mailItem); err != nil {
		return fmt.Errorf("send email for job %s: %w", job.ID, err)
	}

	return nil
}

func (s *PostgresStore) UpdateJobStatus(ctx context.Context, job *JobStatusQueueItem) error {

	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin job transaction: %w", err)
	}

	defer tx.Rollback(context.Background())

	_, err = tx.Exec(ctx,
		UpdateJobStatusSql,
		map[string]any{
			"id":     job.ID,
			"status": job.Status,
		})

	if err != nil {
		return fmt.Errorf("update job status: %w", err)
	}

	// update files if any

	err = insertJobFiles(ctx, tx, job.ID, job.Files)

	if err != nil {
		return fmt.Errorf("update job file: %w", err)
	}

	var userID string
	var jobType string
	var email string

	err = tx.QueryRow(ctx,
		SelectJobUserInfoSql,
		map[string]any{
			"id": job.ID,
		}).Scan(&userID, &jobType, &email)

	if err != nil {
		return fmt.Errorf("select job user and email: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit job transaction: %w", err)
	}

	mailItem := NewJobStatusMailItem(job.ID, jobType, userID, email, job.Status)

	if err := s.emailQueue.SendMail(ctx, mailItem); err != nil {
		return fmt.Errorf("send email for job %s: %w", job.ID, err)
	}

	return nil
}

func insertJobFiles(ctx context.Context, tx pgx.Tx, jobID string, files []*JobFile) error {
	if len(files) > 0 {
		for _, file := range files {
			_, err := tx.Exec(ctx,
				InsertJobFilesSql,
				map[string]any{
					"id":           file.ID,
					"job_id":       jobID,
					"type":         file.Type,
					"bucket":       file.Bucket,
					"object_key":   file.Key,
					"filename":     file.Filename,
					"content_type": file.ContentType,
					"size_bytes":   file.Size,
				})

			if err != nil {
				return fmt.Errorf("insert job file: %w", err)
			}
		}
	}

	return nil
}

// func (s *PostgresStore) PublishOne(ctx context.Context, sender MessageSender) (bool, error) {
// 	tx, err := s.pool.Begin(ctx)
// 	if err != nil {
// 		return false, fmt.Errorf("begin outbox transaction: %w", err)
// 	}
// 	defer tx.Rollback(context.Background())

// 	var userID string
// 	var email string
// 	job := BaseJob{}
// 	var payload []byte

// 	err = tx.QueryRow(ctx, `SELECT
// 		job_id::text,
// 		user_id::text,
// 		email,
// 		payload
// 		FROM job_outbox
// 		WHERE published_at IS NULL ORDER BY created_at, job_id
// 		FOR UPDATE SKIP LOCKED LIMIT 1`).Scan(&job.ID, &userID, &email, &payload)

// 	if errors.Is(err, pgx.ErrNoRows) {
// 		return false, nil
// 	}
// 	if err != nil {
// 		return false, fmt.Errorf("select outbox message: %w", err)
// 	}

// 	mailItem := NewJobSubmittedMailItem(job)

// 	if err := s.EmailSender.SendMail(ctx, mailItem); err != nil {
// 		return false, fmt.Errorf("send email for job %s: %w", job.ID, err)
// 	}

// 	if err := sender.Send(ctx, payload); err != nil {
// 		return false, fmt.Errorf("send job %s: %w", job.ID, err)
// 	}

// 	if _, err := tx.Exec(ctx, `UPDATE job_outbox SET published_at = now() WHERE job_id = $1`, job.ID); err != nil {
// 		return false, fmt.Errorf("mark job %s published: %w", job.ID, err)
// 	}

// 	if err := tx.Commit(ctx); err != nil {
// 		return false, fmt.Errorf("commit published job %s: %w", job.ID, err)
// 	}

// 	return true, nil
// }
