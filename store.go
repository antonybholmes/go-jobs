package jobs

import (
	"context"
	_ "embed"
	"fmt"

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

	_, err = tx.Exec(ctx, `INSERT INTO jobs
		(id, user_id, email, bucket, object_key, filename, content_type, size_bytes, created_at)
		VALUES (:id, :user_id, :email, :bucket, :object_key, :filename, :content_type, :size_bytes, :created_at)`,
		map[string]any{
			"id":           job.ID,
			"user_id":      job.User.ID,
			"email":        job.User.Email,
			"bucket":       job.Bucket,
			"object_key":   job.Key,
			"filename":     job.Filename,
			"content_type": job.ContentType,
			"size_bytes":   job.Size,
			"created_at":   job.CreatedAt,
		})

	if err != nil {
		return fmt.Errorf("insert job: %w", err)
	}

	// _, err = tx.Exec(ctx, `INSERT INTO job_outbox (job_id, payload, created_at)
	// 	VALUES ($1, $2, $3)`, j.ID, payload, j.CreatedAt)
	// if err != nil {
	// 	return fmt.Errorf("insert outbox message: %w", err)
	// }

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit job transaction: %w", err)
	}

	mailItem := NewJobSubmittedMailItem(&job.BaseJob)

	if err := s.emailQueue.SendMail(ctx, mailItem); err != nil {
		return fmt.Errorf("send email for job %s: %w", job.ID, err)
	}

	return nil
}

func (s *PostgresStore) UpdateJobStatus(ctx context.Context, jobID string, status JobStatus) error {

	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin job transaction: %w", err)
	}

	defer tx.Rollback(context.Background())

	_, err = tx.Exec(ctx, `UPDATE jobs SET status = :status::job_status, updated_at = now() WHERE id = :id`, map[string]any{
		"status": status,
		"id":     jobID,
	})

	if err != nil {
		return fmt.Errorf("update job status: %w", err)
	}

	var userID string
	var jobType string
	var email string

	err = tx.QueryRow(ctx, `SELECT user_id, type, email FROM jobs WHERE id = :id`, map[string]any{
		"id": jobID,
	}).Scan(&userID, &jobType, &email)

	if err != nil {
		return fmt.Errorf("select job user and email: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit job transaction: %w", err)
	}

	mailItem := NewJobStatusMailItem(jobID, jobType, userID, email, status)

	if err := s.emailQueue.SendMail(ctx, mailItem); err != nil {
		return fmt.Errorf("send email for job %s: %w", jobID, err)
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
