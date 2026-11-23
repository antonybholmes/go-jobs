package jobs

import (
	"context"
	_ "embed"
	"fmt"

	"github.com/antonybholmes/go-sys/log"
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

	JobType string
)

const (
	CreateJobSql = `INSERT INTO jobs
		(id, user_id, email, type_id, status_id, created_at)
		VALUES (@id::uuid, @user_id::uuid, @email, @type_id, @status_id, now())`

	UpdateJobStatusSql = `UPDATE jobs SET status = @status::job_status, updated_at = now() WHERE id = @id`

	InsertJobFilesSql = `INSERT INTO files (id, job_id, file_type_id, bucket, object_key, filename, content_type, size_bytes)
		VALUES (@id::uuid, @job_id::uuid, @file_type_id, @bucket, @object_key, @filename, @content_type, @size_bytes)
		ON CONFLICT (id) DO UPDATE SET
			file_type_id = EXCLUDED.file_type_id,
			bucket = EXCLUDED.bucket,
			object_key = EXCLUDED.object_key,
			filename = EXCLUDED.filename,
			content_type = EXCLUDED.content_type,
			size_bytes = EXCLUDED.size_bytes;`

	SelectJobUserInfoSql = `SELECT 
		user_id, 
		name as type, 
		email 
		FROM jobs JOIN job_types ON jobs.type_id = job_types.id 
		WHERE jobs.id = @id::uuid`

	JobTypeTest   JobType = "test"
	JobTypeCrispr JobType = "crispr"

	JobStatusSubmittedID int = 1
	JobStatusPendingID   int = 2
	JobStatusRunningID   int = 3
	JobStatusCompletedID int = 4
	JobStatusFailedID    int = 5

	FileTypeInputID  int = 1
	FileTypeOutputID int = 2
)

var (
	JobStatusMap = map[JobStatus]int{
		"submitted": JobStatusSubmittedID,
		"pending":   JobStatusPendingID,
		"running":   JobStatusRunningID,
		"completed": JobStatusCompletedID,
		"failed":    JobStatusFailedID,
	}

	JobTypeMap = map[string]int{
		string(JobTypeTest):   1,
		string(JobTypeCrispr): 2,
	}

	FileTypeMap = map[JobFileType]int{
		JobFileTypeInput:  FileTypeInputID,
		JobFileTypeOutput: FileTypeOutputID,
	}
)

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
		pgx.NamedArgs{
			"id":      job.ID,
			"user_id": job.User.ID,
			"email":   job.User.Email,
			//"created_at": job.CreatedAt,
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

func (s *PostgresStore) InsertJob(ctx context.Context, q *JobQueueItem) error {
	//payload, err := json.Marshal(job)

	// if err != nil {JobQueueItem
	// 	return fmt.Errorf("marshal job: %w", err)
	// }

	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin job transaction: %w", err)
	}

	defer tx.Rollback(context.Background())

	log.Debug().Msgf("Inserting job:  %s", CreateJobSql)

	typeID, ok := JobTypeMap[q.Job.Type]
	if !ok {
		return fmt.Errorf("unknown job type: %s", q.Job.Type)
	}

	_, err = tx.Exec(ctx,
		CreateJobSql,
		pgx.NamedArgs{
			"id":        q.Job.ID,
			"user_id":   q.Job.User.ID,
			"email":     q.Job.User.Email,
			"type_id":   typeID,
			"status_id": JobStatusSubmittedID,
			//"created_at": q.Job.CreatedAt,
		})

	if err != nil {
		return fmt.Errorf("insert job: %w", err)
	}

	err = insertJobFiles(ctx, tx, q.Job.ID, q.Job.Files)

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

	mailItem := NewJobSubmittedMailItem(q.Job)

	if err := s.emailQueue.SendMail(ctx, mailItem); err != nil {
		return fmt.Errorf("send email for job %s: %w", q.Job.ID, err)
	}

	return nil
}

func (s *PostgresStore) UpdateJobStatus(ctx context.Context, q *JobQueueItem) error {

	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin job transaction: %w", err)
	}

	defer tx.Rollback(context.Background())

	_, err = tx.Exec(ctx,
		UpdateJobStatusSql,
		pgx.NamedArgs{
			"id":     q.Job.ID,
			"status": q.Status,
		})

	if err != nil {
		return fmt.Errorf("update job status: %w", err)
	}

	// update files if any

	err = insertJobFiles(ctx, tx, q.Job.ID, q.Job.Files)

	if err != nil {
		return fmt.Errorf("update job file: %w", err)
	}

	var userID string
	var jobType string
	var email string

	err = tx.QueryRow(ctx,
		SelectJobUserInfoSql,
		pgx.NamedArgs{
			"id": q.Job.ID,
		}).Scan(&userID, &jobType, &email)

	if err != nil {
		return fmt.Errorf("select job user and email: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit job transaction: %w", err)
	}

	mailItem := NewJobStatusMailItem(q.Job.ID, jobType, userID, email, q.Status)

	if err := s.emailQueue.SendMail(ctx, mailItem); err != nil {
		return fmt.Errorf("send email for job %s: %w", q.Job.ID, err)
	}

	return nil
}

func insertJobFiles(ctx context.Context, tx pgx.Tx, jobID string, files []*JobFile) error {
	if len(files) > 0 {
		for _, file := range files {

			fileTypeID, ok := FileTypeMap[file.Type]
			if !ok {
				return fmt.Errorf("unknown job file type: %s", file.Type)
			}

			_, err := tx.Exec(ctx,
				InsertJobFilesSql,
				pgx.NamedArgs{
					"id":           file.ID,
					"job_id":       jobID,
					"file_type_id": fileTypeID,
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
