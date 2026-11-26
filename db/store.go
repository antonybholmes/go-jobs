package db

import (
	"context"
	_ "embed"
	"fmt"

	"github.com/antonybholmes/go-jobs"
	"github.com/antonybholmes/go-sys/log"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type (
	TypeID int

	PostgresStore struct {
		emailQueue jobs.MailQueue
		pool       *pgxpool.Pool
	}

	// MessageSender interface {
	// 	Send(ctx context.Context, payload []byte) error
	// }

	// OutboxPublisher interface {
	// 	PublishOne(context.Context, MessageSender) (bool, error)
	// }
)

const (
	// If job already exists, update its status.
	InsertJobSql = `INSERT INTO jobs
		(id, user_id, email, type_id, status_id)
		VALUES 
		(@id::uuid, @user_id::uuid, @email, @type_id, @status_id)
		ON CONFLICT (id) 
		DO UPDATE SET
			status_id = EXCLUDED.status_id,
			updated_at = NOW()`

	UpdateJobStatusSql = `UPDATE jobs SET status_id = @status_id WHERE id = @id::uuid`

	// Inserting job files associated with a job. If they exist, do nothing
	InsertJobFilesSql = `INSERT INTO files 
		(id, job_id, file_type_id, bucket, object_key, filename, content_type, size_bytes)
		VALUES 
		(@id::uuid, @job_id::uuid, @file_type_id, @bucket, @object_key, @filename, @content_type, @size_bytes)
		ON CONFLICT (id) DO NOTHING`

	SelectJobUserInfoSql = `SELECT 
		user_id, 
		name as type, 
		email 
		FROM jobs JOIN job_types ON jobs.type_id = job_types.id 
		WHERE jobs.id = @id::uuid`

	// Special, known job status IDs for the system
	JobStatusSubmittedID TypeID = 1
	JobStatusPendingID   TypeID = 2
	JobStatusRunningID   TypeID = 3
	JobStatusCompletedID TypeID = 4
	JobStatusFailedID    TypeID = 5

	// Known file type IDs for the system
	FileTypeInputID  TypeID = 1
	FileTypeOutputID TypeID = 2
)

var (
	JobStatusMap = map[jobs.JobStatus]TypeID{
		"submitted": JobStatusSubmittedID,
		"pending":   JobStatusPendingID,
		"running":   JobStatusRunningID,
		"completed": JobStatusCompletedID,
		"failed":    JobStatusFailedID,
	}

	JobTypeMap = map[jobs.JobType]TypeID{
		jobs.JobTypeTest:   1,
		jobs.JobTypeCrispr: 2,
	}

	FileTypeMap = map[jobs.JobFileType]TypeID{
		jobs.JobFileTypeInput:  FileTypeInputID,
		jobs.JobFileTypeOutput: FileTypeOutputID,
	}
)

func NewPostgresStore(pool *pgxpool.Pool, emailQueue jobs.MailQueue) *PostgresStore {
	return &PostgresStore{
		pool:       pool,
		emailQueue: emailQueue,
	}
}

// func (s *PostgresStore) CreateJob(ctx context.Context, job *Job) error {
// 	//payload, err := json.Marshal(job)

// 	// if err != nil {
// 	// 	return fmt.Errorf("marshal job: %w", err)
// 	// }

// 	tx, err := s.pool.Begin(ctx)
// 	if err != nil {
// 		return fmt.Errorf("begin job transaction: %w", err)
// 	}

// 	defer tx.Rollback(context.Background())

// 	// For idempotency, job will fail insert if we try to insert a duplicate job ID.

// 	_, err = tx.Exec(ctx,
// 		InsertJobSql,
// 		pgx.NamedArgs{
// 			"id":      job.ID,
// 			"user_id": job.User.ID,
// 			"email":   job.User.Email,
// 			//"created_at": job.CreatedAt,
// 		})

// 	if err != nil {
// 		return fmt.Errorf("insert job: %w", err)
// 	}

// 	err = insertJobFiles(ctx, tx, job.ID, job.Files)

// 	if err != nil {
// 		return fmt.Errorf("update job file: %w", err)
// 	}

// 	if err := tx.Commit(ctx); err != nil {
// 		return fmt.Errorf("commit job transaction: %w", err)
// 	}

// 	mailItem := NewJobSubmittedMailItem(job)

// 	if err := s.emailQueue.SendMail(ctx, mailItem); err != nil {
// 		return fmt.Errorf("send email for job %s: %w", job.ID, err)
// 	}

// 	return nil
// }

func (s *PostgresStore) InsertJob(ctx context.Context, job *jobs.Job) error {

	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin job transaction: %w", err)
	}

	defer tx.Rollback(context.Background())

	log.Debug().Msgf("Inserting job:  %s", InsertJobSql)

	args := pgx.NamedArgs{
		"id":        job.ID,
		"status_id": JobStatusMap[job.Status],
		//"created_at": q.Job.CreatedAt,
	}

	var sql string

	if job.Status == jobs.JobStatusSubmitted {
		typeID, ok := JobTypeMap[job.Type]
		if !ok {
			return fmt.Errorf("unknown job type: %s", job.Type)
		}

		sql = InsertJobSql

		// Insertions need extra fields for new jobs
		args["user_id"] = job.User.ID
		args["email"] = job.User.Email
		args["type_id"] = typeID
	} else {
		// We need only basic job info
		sql = UpdateJobStatusSql
	}

	log.Debug().Msgf("Inserting job with args: %v", args)

	// For idempotency, job will fail insert if we try to insert a duplicate job ID.
	_, err = tx.Exec(ctx,
		sql,
		args)

	if err != nil {
		log.Error().Err(err).Msg("Failed to insert job")
		return fmt.Errorf("insert job: %w", err)
	}

	log.Debug().Msg("Inserting job files")

	// Insert job files associated with the job
	err = insertJobFiles(ctx, tx, job.ID, job.Files)

	if err != nil {
		log.Error().Err(err).Msg("Failed to insert job files")
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

	mailItem := jobs.NewJobMailItem(job)

	log.Debug().Msgf("Sending job email xx: %v %v %v %v %v %v", mailItem.EmailType, job.Type, job.Status,
		job.User, mailItem.From, mailItem.To)

	if err := s.emailQueue.SendMail(ctx, mailItem); err != nil {
		return fmt.Errorf("send email for job %s: %w", job.ID, err)
	}

	return nil
}

// func (s *PostgresStore) UpdateJobStatus(ctx context.Context, jobID string, status JobStatus) error {

// 	statusID, ok := JobStatusMap[status]
// 	if !ok {
// 		return fmt.Errorf("unknown job status: %s", status)
// 	}

// 	tx, err := s.pool.Begin(ctx)
// 	if err != nil {
// 		return fmt.Errorf("begin job transaction: %w", err)
// 	}

// 	defer tx.Rollback(context.Background())

// 	_, err = tx.Exec(ctx,
// 		UpdateJobStatusSql,
// 		pgx.NamedArgs{
// 			"id":        jobID,
// 			"status_id": statusID,
// 		})

// 	if err != nil {
// 		return fmt.Errorf("update job status: %w", err)
// 	}

// 	var userID string
// 	var jobType string
// 	var email string

// 	err = tx.QueryRow(ctx,
// 		SelectJobUserInfoSql,
// 		pgx.NamedArgs{
// 			"id": jobID,
// 		}).Scan(&userID, &jobType, &email)

// 	if err != nil {
// 		return fmt.Errorf("select job user and email: %w", err)
// 	}

// 	if err := tx.Commit(ctx); err != nil {
// 		return fmt.Errorf("commit job transaction: %w", err)
// 	}

// 	mailItem := NewJobMailItem(job)

// 	if err := s.emailQueue.SendMail(ctx, mailItem); err != nil {
// 		return fmt.Errorf("send email for job %s: %w", jobID, err)
// 	}

// 	return nil
// }

func insertJobFiles(ctx context.Context, tx pgx.Tx, jobID string, files []*jobs.JobFile) error {
	if len(files) > 0 {
		for _, file := range files {

			log.Debug().Msgf("Inserting job file: %v", file)

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
					//"bucket":       file.Bucket,
					"object_key":   file.ObjectKey,
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
