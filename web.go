package jobs

import (
	"context"
	"fmt"

	sysdb "github.com/antonybholmes/go-sys/db"
	basemath "github.com/antonybholmes/go-sys/math"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type (
	PostgresWebStore struct {
		emailQueue MailQueue
		pool       *pgxpool.Pool
	}
)

const (
	PageSize = 100

	ListJobsSql = `SELECT 
		id, 
		user_id, 
		email, 
		type_types.name as type, 
		status_types.name as status
		FROM jobs
		JOIN status_types ON jobs.status_id = status_types.id
		JOIN type_types ON jobs.type_id = type_types.id
		WHERE user_id = @user_id::uuid
		LIMIT @limit OFFSET @offset`

	ListJobFilesSql = `SELECT
		jf.id,	
		jf.job_id,
		ft.name as type,
		jf.bucket,
		jf.key,
		jf.filename,
		jf.content_type,
		jf.size
		FROM job_files AS jf
		JOIN file_types AS ft ON jf.type_id = ft.id
		WHERE jf.job_id = ANY(@ids::uuid[])`

	CountJobsSql = `SELECT COUNT(id) FROM jobs WHERE user_id = @user_id::uuid`
)

// Store for the web compoments to list jobs etc.
// This is designed for read only access
func NewPostgresWebStore(pool *pgxpool.Pool) *PostgresWebStore {
	return &PostgresWebStore{
		pool: pool,
	}
}

// CountJobs returns the total number of jobs and pagination statistics for a given user.
// Page size will be clamped to the range [1, PageSize].
func (s *PostgresWebStore) CountJobs(ctx context.Context, userID string, pageSize int) (*sysdb.RecordStats, error) {
	// Must request at least one job per page and no more than PageSize.
	pageSize = basemath.Clamp(pageSize, 1, PageSize)

	var count int

	if err := s.pool.QueryRow(ctx, CountJobsSql, pgx.NamedArgs{
		"user_id": userID,
	}).Scan(&count); err != nil {
		return nil, fmt.Errorf("count jobs: %w", err)
	}

	pages := (count + pageSize - 1) / pageSize

	stats := sysdb.RecordStats{Count: count, Pages: pages, PageSize: pageSize}

	return &stats, nil
}

// List all jobs for a given user.
// Page size will be clamped to the range [1, PageSize].
func (s *PostgresWebStore) ListJobs(ctx context.Context, userID string, page int, pageSize int) ([]*Job, error) {
	// user cannot request more than PageSize jobs per page.
	pageSize = basemath.Clamp(pageSize, 1, PageSize)

	rows, err := s.pool.Query(ctx,
		ListJobsSql,
		pgx.NamedArgs{
			"user_id": userID,
			"limit":   pageSize,
			"offset":  (page - 1) * pageSize,
		})

	if err != nil {
		return nil, fmt.Errorf("list jobs: %w", err)
	}
	defer rows.Close()

	jobs := make([]*Job, 0)

	for rows.Next() {
		var job Job
		if err := rows.Scan(&job.ID, &job.User.ID, &job.User.Email, &job.Type, &job.Status); err != nil {
			return nil, fmt.Errorf("scan job: %w", err)
		}
		jobs = append(jobs, &job)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("list jobs: %w", err)
	}

	// get all files for all jobs and allocate to each job

	ids := make([]string, 0, len(jobs))
	jobMap := make(map[string]*Job, len(jobs))

	for _, job := range jobs {
		ids = append(ids, job.ID)
		jobMap[job.ID] = job
	}

	if len(ids) > 0 {
		fileRows, err := s.pool.Query(ctx, ListJobFilesSql, pgx.NamedArgs{
			"ids": ids,
		})

		if err != nil {
			return nil, fmt.Errorf("list job files: %w", err)
		}

		defer fileRows.Close()

		for fileRows.Next() {
			var jobID string
			var file JobFile
			if err := fileRows.Scan(&file.ID,
				&jobID,
				&file.Type,
				&file.Bucket,
				&file.Key,
				&file.Filename,
				&file.ContentType,
				&file.Size); err != nil {
				return nil, fmt.Errorf("scan job file: %w", err)
			}

			jobMap[jobID].Files = append(jobMap[jobID].Files, &file)
		}

		if err := fileRows.Err(); err != nil {
			return nil, fmt.Errorf("list job files: %w", err)
		}
	}

	return jobs, nil
}
