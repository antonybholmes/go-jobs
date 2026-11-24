package forms

import (
	"errors"

	"uuid"

	"github.com/antonybholmes/go-jobs"
	"github.com/antonybholmes/go-jobs/aws"
	"github.com/antonybholmes/go-sys/log"
	webforms "github.com/antonybholmes/go-web/forms"
	"github.com/antonybholmes/go-web/middleware"
	"github.com/gin-gonic/gin"
)

type (
	// SubmitHandler handles the submission of jobs, including validating metadata,
	// uploading files to the object store, and sending jobs to the job queue.
	SubmitHandler struct {
		jobQueue    jobs.JobQueue
		objectStore jobs.ObjectStore
		form        *webforms.FormParser
		jobType     string
		bucket      string
	}
)

func NewSubmitJobHandler(jobType string,
	jobQueue jobs.JobQueue,
	objectStore jobs.ObjectStore,
	bucket string,
	maxBody int64) *SubmitHandler {
	return &SubmitHandler{
		jobType:     jobType,
		jobQueue:    jobQueue,
		objectStore: objectStore,
		bucket:      bucket,
		form:        webforms.NewFormParser(maxBody),
	}
}

func (h *SubmitHandler) SubmitJob(c *gin.Context) error {
	user, err := middleware.GetJwtUser(c)

	if err != nil {
		c.Error(err)
		return err
	}

	formFiles, err := h.form.GetFormFiles(c, "file")

	if err != nil {
		return err
	}

	// Close opened files and remove multipart temporary files.
	cleanup := func() {
		for _, formFile := range formFiles {
			_ = formFile.Close()
		}
	}

	// defer cleanup to ensure files are closed on all return paths.
	defer cleanup()

	id := uuid.NewV7()

	userID := user.Subject

	job := jobs.Job{
		ID:   id.String(),
		Type: h.jobType,
		User: jobs.JobUser{
			ID:    userID,
			Email: user.Email,
		},
		Status: jobs.JobStatusSubmitted,
		//CreatedAt: time.Now().UTC(),
	}

	for _, formFile := range formFiles {
		key := aws.MakeUploadKey(&job, formFile.Filename)

		jobFile := jobs.NewJobFile("input",
			h.bucket,
			key,
			formFile.Filename,
			formFile.ContentType,
			formFile.Size,
			formFile.Content,
		)

		// add file to job
		job.Files = append(job.Files, jobFile)
	}

	// Store
	if err := h.objectStore.Upload(c.Request.Context(), &job); err != nil {
		log.Error().Msgf("upload job file: job_id=%s error=%v", job.ID, err)

		// The upload failed, so remove any partially uploaded objects.
		cleanup()
		return errors.New("file upload failed")

	}

	log.Debug().Msgf("sending job of type %s", job.Type)

	// Send the job to the queue
	if err := h.jobQueue.SendJob(c.Request.Context(), &job); err != nil {
		log.Error().Msgf("send job to queue: job_id=%s error=%v", job.ID, err)
		cleanup()
		return errors.New("could not send job to queue")
	}

	// if err := h.jobStore.CreateJob(c.Request.Context(), j); err != nil {
	// 	// A failed commit can have an unknown outcome, so deleting the object here
	// 	// could destroy a successfully committed job's input.
	// 	log.Error().Msgf("persist uploaded job; object may need reconciliation: job_id=%s bucket=%s key=%s error=%v", j.ID, j.Bucket, j.Key, err)
	// 	web.InternalErrorResp(c, errors.New("could not persist job"))
	// 	return
	// }

	return nil
}

// Gin version of SubmitJob which can be used as a Gin route handler.
func (h *SubmitHandler) SubmitJobHandler(c *gin.Context) {
	if err := h.SubmitJob(c); err != nil {
		c.Error(err)
		c.Abort()
	}
}
