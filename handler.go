package jobs

import (
	"errors"
	"net/http"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/antonybholmes/go-sys/log"
	"github.com/antonybholmes/go-web"
	"github.com/antonybholmes/go-web/middleware"
	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
)

func NewSubmitJobHandler(
	jobQueue JobQueue,
	objectStore ObjectStore,
	bucket string,
	maxBody int64) *SubmitHandler {
	return &SubmitHandler{
		jobQueue:    jobQueue,
		objectStore: objectStore,
		bucket:      bucket,
		maxBody:     maxBody,
	}
}

func (h *SubmitHandler) SubmitJob(c *gin.Context) {
	user, err := middleware.GetJwtUser(c)

	if err != nil {
		c.Error(err)
		return
	}

	c.Request.Body = http.MaxBytesReader(c.Writer, c.Request.Body, h.maxBody)

	file, fileHeader, err := c.Request.FormFile("file")

	// clean up but seems unnecessary if no multipart form was created
	if c.Request.MultipartForm != nil {
		defer func() {
			if err := c.Request.MultipartForm.RemoveAll(); err != nil {
				log.Error().Msgf("remove multipart temporary files: %v", err)
			}
		}()
	}

	if err != nil {
		var tooLarge *http.MaxBytesError
		if errors.As(err, &tooLarge) {
			c.JSON(http.StatusRequestEntityTooLarge, gin.H{"error": "request body too large"})
		} else {
			web.BadReqResp(c, errors.New("a multipart file field named file is required"))
		}
		return
	}
	defer file.Close()

	id, err := uuid.NewV7()
	if err != nil {
		log.Error().Msgf("generate job ID: %v", err)

		web.InternalErrorResp(c, errors.New("could not create job"))
		return
	}

	contentType := fileHeader.Header.Get("Content-Type")
	if contentType == "" {
		contentType = "application/octet-stream"
	}

	if !ValidMetadata(fileHeader.Filename, 1024) || !ValidMetadata(contentType, 255) {
		web.BadReqResp(c, errors.New("invalid file metadata"))
		return
	}

	userID := user.Subject

	baseJob := BaseJob{
		ID:        id.String(),
		Type:      "crispr",
		CreatedAt: time.Now().UTC(),
	}

	j := Job{
		BaseJob:     baseJob,
		Bucket:      h.bucket,
		Key:         "users/" + userID + "/jobs/crispr/" + id.String() + "/upload",
		Filename:    fileHeader.Filename,
		ContentType: contentType,
		Size:        fileHeader.Size,
	}

	if err := h.objectStore.Upload(c.Request.Context(), &j, file); err != nil {
		log.Error().Msgf("upload job file: job_id=%s error=%v", j.ID, err)
		c.JSON(http.StatusBadGateway, gin.H{"error": "file upload failed"})
		return
	}

	// Send the job to the queue
	if err := h.jobQueue.SendJob(c.Request.Context(), &j); err != nil {
		log.Error().Msgf("send job to queue: job_id=%s error=%v", j.ID, err)
		web.InternalErrorResp(c, errors.New("could not send job to queue"))
		return
	}

	// if err := h.jobStore.CreateJob(c.Request.Context(), j); err != nil {
	// 	// A failed commit can have an unknown outcome, so deleting the object here
	// 	// could destroy a successfully committed job's input.
	// 	log.Error().Msgf("persist uploaded job; object may need reconciliation: job_id=%s bucket=%s key=%s error=%v", j.ID, j.Bucket, j.Key, err)
	// 	web.InternalErrorResp(c, errors.New("could not persist job"))
	// 	return
	// }

	web.MakeAcceptedResp(c, "job created", gin.H{"job_id": j.ID})

	// c.JSON(http.StatusAccepted, gin.H{"job_id": j.ID})
}

func ValidMetadata(value string, maxBytes int) bool {
	return len(value) <= maxBytes && utf8.ValidString(value) && !strings.ContainsRune(value, 0)
}
