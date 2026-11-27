package aws

import (
	"github.com/antonybholmes/go-jobs"
	awssys "github.com/antonybholmes/go-sys/aws"
)

type (
	// CloudFrontJobStore provides signed URLs for job files stored in CloudFront.
	CloudFrontJobStore struct {
		cloudfrontStore *awssys.CloudFrontStore
	}
)

func NewCloudFrontJobStore(cloudfrontStore *awssys.CloudFrontStore) *CloudFrontJobStore {
	return &CloudFrontJobStore{
		cloudfrontStore: cloudfrontStore,
	}
}

// Generate a presigned url for a job file valid for 1 hour
func (s *CloudFrontJobStore) JobFileOneHourPresignedUrl(file *jobs.JobFile) (*awssys.SignedURL, error) {
	return s.cloudfrontStore.OneHourPresignedURL(file.ObjectKey)
}
