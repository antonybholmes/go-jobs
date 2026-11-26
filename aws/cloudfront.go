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

// Generate a url for allowing file downloads
func (s *CloudFrontJobStore) GenerateJobFileUrl(file *jobs.JobFile) (*awssys.SignedURL, error) {
	return s.cloudfrontStore.OneHourSignedURL(file.ObjectKey)
}
