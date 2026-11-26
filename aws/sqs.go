package aws

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/antonybholmes/go-jobs"

	awssys "github.com/antonybholmes/go-sys/aws"
	"github.com/antonybholmes/go-sys/log"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	sqstypes "github.com/aws/aws-sdk-go-v2/service/sqs/types"
)

type (
	SQSJobConsumer struct {
		Consumer *awssys.SQSConsumer
	}

	SQSJobQueue struct {
		Sender *awssys.SQSSender
	}
)

func NewSQSJobConsumer(consumer *awssys.SQSConsumer) *SQSJobConsumer {
	return &SQSJobConsumer{
		Consumer: consumer,
	}
}

// Consume starts consuming messages from the SQS queue.
// This is a long-running function that continuously polls the queue for messages.
func (c *SQSJobConsumer) Consume(ctx context.Context,
	f func(ctx context.Context, job *jobs.Job) error) {
	c.Consumer.Consume(ctx, func(ctx context.Context, message sqstypes.Message) error {

		messageID := awssys.GetMessageID(message)

		var m jobs.Job //QueueItem

		if err := json.Unmarshal([]byte(*message.Body), &m); err != nil {
			return fmt.Errorf("decode message %s: %w", messageID, err)
		}

		if err := f(ctx, &m); err != nil {
			return fmt.Errorf("process job %s: %w", messageID, err)
		}

		return nil
	})
}

func NewSQSJobQueue(client *sqs.Client, queueUrl string) *SQSJobQueue {
	sender := awssys.NewSQSSender(client, queueUrl)

	return &SQSJobQueue{Sender: sender}
}

// Sends a job message to the SQS queue.
// The message is serialized as JSON and includes message attributes.
func (queue *SQSJobQueue) SendJob(ctx context.Context, job *jobs.Job) error {

	//qi := jobs.JobQueueItem{Job: job, Type: jobs.JobQueueItemTypeSubmit}

	log.Debug().Msgf("sending job %s of type %s", job.ID, job.Status)

	body, err := json.Marshal(job)

	if err != nil {
		return err
	}

	err = queue.Sender.Send(ctx, body)

	return err
}
