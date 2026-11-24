package aws

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/antonybholmes/go-jobs"

	awssys "github.com/antonybholmes/go-sys/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	sqstypes "github.com/aws/aws-sdk-go-v2/service/sqs/types"
)

type (
	SQSJobConsumer struct {
		Consumer *awssys.SQSConsumer
	}

	// SQSJobStatusConsumer struct {
	// 	Consumer *awssys.SQSConsumer
	// }

	SQSJobQueue struct {
		Sender *awssys.SQSSender
	}

	// SQSJobStatusQueue struct {
	// 	Sender *awssys.SQSSender
	// }
)

// func NewSQSJobConsumer(consumer *awssys.SQSConsumer) *SQSJobConsumer {
// 	return &SQSJobConsumer{
// 		Consumer: consumer,
// 	}
// }

// // Consume starts consuming messages from the SQS queue.
// // This is a long-running function that continuously polls the queue for messages.
// func (c *SQSJobConsumer) Consume(ctx context.Context,
// 	processJob func(ctx context.Context, job *jobs.Job) error) {
// 	c.Consumer.Consume(ctx, func(ctx context.Context, message sqstypes.Message) error {

// 		messageID := awssys.GetMessageID(message)

// 		var m jobs.Job

// 		if err := json.Unmarshal([]byte(*message.Body), &m); err != nil {
// 			return fmt.Errorf("decode message %s: %w", messageID, err)
// 		}

// 		if err := processJob(ctx, &m); err != nil {
// 			return fmt.Errorf("process job %s: %w", messageID, err)
// 		}

// 		// if err := c.Consumer.DeleteMessage(ctx, message); err != nil {
// 		// 	return fmt.Errorf("delete message %s: %w", messageID, err)
// 		// }

// 		return nil
// 	})
// }

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

// func processSqsMessages(client *sqs.Client, ctx context.Context, resp *sqs.ReceiveMessageOutput) error {

// 	var errs []error
// 	for _, message := range resp.Messages {
// 		if err := processSqsMessage(client, ctx, message); err != nil {
// 			errs = append(errs, err)
// 			continue
// 		}
// 	}

// 	if len(errs) > 0 {
// 		return errors.Join(errs...)
// 	}

// 	return nil
// }

func NewSQSJobQueue(client *sqs.Client, queueUrl string) *SQSJobQueue {
	sender := awssys.NewSQSSender(client, queueUrl)

	return &SQSJobQueue{Sender: sender}
}

// Sends a job message to the SQS queue.
// The message is serialized as JSON and includes message attributes.
func (queue *SQSJobQueue) SendJob(ctx context.Context, job *jobs.Job) error {

	//qi := jobs.JobQueueItem{Job: job, Type: jobs.JobQueueItemTypeSubmit}

	body, err := json.Marshal(job)

	if err != nil {
		return err
	}

	err = queue.Sender.Send(ctx, body)

	return err
}

// func NewSQSJobStatusQueue(client *sqs.Client, queueUrl string) *SQSJobStatusQueue {
// 	sender := awssys.NewSQSSender(client, queueUrl)

// 	return &SQSJobStatusQueue{Sender: sender}
// }

// // Sends a job message to the SQS queue.
// // The message is serialized as JSON and includes message attributes.
// func (queue *SQSJobStatusQueue) SendJobStatus(ctx context.Context, job *jobs.Job) error {

// 	body, err := json.Marshal(job)

// 	if err != nil {
// 		return err
// 	}

// 	err = queue.Sender.Send(ctx, body)

// 	return err
// }
