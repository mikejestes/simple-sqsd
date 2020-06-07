package sqsd

import (
	"errors"
	"runtime"
	"sync"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/aws/aws-sdk-go/service/sqs/sqsiface"
)

type SQSWorker struct {
	logger     Logger
	sqs        sqsiface.SQSAPI
	httpClient HTTPClient

	lock    sync.Mutex
	started bool
	wg      sync.WaitGroup
}

func NewSQSWorker(l Logger, s sqsiface.SQSAPI, httpClient HTTPClient) *SQSWorker {
	return &SQSWorker{
		logger:     l,
		sqs:        s,
		httpClient: httpClient,
	}
}

func (s *SQSWorker) Start(n int) error {
	s.lock.Lock()
	defer s.lock.Unlock()

	if s.started {
		return errors.New("already started")
	}

	s.started = true

	if n <= 0 {
		n = runtime.NumCPU()
	}

	if s.logger != nil {
		s.logger.Printf("Starting %d workers", n)
	}

	s.wg.Add(n)

	for i := 0; i < n; i++ {
		go s.worker(i + 1)
	}

	return nil
}

func (s *SQSWorker) Wait() {
	s.wg.Wait()
}

func (s *SQSWorker) Shutdown() {
	s.lock.Lock()
	defer s.lock.Unlock()

	if s.logger != nil {
		s.logger.Print("Shutting down workers")
	}
}

func (s *SQSWorker) worker(id int) {
	if s.logger != nil {
		s.logger.Printf("Worker #%d starting", id)
	}

	for {
		select {
		// case <-s.ctx.Done():
		// 	s.logger.Printf("Worker #%d shutting down", id)
		// 	s.wg.Done()
		// 	return

		default:
			recInput := &sqs.ReceiveMessageInput{
				// MaxNumberOfMessages:   aws.Int64(int64(s.workerConfig.QueueMaxMessages)),
				// QueueUrl:              aws.String(s.workerConfig.QueueURL),
				// WaitTimeSeconds:       aws.Int64(int64(s.workerConfig.QueueWaitTime)),
				MessageAttributeNames: aws.StringSlice([]string{"All"}),
			}

			output, err := s.sqs.ReceiveMessage(recInput)
			if err != nil {
				s.logger.Printf("Error while receiving messages from the queue: %s", err)
				continue
			}

			if len(output.Messages) == 0 {
				continue
			}

			deleteEntries := make([]*sqs.DeleteMessageBatchRequestEntry, 0)
			changeVisibilityEntries := make([]*sqs.ChangeMessageVisibilityBatchRequestEntry, 0)

			for _, msg := range output.Messages {
				res, err := s.httpClient.Request(msg)
				if err != nil {
					s.logger.Printf("Error making HTTP request: %s", err)
					continue
				}

				if !res.Successful {
					if res.RetryAfterSec > 0 {
						changeVisibilityEntries = append(changeVisibilityEntries, &sqs.ChangeMessageVisibilityBatchRequestEntry{
							Id:                msg.MessageId,
							ReceiptHandle:     msg.ReceiptHandle,
							VisibilityTimeout: aws.Int64(res.RetryAfterSec),
						})
					}

					s.logger.Printf("Non-successful HTTP status code received: %d", res.StatusCode)
					continue
				}

				deleteEntries = append(deleteEntries, &sqs.DeleteMessageBatchRequestEntry{
					Id:            msg.MessageId,
					ReceiptHandle: msg.ReceiptHandle,
				})
			}

			if len(deleteEntries) > 0 {
				delInput := &sqs.DeleteMessageBatchInput{
					Entries: deleteEntries,
					// QueueUrl: aws.String(s.workerConfig.QueueURL),
				}

				_, err = s.sqs.DeleteMessageBatch(delInput)
				if err != nil {
					s.logger.Printf("Error while deleting messages from SQS: %s", err)
				}
			}

			if len(changeVisibilityEntries) > 0 {
				changeVisibilityInput := &sqs.ChangeMessageVisibilityBatchInput{
					Entries: changeVisibilityEntries,
					// QueueUrl: aws.String(s.workerConfig.QueueURL),
				}

				_, err = s.sqs.ChangeMessageVisibilityBatch(changeVisibilityInput)
				if err != nil {
					s.logger.Printf("Error while changing visibility on messages: %s", err)
				}
			}
		}
	}
}
