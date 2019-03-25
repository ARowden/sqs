// Package sqs implements an unordered queue based on AWS Simple Queue Service.
package sqs

import (
	"errors"
	"math/rand"
	"strconv"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sqs"
)

// Config contains required parameters to create a Queue.
type Config struct {
	// The amount of time after receiving an item before it can be pulled from the queue again. This
	// should be enough time to process and delete the message. This must be greater than 0.
	VisibilityTimeoutSeconds int
	// AWS Region the queue is in. Ex. 'us-west-2'. For a list of regions visit:
	// https://docs.aws.amazon.com/general/latest/gr/rande.html#sqs_region
	Region string
	// Name of the Simple Queue Service.
	Name string
}

// awsAPI interface can be used by a SQS backed queue and the mock queue for testing/development.
type awsAPI interface {
	SendMessage(input *sqs.SendMessageInput) (*sqs.SendMessageOutput, error)
	SendMessageBatch(input *sqs.SendMessageBatchInput) (*sqs.SendMessageBatchOutput, error)
	DeleteMessage(input *sqs.DeleteMessageInput) (*sqs.DeleteMessageOutput, error)
	DeleteMessageBatch(input *sqs.DeleteMessageBatchInput) (*sqs.DeleteMessageBatchOutput, error)
	GetQueueAttributes(input *sqs.GetQueueAttributesInput) (*sqs.GetQueueAttributesOutput, error)
	PurgeQueue(input *sqs.PurgeQueueInput) (*sqs.PurgeQueueOutput, error)
	ReceiveMessage(input *sqs.ReceiveMessageInput) (*sqs.ReceiveMessageOutput, error)
}

// Queue implements a queue based on AWS Simple Queue Service.
type Queue struct {
	config Config
	url    string
	svc    awsAPI
}

// NewQueue creates a new Queue.
func NewQueue(config Config, opts ...func(*Queue)) (*Queue, error) {
	if invalidVisibilityTimeout(config.VisibilityTimeoutSeconds) {
		return nil, errors.New("error: visibility timeout must be greater than 0")
	}

	var q Queue
	var err error
	q.config = config
	q.svc, err = getService(config.Region)
	if err != nil {
		return nil, err
	}

	q.url, err = queueURL(config.Name, config.Region)
	if err != nil {
		return nil, err
	}

	return &q, err
}

// Insert inserts a string into the queue.
func (q *Queue) Insert(input string) error {
	request := &sqs.SendMessageInput{
		MessageBody: &input,
		QueueUrl:    &q.url,
	}

	_, err := q.svc.SendMessage(request)
	return err
}

// InsertBatch inserts up to 10 strings into the queue.
func (q *Queue) InsertBatch(inputs []string) error {
	entries := makeBatchRequestEntries(inputs)
	request := &sqs.SendMessageBatchInput{
		Entries:  entries,
		QueueUrl: &q.url,
	}

	_, err := q.svc.SendMessageBatch(request)
	return err
}

// Delete takes a single Item and removes it from the queue.
func (q *Queue) Delete(msg *sqs.Message) error {
	request := &sqs.DeleteMessageInput{
		QueueUrl:      &q.url,
		ReceiptHandle: msg.ReceiptHandle,
	}

	_, err := q.svc.DeleteMessage(request)
	return err
}

// DeleteBatch deletes a batch of up to 10 Items.
func (q *Queue) DeleteBatch(items []*sqs.Message) error {
	entries := makeDeleteMsgBatchRequestEntry(items)
	request := &sqs.DeleteMessageBatchInput{
		Entries:  entries,
		QueueUrl: &q.url,
	}

	_, err := q.svc.DeleteMessageBatch(request)
	return err
}

// Peek returns an Item from the queue but does not delete it. If the Item is not deleted within the
// visibility timeout it could be received again or received by another instance of the queue. If
// the queue is empty nil is returned.
func (q *Queue) Peek() (*sqs.Message, error) {
	resp, err := q.receiveNitems(1)
	if err != nil {
		return nil, err
	}

	if len(resp.Messages) == 0 {
		return nil, nil
	}

	return resp.Messages[0], nil
}

// PeekBatch returns up to 10 Items from the queue put does not delete them. If the Item is not
// deleted within the visibility timeout it could be received again or received by another instance
// of the queue. If the queue is empty nil is returned.
func (q *Queue) PeekBatch() ([]*sqs.Message, error) {
	resp, err := q.receiveNitems(10)
	if err != nil {
		return nil, err
	}

	return resp.Messages, nil
}

// Pop retrieves an Item from the queue, deletes it from the queue and returns it.
func (q *Queue) Pop() (*sqs.Message, error) {
	msg, err := q.Peek()
	if err != nil {
		return nil, err
	}

	err = q.Delete(msg)
	if err != nil {
		return nil, err
	}
	return msg, err
}

// PopBatch retrieves a batch of up to 10 messages from the queue, deletes them from the queue and
// returns them.
func (q *Queue) PopBatch() ([]*sqs.Message, error) {
	var Msgs []*sqs.Message
	Msgs, err := q.PeekBatch()
	if err != nil {
		return Msgs, err
	}

	err = q.DeleteBatch(Msgs)
	return Msgs, err
}

// Clear clears contents of queue and waits for completion (takes up to 60 seconds according to AWS
// spec), but averages sub second times. This may only be called once every 60 seconds. An error
// will be returned if called twice in the same minute.
func (q *Queue) Clear() error {
	err := q.purge()
	if err != nil {
		return err
	}

	q.waitForPurgeCompletion()
	return nil
}

// ApproximateLen returns approximately the number of items in the queue. This attribute can lag the
// actual queue size by up to 30 seconds.
func (q *Queue) ApproximateLen() int {
	queueLenAttribute := aws.String("ApproximateNumberOfMessages")
	request := &sqs.GetQueueAttributesInput{
		QueueUrl:       &q.url,
		AttributeNames: []*string{queueLenAttribute},
	}

	response, _ := q.svc.GetQueueAttributes(request)
	lengthAttribute := response.Attributes[*queueLenAttribute]
	length, _ := strconv.Atoi(*lengthAttribute)
	return length
}

// purge clears the contents of the queue.
func (q *Queue) purge() error {
	request := &sqs.PurgeQueueInput{
		QueueUrl: &q.url,
	}

	_, err := q.svc.PurgeQueue(request)
	return err
}

// waitForPurgeCompletion waits for the size of the queue to be 0 after calling purge.
func (q *Queue) waitForPurgeCompletion() {
	for q.ApproximateLen() > 0 {
		time.Sleep(50 * time.Millisecond)
	}
}

// receiveNitems returns specified number of items. Must be between 1 - 10.
func (q *Queue) receiveNitems(n int) (*sqs.ReceiveMessageOutput, error) {
	result, err := q.svc.ReceiveMessage(&sqs.ReceiveMessageInput{
		AttributeNames: []*string{
			aws.String(sqs.MessageSystemAttributeNameSentTimestamp),
		},
		MessageAttributeNames: []*string{
			aws.String(sqs.QueueAttributeNameAll),
		},
		QueueUrl:            &q.url,
		MaxNumberOfMessages: aws.Int64(int64(n)),
		VisibilityTimeout:   aws.Int64(int64(q.config.VisibilityTimeoutSeconds)),
		WaitTimeSeconds:     aws.Int64(20),
	})
	return result, err
}

// makeBatchRequestEntries takes a slice of string items and returns what can be used as a request
// to aws to insert the items into the queue.
func makeBatchRequestEntries(items []string) (entries []*sqs.SendMessageBatchRequestEntry) {
	for _, item := range items {
		id := randomID()
		newEntry := &sqs.SendMessageBatchRequestEntry{
			Id:          id,
			MessageBody: &item,
		}
		entries = append(entries, newEntry)
	}

	return entries
}

// makeDeleteMsgBatchRequestEntry takes a slice of sqs messages and converts them into a request
// that will delete all of them as a batch.
func makeDeleteMsgBatchRequestEntry(items []*sqs.Message) (entries []*sqs.DeleteMessageBatchRequestEntry) {
	for _, message := range items {
		newEntry := &sqs.DeleteMessageBatchRequestEntry{
			ReceiptHandle: message.ReceiptHandle,
			Id:            message.MessageId,
		}
		entries = append(entries, newEntry)
	}

	return entries
}

// invalidVisibilityTimeout returns true if the timeout is equal to 0. This is a valid setting for
// AWS, but makes the queue unusable.
func invalidVisibilityTimeout(timeout int) bool {
	return timeout == 0
}

// getService returns an AWS SQS service for the specified region.
func getService(region string) (*sqs.SQS, error) {
	s, err := session.NewSession(&aws.Config{Region: &region})
	return sqs.New(s), err
}

// queueURL returns the URL for the specified queue name in the given region.
func queueURL(name, region string) (string, error) {
	svc, err := getService(region)
	if err != nil {
		return "", err
	}

	input := sqs.GetQueueUrlInput{QueueName: &name}
	result, err := svc.GetQueueUrl(&input)
	return *result.QueueUrl, err
}

// randomID generates random string that can be used with messageID and groupID fields.
func randomID() *string {
	var letters = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")
	b := make([]rune, 15)
	for i := range b {
		b[i] = letters[rand.Intn(len(letters))]
	}

	return aws.String(string(b))
}
