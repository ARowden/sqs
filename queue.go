// Package sqs implements an unordered queue based on AWS Simple Queue Service.
package sqs

import (
	"math/rand"
	"strconv"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sqs"
)

// Config contains required parameters to create a Queue.
type Config struct {
	// AWS Region the queue is in. Ex. 'us-west-1'. For a list of regions visit:
	// https://docs.aws.amazon.com/general/latest/gr/rande.html#sqs_region
	Region string
	// Name of the Simple Queue Service.
	Name string
	// The amount of time after receiving an item before it can be pulled from the queue again.
	// This should be enough time to process and delete the message. This must be greater than 0.
	VisibilityTimeoutSeconds int
}

// awsAPI interface can be used by a SQS backed queue and the mock queue for testing/development.
//go:generate mockgen -destination=../mocks/queue_client.go -package=mocks github.com/arowden/sqs queueClient
type queueClient interface {
	SendMessage(*sqs.SendMessageInput) (*sqs.SendMessageOutput, error)
	SendMessageBatch(*sqs.SendMessageBatchInput) (*sqs.SendMessageBatchOutput, error)
	DeleteMessage(*sqs.DeleteMessageInput) (*sqs.DeleteMessageOutput, error)
	DeleteMessageBatch(*sqs.DeleteMessageBatchInput) (*sqs.DeleteMessageBatchOutput, error)
	GetQueueAttributes(*sqs.GetQueueAttributesInput) (*sqs.GetQueueAttributesOutput, error)
	PurgeQueue(*sqs.PurgeQueueInput) (*sqs.PurgeQueueOutput, error)
	ReceiveMessage(*sqs.ReceiveMessageInput) (*sqs.ReceiveMessageOutput, error)
	CreateQueue(*sqs.CreateQueueInput) (*sqs.CreateQueueOutput, error)
	DeleteQueue(*sqs.DeleteQueueInput) (*sqs.DeleteQueueOutput, error)
	GetQueueUrl(*sqs.GetQueueUrlInput) (*sqs.GetQueueUrlOutput, error)
}

// Queue implements a queue based on AWS Simple Queue Service.

// TODO make this client
type Queue struct {
	config Config
	client queueClient
	url    string
}

// NewQueue creates a new Queue.
func CreateClient(config Config) (*Queue, error) {
	c := &Queue{config: config}
	s, err := session.NewSession(&aws.Config{Region: &c.config.Region})
	if err != nil {
		return nil, err
	}

	c.client = sqs.New(s)
	err = c.createQueue()
	if err != nil {
		return nil, err
	}

	c.url, err = queueURL(c.config.Name, c.client)
	return c, err
}

// DeleteQueue deletes the specified queue from AWS.
func (q *Queue) DeleteQueue() error {
	req := &sqs.DeleteQueueInput{QueueUrl: &q.url}
	_, err := q.client.DeleteQueue(req)
	return err
}

// Insert inserts a string into the queue.
func (q *Queue) Insert(input string) error {
	request := &sqs.SendMessageInput{
		MessageBody: &input,
		QueueUrl:    &q.url,
	}

	_, err := q.client.SendMessage(request)
	return err
}

// InsertBatch inserts up to 10 strings into the queue.
func (q *Queue) InsertBatch(inputs []string) error {
	entries := makeBatchRequestEntries(inputs)
	request := &sqs.SendMessageBatchInput{
		Entries:  entries,
		QueueUrl: &q.url,
	}

	_, err := q.client.SendMessageBatch(request)
	return err
}

// Delete takes a single Item and removes it from the queue.
func (q *Queue) Delete(msg *sqs.Message) error {
	request := &sqs.DeleteMessageInput{
		QueueUrl:      &q.url,
		ReceiptHandle: msg.ReceiptHandle,
	}

	_, err := q.client.DeleteMessage(request)
	return err
}

// DeleteBatch deletes a batch of up to 10 Items.
func (q *Queue) DeleteBatch(items []*sqs.Message) error {
	entries := makeDeleteMsgBatchRequestEntry(items)
	request := &sqs.DeleteMessageBatchInput{
		Entries:  entries,
		QueueUrl: &q.url,
	}

	_, err := q.client.DeleteMessageBatch(request)
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

// ApproximateLen returns approximately the number of items in the queue. This attribute can lag the
// actual queue size by up to 30 seconds.
func (q *Queue) ApproximateLen() int {
	queueLenAttribute := aws.String("ApproximateNumberOfMessages")
	request := &sqs.GetQueueAttributesInput{
		QueueUrl:       &q.url,
		AttributeNames: []*string{queueLenAttribute},
	}

	response, _ := q.client.GetQueueAttributes(request)
	lengthAttribute := response.Attributes[*queueLenAttribute]
	length, _ := strconv.Atoi(*lengthAttribute)
	return length
}

// Purge clears the contents of the queue.
func (q *Queue) Purge() error {
	request := &sqs.PurgeQueueInput{
		QueueUrl: &q.url,
	}

	_, err := q.client.PurgeQueue(request)
	return err
}

// receiveNitems returns specified number of items. Must be between 1 - 10.
// TODO Nmsgs
func (q *Queue) receiveNitems(n int) (*sqs.ReceiveMessageOutput, error) {
	result, err := q.client.ReceiveMessage(&sqs.ReceiveMessageInput{
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

// createQueue creates a new sqs queue in AWS.
func (q *Queue) createQueue() error {
	req := &sqs.CreateQueueInput{QueueName: &q.config.Name}
	_, err := q.client.CreateQueue(req)
	return err
}

func queueURL(name string, client queueClient) (string, error) {
	req := &sqs.GetQueueUrlInput{QueueName: &name}
	res, err := client.GetQueueUrl(req)
	return *res.QueueUrl, err
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
func makeDeleteMsgBatchRequestEntry(items []*sqs.Message) []*sqs.DeleteMessageBatchRequestEntry {
	var entries []*sqs.DeleteMessageBatchRequestEntry
	for _, message := range items {
		newEntry := &sqs.DeleteMessageBatchRequestEntry{
			ReceiptHandle: message.ReceiptHandle,
			Id:            message.MessageId,
		}
		entries = append(entries, newEntry)
	}

	return entries
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
