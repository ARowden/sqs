// Package sqs implements an unordered queue for distributed systems based on AWS Simple Queue Service.
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

var (
	// ErrInvalidVisibilityTimeout indicates the set VisibilityTimeoutSeconds was 0. When this happens the same items
	// will be returned multiple times even within the same batch request.
	ErrInvalidVisibilityTimeout = errors.New("visibility timeout must be greater than 0")
)

// Item is the data type stored in the queue.
type Item sqs.Message

// Config contains required parameters to create a Queue.
type Config struct {
	VisibilityTimeoutSeconds int // The amount of time after receiving an item before it can be pulled from the queue
	// again. This should be enough time to process and delete the message. This must be greater than 0.
	Region string // AWS Region the queue is in. Ex. 'us-west-2'. For a list of regions visit:
	// https://docs.aws.amazon.com/general/latest/gr/rande.html#sqs_region
	Name string // Name of the Simple Queue Service.
}

// Queue implements a queue based on AWS Simple Queue Service.
type Queue struct {
	VisibilityTimeoutSeconds int      // Time after receiving the message before it can be pulled form the queue again.
	Name                     *string  // The name of the queue.
	Region                   *string  // AWS region the queue is located in.
	URL                      *string  // The URL of the queue.
	svc                      *sqs.SQS // Service struct from the SQS SDK.
}

// NewQueue creates a new Queue.
func NewQueue(config Config) (*Queue, error) {
	var q Queue
	var err error

	if invalidVisibilityTimeout(config.VisibilityTimeoutSeconds) {
		return nil, ErrInvalidVisibilityTimeout
	}

	q.VisibilityTimeoutSeconds = config.VisibilityTimeoutSeconds
	q.svc, err = service(&config.Region)
	if err != nil {
		return nil, err
	}

	q.URL, err = queueURL(config.Name, config.Region)
	return &q, err
}

// Insert inserts a string into the queue.
func (q *Queue) Insert(input string) error {
	request := &sqs.SendMessageInput{
		MessageBody: &input,
		QueueUrl:    q.URL,
	}

	_, err := q.svc.SendMessage(request)
	return err
}

// InsertBatch inserts up to 10 strings into the queue.
func (q *Queue) InsertBatch(inputs []string) error {
	entries := makeBatchRequestEntries(inputs)
	request := &sqs.SendMessageBatchInput{
		Entries:  entries,
		QueueUrl: q.URL,
	}

	_, err := q.svc.SendMessageBatch(request)
	return err
}

// Delete takes a single Item and removes it from the queue.
func (q *Queue) Delete(message *Item) error {
	request := &sqs.DeleteMessageInput{
		QueueUrl:      q.URL,
		ReceiptHandle: message.ReceiptHandle,
	}

	_, err := q.svc.DeleteMessage(request)
	return err
}

// DeleteBatch deletes a batch of up to 10 Items.
func (q *Queue) DeleteBatch(messages []*Item) error {
	items := convertItemsToAwsMessages(messages)
	entries := makeDeleteMessageBatchRequestEntry(items)
	request := &sqs.DeleteMessageBatchInput{
		Entries:  entries,
		QueueUrl: q.URL,
	}

	_, err := q.svc.DeleteMessageBatch(request)
	return err
}

// Peek returns an Item from the queue but does not delete it. If the Item is not deleted within the visibility timeout
// it could be received again or received by another instance of the queue. If the queue is empty nil is returned.
func (q *Queue) Peek() (*Item, error) {
	items, err := q.receiveNitems(1)
	if err != nil {
		return nil, err
	}

	localItems := convertAwsMessagesToItems(items.Messages)
	if len(localItems) == 0 {
		return nil, nil
	}

	return localItems[0], nil
}

// PeekBatch returns up to 10 Items from the queue put does not delete them. If the Item is not deleted within the
// visibility timeout it could be received again or received by another instance of the queue. If the queue is empty nil
// is returned.
func (q *Queue) PeekBatch() ([]*Item, error) {
	messages, err := q.receiveNitems(10)
	items := convertAwsMessagesToItems(messages.Messages)
	return items, err
}

// Pop retrieves an Item from the queue, deletes it from the queue and returns it.
func (q *Queue) Pop() (*Item, error) {
	response, err := q.receiveNitems(1)
	if err != nil {
		return nil, err
	}

	items := convertAwsMessagesToItems(response.Messages)
	if len(items) == 0 {
		return nil, nil
	}

	item := items[0]
	err = q.Delete(item)
	return item, err
}

// PopBatch retrieves a batch of up to 10 items from the queue, deletes them from the queue and returns them.
func (q *Queue) PopBatch() ([]*Item, error) {
	messages, err := q.PeekBatch()
	if err != nil {
		return nil, err
	}

	if messages == nil {
		return nil, nil
	}

	err = q.DeleteBatch(messages)
	return messages, err
}

// Clear clears contents of queue and waits for completion (takes up to 60 seconds according to AWS spec), but averages
// sub second times. This may only be called once every 60 seconds. An error will be returned if called twice in the
// same minute.
func (q *Queue) Clear() error {
	err := q.purge()
	if err != nil {
		return err
	}

	q.waitForPurgeCompletion()
	return nil
}

// ApproximateLen returns approximately the number of items in the queue. This attribute can lag the actual queue size
// by up to 30 seconds.
func (q *Queue) ApproximateLen() int {
	queueLenAttribute := aws.String("ApproximateNumberOfMessages")
	request := &sqs.GetQueueAttributesInput{
		QueueUrl:       q.URL,
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
		QueueUrl: q.URL,
	}

	_, err := q.svc.PurgeQueue(request)
	return err
}

// waitForPurgeCompletion waits for the size of the queue to be 0 after calling purge.
func (q *Queue) waitForPurgeCompletion() {
	for q.ApproximateLen() > 0 {
		time.Sleep(50)
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
		QueueUrl:            q.URL,
		MaxNumberOfMessages: aws.Int64(int64(n)),
		VisibilityTimeout:   aws.Int64(int64(q.VisibilityTimeoutSeconds)),
		WaitTimeSeconds:     aws.Int64(20),
	})
	return result, err
}

// convertAwsMessagesToItems converts aws sqs.messages to our local type, Item.
func convertAwsMessagesToItems(awsMessages []*sqs.Message) (messages []*Item) {
	for _, message := range awsMessages {
		newMessage := Item(*message)
		messages = append(messages, &newMessage)
	}
	return messages
}

// convertItemsToAwsMessages converts slice of Items to sqs.Messages
func convertItemsToAwsMessages(messages []*Item) (sqsMessages []*sqs.Message) {
	for _, message := range messages {
		newMessage := sqs.Message(*message)
		sqsMessages = append(sqsMessages, &newMessage)
	}

	return sqsMessages
}

// makeBatchRequestEntries takes a slice of string items and returns what can be used as a request to aws
// to insert the items into the queue.
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

// makeDeleteMessageBatchRequestEntry takes a slice of sqs messages and converts them into a request that will delete
// all of them as a batch.
func makeDeleteMessageBatchRequestEntry(messages []*sqs.Message) (entries []*sqs.DeleteMessageBatchRequestEntry) {
	for _, message := range messages {
		newEntry := &sqs.DeleteMessageBatchRequestEntry{
			ReceiptHandle: message.ReceiptHandle,
			Id:            message.MessageId,
		}
		entries = append(entries, newEntry)
	}

	return entries
}

// invalidVisibilityTimeout returns true if the timeout is equal to 0. This is a valid setting for AWS, but makes the
// queue unusable.
func invalidVisibilityTimeout(timeout int) bool {
	return timeout == 0
}

// service returns an AWS SQS service for the specified region.
func service(region *string) (*sqs.SQS, error) {
	s, err := session.NewSession(&aws.Config{Region: region})
	return sqs.New(s), err
}

// queueURL returns the URL for the specified queue name in the given region.
func queueURL(name, region string) (*string, error) {
	svc, err := service(&region)
	if err != nil {
		return nil, err
	}

	input := sqs.GetQueueUrlInput{QueueName: &name}
	result, err := svc.GetQueueUrl(&input)
	return result.QueueUrl, err
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
