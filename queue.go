// Package sqs implements an unordered queue for distributed systems based on AWS Simple Queue Service.
package sqs

import (
	"errors"
	"math/rand"
	"strconv"
	"time"

	"strings"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sqs"
)

var (
	// ErrInvalidVisibilityTimeout indicates the set VisibilityTimeout was 0. When this happens the same
	// messages will be returned multiple times even within the same batch request.
	ErrInvalidVisibilityTimeout = errors.New("VisibilityTimeout must be greater than 0")
)

// Item is the data type stored in the queue.
type Item sqs.Message

// Config struct contains required parameters to create a Queue object.
//
// VisibilityTimeout - The amount of time after receiving an item before it can be pulled from the queue again. This
//                     should be enough time to process and delete the message. This must be greater than 0. Setting to
//                     0 will make it impossible to ever process a message and the same message will be returned
//                     multiple times in the same batch request.
//
// Region            - The AWS Region the queue is in. Ex. 'us-west-2'. For a list of regions visit:
//                     https://docs.aws.amazon.com/general/latest/gr/rande.html#sqs_region
//
// Name              - The name of the Simple Queue Service.
type Config struct {
	VisibilityTimeout int
	Region            string
	Name              string
}

// Queue holds aws sqs properties required to send/receive sqs messages via the aws sdk.
type Queue struct {
	VisibilityTimeout int
	svc               *sqs.SQS
	Name              *string
	Region            *string
	URL               *string
}

// NewQueue returns a new SqsHandle with service and queue URL set.
func NewQueue(config Config) (*Queue, error) {
	var q Queue
	var err error

	if invalidVisibilityTimeout(config.VisibilityTimeout) {
		return nil, ErrInvalidVisibilityTimeout
	}

	q.VisibilityTimeout = config.VisibilityTimeout
	q.svc, err = getService(&config.Region)
	if err != nil {
		return nil, err
	}

	q.URL, err = getQueueURL(config.Name, config.Region)
	return &q, err
}

// ApproximateLen returns approximately the number of items in the queue. This attribute lags the actual queue size by
// up to 30 seconds. After 30 seconds of no activity, it will
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

// Insert takes a string and inserts it into the queue.
func (q *Queue) Insert(input string) error {
	request := &sqs.SendMessageInput{
		MessageBody: &input,
		QueueUrl:    q.URL,
	}

	_, err := q.svc.SendMessage(request)
	return err
}

// InsertBatch takes up to 10 items and inserts them into the queue.
func (q *Queue) InsertBatch(inputs []string) error {
	entries := q.makeBatchRequestEntries(inputs)
	request := &sqs.SendMessageBatchInput{
		Entries:  entries,
		QueueUrl: q.URL,
	}

	_, err := q.svc.SendMessageBatch(request)
	return err
}

// Delete takes a single item and removes it from the queue.
func (q *Queue) Delete(message *Item) error {
	request := &sqs.DeleteMessageInput{
		QueueUrl:      q.URL,
		ReceiptHandle: message.ReceiptHandle,
	}

	_, err := q.svc.DeleteMessage(request)
	return err
}

// DeleteBatch deletes a batch of up to 10 sqs messages.
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

// Peek returns an item from the queue but does not delete it. If the queue is empty nil is returned.
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

// PeekBatch returns up to 10 items from the queue but does not delete them.
func (q *Queue) PeekBatch() ([]*Item, error) {
	messages, err := q.receiveNitems(10)
	items := convertAwsMessagesToItems(messages.Messages)
	return items, err
}

// Pop retrieves an item from the queue and deletes it.
func (q *Queue) Pop() (*Item, error) {
	items, err := q.receiveNitems(1)
	if err != nil {
		return nil, err
	}

	messages := convertAwsMessagesToItems(items.Messages)
	if len(messages) == 0 {
		return nil, nil
	}

	item := messages[0]
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
// sub second times.
func (q *Queue) Clear() error {
	err := q.purge()
	if err != nil {
		return err
	}

	q.waitForPurgeCompletion()
	return nil
}

// CreateQueue creates a new queue.
func CreateQueue(name, region string) error {
	svc, err := getService(&region)
	if err != nil {
		return err
	}

	input := &sqs.CreateQueueInput{
		QueueName: &name,
	}

	_, err = svc.CreateQueue(input)
	return err
}

// DeleteQueue deletes the specified queue from AWS.
func DeleteQueue(name, region string) error {
	url, err := getQueueURL(name, region)
	if err != nil {
		return err
	}

	svc, err := getService(&region)
	if err != nil {
		return err
	}

	request := &sqs.DeleteQueueInput{QueueUrl: url}

	_, err = svc.DeleteQueue(request)
	return err
}

// QueueExists returns true if the queue exists, False otherwise.
func QueueExists(name, region string) (bool, error) {
	request := &sqs.ListQueuesInput{
		QueueNamePrefix: aws.String(name),
	}

	svc, err := getService(&region)
	if err != nil {
		return false, err
	}

	output, err := svc.ListQueues(request)
	if err != nil {
		return false, err
	}

	if !queueInQueueList(output, name) {
		return false, nil
	}

	return true, nil
}

func queueInQueueList(queueList *sqs.ListQueuesOutput, name string) bool {
	for _, url := range queueList.QueueUrls {
		if strings.HasSuffix(*url, name) {
			return true
		}
	}

	return false
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
		VisibilityTimeout:   aws.Int64(int64(q.VisibilityTimeout)),
		WaitTimeSeconds:     aws.Int64(20),
	})
	return result, err
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

// makeBatchRequestEntries takes a slice of string items and returns what can be used as a request to aws
// to insert the items into the queue.
func (q *Queue) makeBatchRequestEntries(items []string) (entries []*sqs.SendMessageBatchRequestEntry) {
	for _, item := range items {
		id := generateRandomID()
		newEntry := &sqs.SendMessageBatchRequestEntry{
			Id:          id,
			MessageBody: &item,
		}
		entries = append(entries, newEntry)
	}

	return entries
}

// getService returns an AWS SQS service for the specified region.
func getService(region *string) (*sqs.SQS, error) {
	s, err := session.NewSession(&aws.Config{Region: region})
	return sqs.New(s), err
}

// getQueueURL returns the URL for the specified queue name in the given region.
func getQueueURL(name, region string) (*string, error) {
	svc, err := getService(&region)
	if err != nil {
		return nil, err
	}

	input := sqs.GetQueueUrlInput{QueueName: &name}
	result, err := svc.GetQueueUrl(&input)
	return result.QueueUrl, err
}

// invalidVisibilityTimeout returns true if the timeout is equal to 0. This is a valid setting for AWS, but makes the
// queue unusable.
func invalidVisibilityTimeout(timeout int) bool {
	return timeout == 0
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

// generateRandomID generates random string that can be used with messageID and groupID fields.
func generateRandomID() *string {
	var letters = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")
	b := make([]rune, 15)
	for i := range b {
		b[i] = letters[rand.Intn(len(letters))]
	}

	return aws.String(string(b))
}