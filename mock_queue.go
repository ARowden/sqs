package sqs

import (
	"strconv"

	"github.com/aws/aws-sdk-go/service/sqs"
)

type mockService struct {
	queue []string
}

// newMockQueue returns a mock queue for testing without SQS or network connection.
func newMockQueue(config *Config) (*Queue, error) {
	var q Queue
	if invalidVisibilityTimeout(config.VisibilityTimeoutSeconds) {
		return nil, ErrInvalidVisibilityTimeout
	}

	q.VisibilityTimeoutSeconds = config.VisibilityTimeoutSeconds
	q.svc = &mockService{}
	return &q, nil
}

func (m *mockService) SendMessage(input *sqs.SendMessageInput) (*sqs.SendMessageOutput, error) {
	m.queue = append(m.queue, *input.MessageBody)
	return nil, nil
}

func (m *mockService) SendMessageBatch(input *sqs.SendMessageBatchInput) (*sqs.SendMessageBatchOutput, error) {
	for _, input := range input.Entries {
		m.queue = append(m.queue, *input.MessageBody)
	}

	return nil, nil
}

func (m *mockService) DeleteMessage(input *sqs.DeleteMessageInput) (*sqs.DeleteMessageOutput, error) {
	m.queue = m.queue[1:]
	return nil, nil
}

func (m *mockService) DeleteMessageBatch(input *sqs.DeleteMessageBatchInput) (*sqs.DeleteMessageBatchOutput, error) {
	m.queue = m.queue[len(input.Entries):]
	return nil, nil
}

func (m *mockService) GetQueueAttributes(input *sqs.GetQueueAttributesInput) (*sqs.GetQueueAttributesOutput, error) {
	attributes := make(map[string]*string)
	numMessages := strconv.Itoa(len(m.queue))
	attributes["ApproximateNumberOfMessages"] = &numMessages
	response := sqs.GetQueueAttributesOutput{Attributes: attributes}
	return &response, nil
}

func (m *mockService) PurgeQueue(input *sqs.PurgeQueueInput) (*sqs.PurgeQueueOutput, error) {
	m.queue = []string{}
	return nil, nil
}

func (m *mockService) ReceiveMessage(input *sqs.ReceiveMessageInput) (*sqs.ReceiveMessageOutput, error) {
	var numMessagesToReturn int
	var response sqs.ReceiveMessageOutput
	numMessagesRequested := int(*input.MaxNumberOfMessages)
	numMessagesAvailable := len(m.queue)
	if numMessagesAvailable > numMessagesRequested {
		numMessagesToReturn = numMessagesRequested
	} else {
		numMessagesToReturn = numMessagesAvailable
	}

	var handle string
	for i := 0; i < numMessagesToReturn; i++ {
		newMessage := m.queue[i]
		newSqsMessage := sqs.Message{
			Body:          &newMessage,
			ReceiptHandle: &handle,
		}
		response.Messages = append(response.Messages, &newSqsMessage)
	}
	return &response, nil
}
