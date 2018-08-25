package sqs

import (
	"strconv"

	"github.com/aws/aws-sdk-go/service/sqs"
)

// MockAPIService implements a mocked AWS SQS API for testing without credentials.
type MockAPIService struct {
	queue []string
}

func (m *MockAPIService) SendMessage(input *sqs.SendMessageInput) (*sqs.SendMessageOutput, error) {
	m.queue = append(m.queue, *input.MessageBody)
	return nil, nil
}

func (m *MockAPIService) SendMessageBatch(input *sqs.SendMessageBatchInput) (*sqs.SendMessageBatchOutput, error) {
	for _, input := range input.Entries {
		m.queue = append(m.queue, *input.MessageBody)
	}

	return nil, nil
}

func (m *MockAPIService) DeleteMessage(input *sqs.DeleteMessageInput) (*sqs.DeleteMessageOutput, error) {
	m.queue = m.queue[1:]
	return nil, nil
}

func (m *MockAPIService) DeleteMessageBatch(input *sqs.DeleteMessageBatchInput) (*sqs.DeleteMessageBatchOutput, error) {
	m.queue = m.queue[len(input.Entries):]
	return nil, nil
}

func (m *MockAPIService) GetQueueAttributes(input *sqs.GetQueueAttributesInput) (*sqs.GetQueueAttributesOutput, error) {
	attributes := make(map[string]*string)
	numMessages := strconv.Itoa(len(m.queue))
	attributes["ApproximateNumberOfMessages"] = &numMessages
	response := sqs.GetQueueAttributesOutput{Attributes: attributes}
	return &response, nil
}

func (m *MockAPIService) PurgeQueue(input *sqs.PurgeQueueInput) (*sqs.PurgeQueueOutput, error) {
	m.queue = []string{}
	return nil, nil
}

func (m *MockAPIService) ReceiveMessage(input *sqs.ReceiveMessageInput) (*sqs.ReceiveMessageOutput, error) {
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
