package sqs

import (
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/sqs"
)

// CreateQueue creates a new queue.
func CreateQueue(name, region string) error {
	svc, err := getService(region)
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
	url, err := queueURL(name, region)
	if err != nil {
		return err
	}

	svc, err := getService(region)
	if err != nil {
		return err
	}

	request := &sqs.DeleteQueueInput{QueueUrl: &url}

	_, err = svc.DeleteQueue(request)
	return err
}

// QueueExists returns true if the queue exists, False otherwise.
func QueueExists(name, region string) (bool, error) {
	request := &sqs.ListQueuesInput{
		QueueNamePrefix: aws.String(name),
	}

	svc, err := getService(region)
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
		if *url == name {
			return true
		}
	}

	return false
}
