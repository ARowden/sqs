This project is a Queue based on AWS's Simpile Queue Service with a dead simple API so that anyone, without knowledge of SQS, can 
be up and running with in in minutes. 

For example, to retrive and delete a batch of messages with the AWS SDK, it would require...

```golang
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

	for _, message := range messages {
		newEntry := &sqs.DeleteMessageBatchRequestEntry{
			ReceiptHandle: message.ReceiptHandle,
			Id:            message.MessageId,
		}
		entries = append(entries, newEntry)
	}
  
  	items := convertItemsToAwsMessages(messages)
	entries := makeDeleteMessageBatchRequestEntry(items)
	request := &sqs.DeleteMessageBatchInput{
		Entries:  entries,
		QueueUrl: q.URL,
	}

	output, err := q.svc.DeleteMessageBatch(request)
```
Which has been simplified to...

```golang
items, err := queue.PopBatch()
// or
item, err := queue.Pop()
```
### Getting started
#### Get the server set up with AWS access
You will need an AWS account and permissions granted to the server you will be running from. For a simple configuration, create an IAM user
and grant create an IAM user and grant the minimum privileges they will require (SQS Read, Delete, Write). Next install the [AWS CLI tool](https://docs.aws.amazon.com/cli/latest/userguide/installing.html) 
and run `awscli --config`, here you'll give the access key and seceret access key from the IAM role (this is not a best practice for secuirty).

#### Creating a queue
To initialize a new queue, you need to create a configuration with the the name of the queue, the AWS region of the queue and the visibility timeout for each message. 
The visability timeout is the amount of time you have to process a pulled message before it reappears in the queue. Depending on your workflow you may not need this, but it allows a
simple way to retry processing a message if an error occurs when processing it. [List of regions](https://docs.aws.amazon.com/general/latest/gr/rande.html#sqs_region).
```golang
config := sqs.Config{
    VisibilityTimeoutSeconds: 10,
    Region: "aws-west-1",
    Name: "TestQueue",
}

queue, err := NewQueue(config)
```
