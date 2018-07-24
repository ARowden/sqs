This project is a Queue based on AWS's Simpile Queue Service. The goal of this project was to create a dead simple API for 
Amazons Simpile Queue Service so that anyone, with almost no knowledge of SQS can be up and running in minutes.

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

### SQS creation
```golang
sqs.CreateQueue("QueueName", "AWS_REGION")
```

### Supported Operations:
#### Pop
Receives one item from the queue and deletes it from the queue.
#### PopBatch
Receives up to 10 items from the queue and deletes them from the queue.
#### Peek
Returns one item from the queue, but does not delete it.
#### PeekBatch
Returns up to 10 items from the queue, but does not delete them.
#### Insert
Inserts one item (string) into the queue.
#### InsertBatch
Inserts up to 10 items (strings) into the queue.
#### Clear
Clears all items from the queue. This may take up to 60 seconds and may only be called once every 60 seconds.
#### ApproximateLength
Returns the number of items in the queue, but lags the actual number of items in the queue by up to 30 seconds.
