package sqs

import (
	"log"
	"os"
	"testing"
	"time"
)

var queue *Queue

const (
	VisibilityTimeout = 5
	TestQueue         = "TEST_QUEUE"
	Region            = "us-west-2"
)

func setup() {
	// Create a queue for testing.
	err := CreateQueue(TestQueue, Region)
	if err != nil {
		log.Panic(err.Error())
	}

	waitForQueueCreation()

	// Create global queue for test cases.
	config := &Config{
		VisibilityTimeoutSeconds: VisibilityTimeout,
		Region: Region,
		Name:   TestQueue,
	}

	queue, err = NewQueue(config)
	if err != nil {
		log.Panic("Error: ", err.Error())
	}
}

func cleanup() {
	// Delete the test queue.
	err := DeleteQueue(TestQueue, Region)
	if err != nil {
		log.Panic(err.Error())
	}
}

func TestMain(m *testing.M) {
	setup()
	code := m.Run()
	cleanup()
	os.Exit(code)
}

func TestCreateQueue(t *testing.T) {
	queueName := "TEST"
	CreateQueue(queueName, Region)
	waitForQueueCreation()
	exists, err := QueueExists(queueName, Region)
	if err != nil {
		t.Error(err.Error())
		return
	}

	if !exists {
		t.Error("CreateQueue failed")
		return
	}
}

func TestDeleteQueue(t *testing.T) {
	queueName := "TEST"
	err := DeleteQueue(queueName, Region)
	if err != nil {
		t.Error(err.Error())
		return
	}

	waitForQueueCreation()
	exists, err := QueueExists(queueName, Region)
	if err != nil {
		t.Error(err.Error())
		return
	}

	if exists {
		t.Error("DeleteQueue failed.")
	}
}

func TestInsert(t *testing.T) {
	lenBefore := getExactLen(queue)

	insertItem(t)

	lenAfter := getExactLen(queue)

	if lenBefore+1 != lenAfter {
		t.Error("Insert fail")
	}
}

func TestInsertBatch(t *testing.T) {
	itemsInserted := 10
	expectedLen := getExactLen(queue) + itemsInserted

	var items []string
	for i := 0; i < itemsInserted; i++ {
		items = append(items, "test_messages")
	}

	err := queue.InsertBatch(items)
	if err != nil {
		t.Error(err.Error())
		return
	}

	actualLen := getExactLen(queue)
	if expectedLen != actualLen {
		t.Errorf("Inserted %d items but only %d succeeded.", itemsInserted, (actualLen - expectedLen))
	}
}

func TestDelete(t *testing.T) {
	insertItem(t)
	lenBefore := getExactLen(queue)

	item, err := queue.Peek()
	if err != nil {
		t.Error(err.Error())
		return
	}

	err = queue.Delete(item)
	if err != nil {
		t.Error(err.Error())
		return
	}

	lenAfter := getExactLen(queue)
	if lenBefore != lenAfter+1 {
		t.Error("Delete failed.")
	}
}

func TestDeleteBatch(t *testing.T) {
	insertItems(t)
	items, err := queue.PeekBatch()
	if err != nil {
		t.Error(err.Error())
		return
	}

	expectedLen := getExactLen(queue) - len(items)
	err = queue.DeleteBatch(items)
	if err != nil {
		t.Error(err.Error())
		return
	}

	if getExactLen(queue) != expectedLen {
		t.Error("Delete batch failed.")
	}
}

func TestPeek(t *testing.T) {
	insertItem(t)
	result, err := queue.Peek()
	if err != nil {
		t.Error(err.Error())
	}

	if result == nil {
		t.Error("Test peek failed")
	}
}

func TestClear(t *testing.T) {
	insertItems(t)

	err := queue.Clear()
	if err != nil {
		t.Error(err.Error())
		return
	}

	if getExactLen(queue) != 0 {
		t.Error("Clear failed.")
	}
}

func TestPeekBatch(t *testing.T) {
	insertItems(t)

	result, err := queue.PeekBatch()
	if err != nil {
		t.Error(err.Error())
		return
	}

	if len(result) < 2 {
		t.Errorf("Only received %v items. Expected 2 - 10.", len(result))
	}

	t.Logf("Batch peek returned %v items.", len(result))
}

func TestPop(t *testing.T) {
	_, err := queue.Pop()
	if err != nil {
		t.Error(err.Error())
	}
}

func TestPopBatch(t *testing.T) {
	_, err := queue.PopBatch()
	if err != nil {
		t.Error(err.Error())
	}
}

func insertItem(t *testing.T) {
	err := queue.Insert("Test item")
	if err != nil {
		t.Error(err.Error())
		return
	}
}

func insertItems(t *testing.T) {
	var items []string
	for i := 0; i < 10; i++ {
		items = append(items, "test_messages")
	}

	err := queue.InsertBatch(items)
	if err != nil {
		t.Error(err.Error())
	}
}

func getExactLen(q *Queue) int {
	waitForAccurateLen()
	return q.ApproximateLen()
}

func waitForAccurateLen() {
	time.Sleep(time.Second * 30)
}

func waitForQueueCreation() {
	time.Sleep(time.Second * 60)
}
