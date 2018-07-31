package sqs

import (
	"testing"
)

var queue *Queue

const (
	VisibilityTimeout = 5
	TestQueue         = "TEST_QUEUE"
	Region            = "us-west-2"
)

func init() {
	// Create global queue for test cases.
	config := &Config{
		VisibilityTimeoutSeconds: VisibilityTimeout,
		Region: Region,
		Name:   TestQueue,
	}

	queue, _ = newMockQueue(config)
}

func TestInsert(t *testing.T) {
	startingNumMessages := queue.ApproximateLen()

	err := queue.Insert("Test")
	if err != nil {
		t.Errorf("Insert threw error.")
	}

	if queue.ApproximateLen() != startingNumMessages+1 {
		t.Errorf("Insert failed.")
	}
}

func TestInsertBatch(t *testing.T) {
	startingNumMessages := queue.ApproximateLen()

	input := []string{"test", "test", "test", "test", "test"}

	numMessagesAdded := len(input)

	err := queue.InsertBatch(input)
	if err != nil {
		t.Error("InsertBatch failed.")
	}

	if queue.ApproximateLen() != numMessagesAdded+startingNumMessages {
		t.Error("InsertBatch failed.")
	}
}

func TestPeekOnEmptyQueue(t *testing.T) {
	// Start with empty queue
	queue.Clear()

	_, err := queue.Peek()
	if err != nil {
		t.Error(err.Error())
	}
}

func TestPeek(t *testing.T) {
	testItem := "TEST ITEM"
	// Start with empty queue
	queue.Clear()

	// Insert an item to peek
	queue.Insert(testItem)

	item, err := queue.Peek()
	if err != nil {
		t.Error(err.Error())
	}

	if item.String() != testItem {
		t.Error("Peek returned incorrect item.")
		t.Errorf("Actual: %v", item.String())
		t.Errorf("Expected: %v", testItem)
	}
}

func TestPeekBatch(t *testing.T) {
	// Start with empty queue
	queue.Clear()

	testItems := []string{"test", "test", "test", "test", "test"}
	queue.InsertBatch(testItems)

	result, err := queue.PeekBatch()
	if err != nil {
		t.Error(err.Error())
	}

	for i := range testItems {
		if result[i].String() != testItems[i] {
			t.Error("PeekBatch failed.")
		}
	}
}

func TestDelete(t *testing.T) {
	queue.Insert("test item")

	startingNumMessages := queue.ApproximateLen()

	item, err := queue.Peek()
	if err != nil {
		t.Error(err.Error())
	}

	err = queue.Delete(item)
	if err != nil {
		t.Error(err.Error())
	}

	endingNumMessages := queue.ApproximateLen()
	if endingNumMessages+1 != startingNumMessages {
		t.Error("Delete failed.")
	}
}

func TestDeleteBatch(t *testing.T) {
	// Start with empty queue
	queue.Clear()

	testItems := []string{"test", "test", "test", "test", "test"}
	queue.InsertBatch(testItems)

	items, err := queue.PeekBatch()
	if err != nil {
		t.Error(err.Error())
	}

	err = queue.DeleteBatch(items)
	if err != nil {
		t.Error(err.Error())
	}

	if queue.ApproximateLen() != 0 {
		t.Error("DeleteBatch failed.")
	}

}

func TestPop(t *testing.T) {
	testItem := "test item"
	// Start with empty queue
	queue.Clear()

	queue.Insert(testItem)

	item, err := queue.Pop()
	if err != nil {
		t.Error(err.Error())
	}

	if item.String() != testItem {
		t.Error("Pip returned incorrect item.")
		t.Errorf("Actual: %v", item.String())
		t.Errorf("Expected: %v", testItem)
	}

	if queue.ApproximateLen() != 0 {
		t.Errorf("Pop failed to delete item.")
	}
}

func TestPopBatch(t *testing.T) {
	// Start with empty queue
	queue.Clear()

	testItems := []string{"test", "test", "test", "test", "test"}
	queue.InsertBatch(testItems)

	result, err := queue.PopBatch()
	if err != nil {
		t.Error(err.Error())
	}

	for i := range testItems {
		if result[i].String() != testItems[i] {
			t.Error("PeekBatch failed.")
		}
	}

	if queue.ApproximateLen() != 0 {
		t.Error("DeleteBatch failed.")
	}
}

func TestQueueCreation(t *testing.T) {
	config := Config{
		VisibilityTimeoutSeconds: 1,
		Region: "us-west-2",
		Name:   "test",
	}

	_, err := NewQueue(&config)
	if err != nil {
		t.Error(err.Error())
	}
}
