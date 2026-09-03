package driver

import "errors"

// Sentinel errors returned by the driver. Match them with errors.Is.
var (
	// ErrClosed is returned by any operation on a closed consumer.
	ErrClosed = errors.New("consumer is closed")

	// ErrTopicNotFound means the broker reported no metadata for the topic,
	// which usually means it does not exist rather than that it is empty. An
	// existing but empty topic still has partitions and assigns normally.
	ErrTopicNotFound = errors.New("topic not found")

	// ErrNotAssigned means an operation that needs the partition set was called
	// before AssignAll succeeded.
	ErrNotAssigned = errors.New("no partitions assigned yet")
)
