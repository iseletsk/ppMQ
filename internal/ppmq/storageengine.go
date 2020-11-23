package ppmq

// StorageFactory interface is used to initialize various persistent strategies without re-implementing overall
// storage engine functionality
type StorageFactory interface {
	// MessageStore returns persistence engine for messages
	MessageStore(topicName string) (PPMessageStore, error)
	// MessageQueue returns persistence engine for queue
	MessageQueue(topicName, subscriberName string) (PPQueue, error)
	// AckStore returns persistence engine for Ack
	AckStore(topicName string) (AckStorageEngine, error)
}

// AckStorageEngine interface provides a way to load & store AckPair
type AckStorageEngine interface {
	// LoadAck loads slice of AckPair for given subscriber
	LoadAck(cqName string) ([]AckPair, error)
	// SaveAck saves slice of AckPair for given subscriber
	SaveAck(cqName string, waiting []AckPair) error
}

// StorageEngine interface is needed so that we can have multiple implementations of storage models without
//reimplementing MessageTopic
type StorageEngine interface {
	// Init initializes StorageEngine for given MessageTopic, using particular StorageFactory
	Init(mq MessageTopic, factory StorageFactory) error
	// Closes all underlying persistence engines, making sure that data is saved (if supported)
	Close() error
	// Checks if the are available messages for given subscriber that can be served now
	IsAvailable(cqName string) (bool, error)
	// Retrieves up to count messages for given subscriber
	NextBatch(cqName string, count int) ([]Message, error)
	// Returns slice of subscribers (names) for given StorageEngine
	GetNamesOfSubscribers() []string
	// Add slice of messages to the MessageTopic
	Add(msgs []Message) error
	// Ack slice of messages for given subscriber
	Ack(cqName string, keys []MessageKey) error
	// ResetAck resets unacknowledged messages for given subscriber
	ResetAck(cqName string) error
	// TimeoutAck resets expired ack for all subscribers for MessageTopic
	TimeoutAck() error
	// TestState is used during testing process to test final state of the queue for a given subscriber
	// It should be assumed that the method is destructive, and the queue/MessageTopic will be in invalid state
	// afterwards. Used specifically for testing purposes.
	TestState(cqName string, queue []MessageKey, ack []AckPair) error
}

// PPMessageStore is used to persist messages
type PPMessageStore interface {
	// Init message store engine
	Init() error
	// Close message store engine, safely persisting all messages if needed
	Close() error
	// Get Message and MessageMeta for given MessageKey. Returns ErrNoSuchMessage if doesn't exist
	Get(key MessageKey) (Message, MessageMeta, error)
	// GetMeta returns MessageMeta for given MessageKey, Returns ErrNoSuchMessage if doesn't exist
	GetMeta(key MessageKey) (MessageMeta, error)
	// Put persists Message & MessageMeta
	Put(m Message, meta MessageMeta) error
	// UpdateMeta updates MessageMeta
	UpdateMeta(key MessageKey, meta MessageMeta) error
	// Delete removes Message & MessageMeta for given key from the storage
	Delete(key MessageKey) error
	// Exists checks if Message exists
	Exists(key MessageKey) (bool, error)
	// Sync saves all changes to disk
	Sync() error
	// Abort tries to safely abort the operation. Not implemented at this moment.
	Abort() error
}

// PPQueue interface for persisting queue for subscriber
type PPQueue interface {
	// Init underlying persistence layer
	Init() error
	// Push message key into the queue
	Push(key MessageKey) error
	// Prepend slice of message keys into the queue. Used on ack reset & timeout, when we want to resend some messages
	Prepend(keys []MessageKey) error
	// Pop message from the queue
	Pop() (MessageKey, error)
	// Peek check first available message on the queue
	Peek() (MessageKey, error)
	// IsEmpty returns true if queue is empty
	IsEmpty() (bool, error)
	// Sync saves queue state
	Sync() error
	// Abort attempts to abort the changes to the queue before saving (might not be implemented)
	Abort() error
	// Closes the queue, saving all unsaved data.
	Close() error
}
