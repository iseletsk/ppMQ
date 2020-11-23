package ppmq

// MemoryQueue is non-persistent, memory only implementation of StorageEngine for the queue
type MemoryQueue struct {
	Queue []MessageKey
}

func (mq *MemoryQueue) Init() error {
	mq.Queue = make([]MessageKey, 0)
	return nil
}
func (mq *MemoryQueue) Push(key MessageKey) error {
	mq.Queue = append(mq.Queue, key)
	return nil
}

func (mq *MemoryQueue) Prepend(keys []MessageKey) error {
	mq.Queue = append(keys, mq.Queue...)
	return nil
}

func (mq *MemoryQueue) Pop() (MessageKey, error) {
	if len(mq.Queue) > 0 {
		key := mq.Queue[0]
		mq.Queue = mq.Queue[1:]
		return key, nil
	}
	return "", ErrQueueEmpty
}

func (mq *MemoryQueue) Peek() (MessageKey, error) {
	if len(mq.Queue) > 0 {
		key := mq.Queue[0]
		return key, nil
	}
	return "", ErrQueueEmpty
}

func (mq *MemoryQueue) IsEmpty() (bool, error) {
	return len(mq.Queue) == 0, nil
}

func (mq *MemoryQueue) Sync() error {
	return nil
}

func (mq *MemoryQueue) Abort() error {
	return nil
}

func (mq *MemoryQueue) Close() error {
	return nil
}
