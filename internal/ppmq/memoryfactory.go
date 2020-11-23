package ppmq

// MemoryFactory is a storage factory that returns non-persistent Interfaces for handing Message, Ack & Topic storage.
// It was used for initial development and debugging.
type MemoryFactory struct {
	ms *MemoryMessageStore
}

// init memory factory
func (pf *MemoryFactory) init() error {
	if pf.ms == nil {
		pf.ms = &MemoryMessageStore{}
		return pf.ms.Init()
	}
	return nil
}

func (pf *MemoryFactory) MessageStore(_ string) (PPMessageStore, error) {
	err := pf.init()
	return pf.ms, err
}

func (pf *MemoryFactory) MessageQueue(_, _ string) (PPQueue, error) {
	q := MemoryQueue{}
	err := q.Init()
	return &q, err
}

func (pf *MemoryFactory) AckStore(_ string) (AckStorageEngine, error) {
	err := pf.init()
	return pf.ms, err
}
