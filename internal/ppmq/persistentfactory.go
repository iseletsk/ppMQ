package ppmq

import (
	"path"
)

// PersistentFactory is a storage factory for MessageTopic based on LevelDB & dque implementation
type PersistentFactory struct {
	Path string // location where data should be stored for given MessageTopic
	ms   *LdbMessageStore
}

// init initializes LdbMessageStore if it wasn't initialized yet
func (pf *PersistentFactory) init(topicName string) error {
	if pf.ms == nil || pf.ms.db == nil {
		pf.ms = &LdbMessageStore{path: path.Join(pf.Path, "db", topicName)}
		return pf.ms.Init()
	}
	return nil
}

func (pf *PersistentFactory) MessageStore(topicName string) (PPMessageStore, error) {
	err := pf.init(topicName)
	return pf.ms, err
}

func (pf *PersistentFactory) MessageQueue(topicName, subscriberName string) (PPQueue, error) {
	q := DQueue{path.Join(pf.Path, "queue", topicName), subscriberName, nil}
	err := q.Init()
	return &q, err
}

func (pf *PersistentFactory) AckStore(topicName string) (AckStorageEngine, error) {
	err := pf.init(topicName)
	return pf.ms, err
}
