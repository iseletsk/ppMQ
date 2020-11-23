package ppmq

import (
	"fmt"
	"github.com/iseletsk/dque"
	"github.com/pkg/errors"
	"os"
)

// Dqueue implements persistent queue for storing message keys that needs to be served to subscribers
// it is based on a fork of https://github.com/joncrlsn/dque
type DQueue struct {
	path string
	name string
	q    *dque.DQue
}

// dqueKey struct is needed to wrap MessageKey in struct so we can use encoding/gob
type dqueKey struct {
	Id MessageKey
}

// keyBuilder is by DQue to know types of objects to instantiate before retrieving them from storage
func keyBuilder() interface{} {
	return &dqueKey{}
}

// Init initialized DQue. This includes creating necessary directories if they don't exist
// It will open existing DQue if it is already there, or create new one if not.
func (D *DQueue) Init() error {
	var err error
	if _, err = os.Stat(D.path); os.IsNotExist(err) {
		if err := os.MkdirAll(D.path, 0700); err != nil {
			return err
		}
	}
	D.q, err = dque.NewOrOpen(D.name, D.path, 50, keyBuilder)
	if err != nil {
		return err
	}
	return D.q.TurboOn()
}

func (D *DQueue) Push(key MessageKey) error {
	return D.q.Enqueue(&dqueKey{key})
}

func (D *DQueue) Prepend(keys []MessageKey) error {
	objs := make([]interface{}, len(keys))
	for i, k := range keys {
		objs[i] = &dqueKey{k}
	}
	return D.q.Prepend(objs)
}

// unmaskDqueKey is a convenience method to type cast o interface{} to MessageKey nad deal with errors if necessary
func unmaskDqueKey(o interface{}, err error, prefix string) (MessageKey, error) {
	wrapMessage := "unable to " + prefix + " message"
	if err != nil {
		if err == dque.ErrEmpty {
			return "", ErrQueueEmpty
		}
		return "", errors.Wrap(err, wrapMessage)
	}
	k, ok := o.(*dqueKey)
	if !ok {
		return "", errors.Wrap(fmt.Errorf("wrong object type %T", o), wrapMessage)
	}
	return k.Id, nil
}

func (D *DQueue) Pop() (MessageKey, error) {
	o, err := D.q.Dequeue()
	return unmaskDqueKey(o, err, "pop")
}

func (D *DQueue) Peek() (MessageKey, error) {
	o, err := D.q.Peek()
	return unmaskDqueKey(o, err, "peek")
}

func (D *DQueue) IsEmpty() (bool, error) {
	return D.q.Size() == 0, nil
}

func (D *DQueue) Sync() error {
	return D.q.TurboSync()
}

// Abort does nothing in this implementation, reserved for the future
func (D *DQueue) Abort() error {
	return nil
}

func (D *DQueue) Close() error {
	return D.q.Close()
}
