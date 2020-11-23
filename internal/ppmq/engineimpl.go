package ppmq

import (
	"fmt"
	"github.com/pkg/errors"
	"log"
	"reflect"
	"strconv"
	"time"
)

// SubscriberQueue is storage agnostic subscriber queue used keep track of messages queued and unprocessed Ack
type SubscriberQueue struct {
	Name       string           // Name of the subscriber
	Queue      []PPQueue        // Queue is a slice of queues, one per priority, highest starts with 0
	AckWaiting []AckPair        // AckWaiting list ack for message id
	AckEngine  AckStorageEngine // used to store ack pairs
}

// EngineImpl is storage agnostic, message queue implementation that lacks synchronization
// It uses MessageTopic, PPMessageStore & AckStorageEngine implementations to persist
// message queue.
type EngineImpl struct {
	Topic     MessageTopic                // Topic for which we are listening
	Messages  PPMessageStore              // Messages is a store implementation for messages
	SQ        map[string]*SubscriberQueue // SQ is a map of subscriber name and SubscriberQueue pairs
	idCounter uint64                      // id counter for auto-generated message IDs, used only in Topics where dedupe is disabled
}

// SaveAck saves ack to the persistent storage
func (cq *SubscriberQueue) SaveAck() error {
	return cq.AckEngine.SaveAck(cq.Name, cq.AckWaiting)
}

// Abort operation done on SubscriberQueue (not implemented in underlying engines)
func (cq *SubscriberQueue) Abort() error {
	for _, queue := range cq.Queue {
		if err := queue.Abort(); err != nil {
			return err
		}
	}
	return nil
}

// Sync save changes to disk.
func (cq *SubscriberQueue) Sync() error {
	for _, queue := range cq.Queue {
		if err := queue.Sync(); err != nil {
			return err
		}
	}
	return nil
}

// Init SubscriberQueue including initializing or loading any underlying persistent storage components
func (cq *SubscriberQueue) Init(priorities int, factory StorageFactory, topicName, subscriberName string) error {
	cq.Queue = make([]PPQueue, priorities)
	var err error
	for i := 0; i < priorities; i++ {
		cq.Queue[i], err = factory.MessageQueue(topicName, subscriberName+"-"+strconv.Itoa(i))
		if err != nil {
			return err
		}

	}
	cq.AckEngine, err = factory.AckStore(topicName)
	if err != nil {
		return err
	}
	cq.AckWaiting, err = cq.AckEngine.LoadAck(cq.Name)
	if err != nil {
		log.Printf("Error retreving ACK pairs %v\n", err)
		cq.AckWaiting = make([]AckPair, 0) //Todo: do we want to consider it non-error?
	}
	return nil
}

// Add MessageKey to SubscriberQueue
func (cq *SubscriberQueue) Add(key MessageKey, priority int) error {
	return cq.Queue[priority].Push(key)
}

// Ack MessageKey
func (cq *SubscriberQueue) Ack(key MessageKey) bool {
	for i, e := range cq.AckWaiting {
		if e.Key == key {
			cq.AckWaiting = append(cq.AckWaiting[:i], cq.AckWaiting[i+1:]...)
			return true
		}
	}
	return false
}

// IsAvailable returns true if any messages in this SubscriberQueue are available to be served in the queue
// Message will not be available if there are no messages in the queue, or
// if some messages hasn't been Ack yet.
func (cq *SubscriberQueue) IsAvailable() bool {
	return len(cq.Queue) > 0 && len(cq.AckWaiting) == 0
}

// IsAvailable returns true if any messages are available to be served for sName subscriber to any of the subscribers
func (engine *EngineImpl) IsAvailable(sName string) (bool, error) {
	cq, ok := engine.SQ[sName]
	if !ok {
		return false, fmt.Errorf("unknown queue %s", sName)
	}
	log.Println("SQ Push: ", sName)
	return cq.IsAvailable(), nil
}

// nextId returns new MessageKey for the Message
func (engine *EngineImpl) nextId() MessageKey {
	engine.idCounter++
	return MessageKey(strconv.FormatUint(engine.idCounter-1, 10))
}

// Init initializes Topic including loading or creating any underlying persistent elements
func (engine *EngineImpl) Init(mq MessageTopic, factory StorageFactory) error {
	engine.Topic = mq
	var err error
	engine.Messages, err = factory.MessageStore(mq.Name)
	if err != nil {
		return errors.Wrap(err, "error initializing")
	}
	engine.SQ = make(map[string]*SubscriberQueue)
	for _, name := range engine.Topic.Subscriber {
		cq := SubscriberQueue{Name: name}
		if err := cq.Init(engine.Topic.Priorities, factory, mq.Name, name); err != nil {
			return errors.Wrap(err, "error initializing")
		}
		engine.SQ[name] = &cq
	}
	engine.idCounter = 0
	return nil
}

// Close closes underlying persistent elements. If any errors are thrown, returns last one
func (engine *EngineImpl) Close() error {
	err := engine.Messages.Close()
	for _, sq := range engine.SQ {
		for _, q := range sq.Queue {
			e := q.Close()
			if e != nil {
				err = e
			}
		}
	}
	return err
}

// Add slice of messages to the Topic
func (engine *EngineImpl) Add(msgs []Message) error {
	for _, m := range msgs {
		if err := engine.add(m); err != nil {
			engine.abort()
			return err
		}
	}
	return engine.sync()
}

// add adds Message to the Topic
func (engine *EngineImpl) add(m Message) error {
	if !engine.Topic.Dedupe {
		m.Key = engine.nextId()
	}
	meta := MessageMeta{len(engine.SQ), make(map[string]bool)}
	if engine.Topic.Dedupe {
		oldM, oldMeta, err := engine.Messages.Get(m.Key)
		if err != nil && err != ErrNoSuchMessage {
			return err
		}

		if err == nil { //handle dedupe case only if message is already here, and hasn't been removed from the queue yet
			for _, cq := range engine.SQ {
				//we will use the fact that if message not present we will just skip element in the queue
				cq.Ack(m.Key) // remove key from ack, need to do that, as otherwise we might remove message that just came in, if key is the same
				if oldMeta.Sent[cq.Name] || oldM.Priority > m.Priority {
					// if it was already sent for this queue, or if new message has higher priority...
					if err := cq.Add(m.Key, m.Priority); err != nil {
						return err
					}
				}
			}
			if err := engine.Messages.Put(m, meta); err != nil {
				return err
			}
			return nil
		}
	}
	if err := engine.Messages.Put(m, meta); err != nil {
		return err
	}
	for _, cq := range engine.SQ {
		if err := cq.Add(m.Key, m.Priority); err != nil {
			return err
		}
	}
	return nil
}

// sync makes sures that changes done are persisted
func (engine *EngineImpl) sync() error {
	if err := engine.Messages.Sync(); err != nil {
		engine.abort()
		return err
	}
	for _, cq := range engine.SQ {
		if err := cq.Sync(); err != nil {
			return err
		}
	}
	return nil
}

// abort aborts changes (typically called on error), yet the implementation is deeply flawed
// it tries it best, but it is not transactional.
func (engine *EngineImpl) abort() {
	if err := engine.Messages.Abort(); err != nil {
		log.Printf("Unable to abort %v\n", err) //todo let's print out the error, but not much else we can do.
	}
	for _, cq := range engine.SQ {
		if err := cq.Abort(); err != nil {
			log.Printf("Unable to abort %v\n", err) // nothing to do but printout
		}
	}
}

// Ack processes the fact that list of message keys for subscriber had been acknowledged
func (engine *EngineImpl) Ack(cqName string, keys []MessageKey) error {
	cq, err := engine.GetSubscriber(cqName)
	if err != nil {
		return err
	}
	for _, key := range keys {
		if err := engine.ack(cq, key); err != nil {
			engine.abort()
			return err
		}
	}
	cq.SaveAck()
	return engine.sync()
}

// ack processes individual message key for the subscriber
func (engine *EngineImpl) ack(cq *SubscriberQueue, key MessageKey) error {
	if cq.Ack(key) {
		meta, err := engine.Messages.GetMeta(key)

		if err == nil {
			meta.AckCount--
			if meta.AckCount == 0 {
				if err := engine.Messages.Delete(key); err != nil {
					return err
				}
			} else {
				if err := engine.Messages.UpdateMeta(key, meta); err != nil {
					return err
				}
			}
		} else {
			return fmt.Errorf("the message %v is not in storage (subscriber: %s)", key, cq.Name) // TODO: is it really an error
		}
	} else {
		// ack might have expired, do nothing here
	}

	return nil
}

// Check for old messages
func (engine *EngineImpl) Expire(keys []MessageKey) error {
	for _, key := range keys {
		if err := engine.Messages.Delete(key); err != nil {
			return err
		}
		for _, cq := range engine.SQ {
			cq.Ack(key)
			// no need to remove from the queue, if no such message, we will just skip
		}
	}
	return engine.sync()
}

// GetNamesOfSubscribers returns a slice of subscribers names
func (engine *EngineImpl) GetNamesOfSubscribers() []string {
	r := make([]string, len(engine.SQ))
	for k := range engine.SQ {
		r = append(r, k)
	}
	return r
}

// GetSubscriber returns subscriber queue given subscriber name
func (engine *EngineImpl) GetSubscriber(cqName string) (*SubscriberQueue, error) {
	cq, ok := engine.SQ[cqName]
	if !ok {
		return nil, fmt.Errorf("unknown subscriber %s", cqName)
	}
	return cq, nil
}

// dumpQueue is used for testing and returns a slice of messages keys in the queue for given subscriber
// note that expired messages will not show up. The message is destructive. After its use the queue will be empty
func (engine *EngineImpl) dumpQueue(cqName string) ([]MessageKey, error) {
	cq, err := engine.GetSubscriber(cqName)
	dumped := make(map[MessageKey]bool)
	if err != nil {
		return nil, err
	}
	result := make([]MessageKey, 0)
	now := App.Clock.Now()
	for _, queue := range cq.Queue {
		for {
			key, err := queue.Pop()
			if err != nil {
				if err == ErrQueueEmpty {
					break
				} else {
					return nil, err
				}
			}
			if dumped[key] {
				continue
			}
			m, mm, err := engine.Messages.Get(key)
			if err == ErrNoSuchMessage {
				continue
			}
			if err != nil {
				return nil, err
			}
			if !m.Expires.IsZero() && m.Expires.Before(now) {
				continue
			}
			if mm.Sent[cqName] {
				continue
			}
			dumped[m.Key] = true
			result = append(result, m.Key)
		}
	}
	return result, nil
}

// NextBatch retrieves up to count messages from the Subscriber's queue if available
func (engine *EngineImpl) NextBatch(cqName string, count int) ([]Message, error) {
	result := make([]Message, 0)
	cq, err := engine.GetSubscriber(cqName)
	if err != nil {
		engine.abort()
		return result, err
	}
	now := App.Clock.Now()
	ackExpire := time.Time{}
	if engine.Topic.AckRetry != 0 {
		ackExpire = now.Add(engine.Topic.AckRetry)
	}
	i := 0
	expiredKeys := make([]MessageKey, 0)
Done:
	for _, queue := range cq.Queue {
		for {
			key, err := queue.Pop()
			if err != nil {
				if err == ErrQueueEmpty {
					break
				} else {
					engine.abort()
					return nil, err
				}
			}
			m, meta, err := engine.Messages.Get(key)
			if err == ErrNoSuchMessage {
				continue
				//if message no longer present... remove it
			}
			if err != nil {
				engine.abort()
				return nil, err
			}
			if !m.Expires.IsZero() && m.Expires.Before(now) {
				expiredKeys = append(expiredKeys, m.Key)
				continue
			}

			if meta.Sent[cqName] {
				//already sent that one for this queue, so let's skip it
				continue
			}

			result = append(result, m)
			//mark as sent for the queue, and re-add
			meta.Sent[cqName] = true
			if err := engine.Messages.UpdateMeta(m.Key, meta); err != nil {
				engine.abort()
				return nil, err
			}
			//add ack waiting
			cq.AckWaiting = append(cq.AckWaiting, AckPair{key, ackExpire})
			i++
			if i == count {
				break Done
			}
		}
	}
	if err := engine.Expire(expiredKeys); err != nil {
		engine.abort()
		return nil, err
	}
	if err := cq.SaveAck(); err != nil {
		engine.abort()
		return nil, err
	}
	return result, engine.sync()
}

// TimeoutAck deals with the case when we didn't receive Ack for message in time.
// In ack timed out, we put message in front of the queue for the receiver to redeliver it
func (engine *EngineImpl) TimeoutAck() error {
	for _, cq := range engine.SQ {
		if err := engine.filterAck(cq, ackExpired); err != nil {
			engine.abort()
			return err
		}
		if err := cq.SaveAck(); err != nil {
			engine.abort()
			return err
		}
	}
	return engine.sync()
}

// ResetAck is used on restarts, to reset all Ack so that messages would be re-delivered
func (engine *EngineImpl) ResetAck(cqName string) error {
	cq, err := engine.GetSubscriber(cqName)
	if err != nil {
		engine.abort()
		return err
	}
	err = engine.filterAck(cq, ackAll)
	if err != nil {
		engine.abort()
		return err
	}
	if err := cq.SaveAck(); err != nil {
		engine.abort()
		return err
	}
	return engine.sync()
}

type ackResetFunc func(ack AckPair) bool

// ackExpired used to filter out expired Ack pairs
func ackExpired(ack AckPair) bool {
	return ack.Expires.Before(App.Clock.Now())
}

// ackAll is a convenience method filterAck to include all AckPair
func ackAll(_ AckPair) bool {
	return true
}

// filterAck decides which ack to reset depending on the call (timeout or reset)
func (engine *EngineImpl) filterAck(cq *SubscriberQueue, ackReset ackResetFunc) error {
	tmpQ := make([][]MessageKey, len(cq.Queue))
	for i := range tmpQ {
		tmpQ[i] = make([]MessageKey, 0)
	}
	for _, ack := range cq.AckWaiting {
		if !ackReset(ack) {
			continue
		}
		m, meta, err := engine.Messages.Get(ack.Key)
		if err == ErrNoSuchMessage {
			continue
		}
		if err != nil {
			return err
		}
		meta.AckCount++
		meta.Sent[cq.Name] = false
		if err := engine.Messages.UpdateMeta(ack.Key, meta); err != nil {
			return err
		}
		tmpQ[m.Priority] = append(tmpQ[m.Priority], ack.Key)

	}
	for i := range tmpQ {
		if err := cq.Queue[i].Prepend(tmpQ[i]); err != nil {
			return err
		}
	}
	cq.AckWaiting = make([]AckPair, 0)
	return nil
}

// TestState is used during testing process to test final state of the queue for a given subscriber
// It should be assumed that the method is destructive, and the queue/MessageTopic will be in invalid state
// afterwards. Used specifically for testing purposes.
func (engine *EngineImpl) TestState(cqName string, queue []MessageKey, ack []AckPair) error {
	cq, err := engine.GetSubscriber(cqName)
	if err != nil {
		return err
	}
	cQueue, err := engine.dumpQueue(cqName)
	if err != nil {
		return err
	}

	if !reflect.DeepEqual(cQueue, queue) {
		return fmt.Errorf("queue doesn't match expected: (%v), state: (%v)", queue, cQueue)
	}

	if !reflect.DeepEqual(ack, cq.AckWaiting) {
		return fmt.Errorf("ack doesn't match expected %v state (%v)", ack, cq.AckWaiting)
	}
	for _, k := range ack {
		_, _, err := engine.Messages.Get(k.Key)
		if err == ErrNoSuchMessage {
			return fmt.Errorf("message missing for ack %v", k)
		}
		if err != nil {
			return err
		}
	}
	return nil
}
