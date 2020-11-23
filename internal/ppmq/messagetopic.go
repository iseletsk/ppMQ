package ppmq

import (
	"log"
	"sync"
	"time"
)

// Init MessageTopic initializing underlying storage engine based on StorageFactory passed
func (mq *MessageTopic) Init(factory StorageFactory) error {
	mq.engine = &EngineImpl{}
	mq.lock = &sync.Mutex{}
	return mq.engine.Init(*mq, factory)
}

// SendToSubscriber sends messages if available to subscriber
func (mq *MessageTopic) SendToSubscriber(cqName string) {
	available, err := mq.engine.IsAvailable(cqName)
	if err != nil {
		log.Println(err)
		return
	}
	if !available {
		log.Println("Nope, nothing to push")
		return //nothing in the queue or still waiting for ack
	}
	messages, err := mq.engine.NextBatch(cqName, App.Conf.subscriberMap[cqName].BatchSize)
	if err != nil {
		log.Println("Error getting next batch", err)
		return
	}
	if len(messages) == 0 {
		return // nothing to do
	}
	log.Println("Pushing bunch of them: ", messages)

	Send(cqName, MQWrapper{Topic: mq.Name, Messages: messages})
}

// SendToSubscribers sends messages to all subscribers for give MessageTopic
func (mq *MessageTopic) SendToSubscribers() {
	log.Println("Sending messages to customers")
	for _, cqName := range mq.engine.GetNamesOfSubscribers() {
		mq.SendToSubscriber(cqName)
	}
}

// Publish messages to the MessageTopic. The call will add messages to the queue
// and trigger SendToSubscribers call
func (mq *MessageTopic) Publish(msgs []Message) error {
	mq.lock.Lock()
	defer mq.lock.Unlock()
	if err := mq.engine.Add(msgs); err != nil {
		return err
	}
	mq.SendToSubscribers()
	return nil
}

// Ack messages for subscriber. The call will trigger SendToSubscriber for the
// given subscriber
func (mq *MessageTopic) Ack(cqName string, ids []MessageKey) error {
	mq.lock.Lock()
	defer mq.lock.Unlock()

	if err := mq.engine.Ack(cqName, ids); err != nil {
		return err
	}
	mq.SendToSubscriber(cqName)
	return nil
}

// Reconnect acknowledges MessageTopic that subscriber has reconnected,
// and that whatever messages waiting in Ack should be resent to subscriber
func (mq *MessageTopic) Reconnect(cqName string) error {
	mq.lock.Lock()
	defer mq.lock.Unlock()

	if err := mq.engine.ResetAck(cqName); err != nil {
		return err
	}
	mq.SendToSubscriber(cqName)
	return nil
}

// TimeoutAck checks if any of the Ack in the MessageTopic has expired
// and corresponding messages should be resent to subscribers.
// This method will be called automatically once a minute
func (mq *MessageTopic) TimeoutAck() error {
	mq.lock.Lock()
	defer mq.lock.Unlock()
	if err := mq.engine.TimeoutAck(); err != nil {
		return err
	}
	mq.SendToSubscribers()
	return nil
}

// ackTimeoutCheck holds the interval on how often TimeoutAck() should be called
var ackTimeoutCheck = 1 * time.Minute

// InitTimeoutAck initializes goroutine that checks for Ack timeouts
func InitTimoutAck(done <-chan bool) {
	timer := App.Clock.Timer(ackTimeoutCheck)
	// we are using timer/resetting it so that if it takes more than 1min to reset ack
	// next one would be in 1 min again

	go func() {
		for {
			select {
			case <-done:
				return
			case <-timer.C:
				for _, mq := range App.Conf.topicMap {
					if err := mq.TimeoutAck(); err != nil {
						log.Printf("Error timeout ack %v", err)
					}
				}
				timer.Reset(ackTimeoutCheck)
			}

		}
	}()
}
