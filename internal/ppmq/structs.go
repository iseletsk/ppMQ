package ppmq

import (
	"sync"
	"time"
)

// A MessageTopic keeps the configuration on the message topic as well as storage engine with all the topic information
// We expect that it will be configured and loaded from yaml file.
type MessageTopic struct {
	Name     string        `yaml:"Name"`               // name of the topic
	AckRetry time.Duration `yaml:"AckRetry,omitempty"` //retry resending message if Ack was received in
	// AckRetry interval, 0 if retry disabled
	Dedupe bool `yaml:"Dedupe,omitempty"` // if true, messages will be dedupe based on MessageKey,
	// latest will be always used instead of old message
	Priorities int           `yaml:"Priorities,omitempty"` // 1 or more, if 1 - no priorities...
	Expire     bool          `yaml:"Expire,omitempty"`     // True if message can expire
	Subscriber []string      `yaml:"Subscribers"`          // List of subscribers
	engine     StorageEngine // Message / Topic storage engine
	lock       *sync.Mutex   // We are using lock per topic
}

// A Subscriber keeps the configuration information about subscriber. It is expected that the info will be loaded
// from yaml file
type Subscriber struct {
	Id        int    `yaml:",omitempty"`          // subscriber id (generated on init)
	Name      string `yaml:"Name"`                // subscriber name
	BatchSize int    `yaml:"BatchSize,omitempty"` // max of messages to send to subscriber in one go
	Socket    string `yaml:"Socket"`              // socket to which to send the messages
}

// MessageKey aliases key type for messages
type MessageKey string

// MessagePayload alias for payload sent through the message
type MessagePayload string

// Message struct the basic structure used to send / receive & store messages
type Message struct {
	// MessageKey used for dedupe, ignored if dedupe in MessageTopic  is disabled
	Key MessageKey `json:",omitempty"`

	// MessagePayload the actual payload
	Payload MessagePayload

	// Expires time when message becomes expired, and should be thrown out, can be empty to represent messages
	// that don't expire
	Expires time.Time `json:",omitempty"`

	// Priority defines message priority, starts with 0 (highest priority)
	Priority int `json:",omitempty"`
}

// MQWrapper used to communicate between subscriber & ppMQ
type MQWrapper struct {
	// Topic name of the MessageTopic for which message is sent
	Topic string `json:"topic,omitempty"`
	// Subscriber name of the subscriber sending or receiving
	Subscriber string `json:"subscriber,omitempty"`
	// Messages slice of Message (used to send to Subscriber)
	Messages []Message `json:"messages,omitempty"`
	// Acks slice of MessageKey received from Subscriber to acknowledge processing
	Acks []MessageKey `json:"acks,omitempty"`
	// Command is sent by Subscriber, right now only on reconnect/restart of the Subscriber to reset Acks
	Command string `json:"command,omitempty""`
}
