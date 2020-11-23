package ppmq

import (
	"bytes"
	"encoding/gob"
	"time"
)

// AckPair holds MessageKey & expiration time for Ack
type AckPair struct {
	Key     MessageKey
	Expires time.Time
}

// MessageMeta is stored by StorageEngine for each message to track if message has been sent for particular
// subscriber, and how many acknowledgements were received. This is needed so we can safely remove message once it was
// delivered & acknowledged by each subscriber.
type MessageMeta struct {
	AckCount int
	Sent     map[string]bool
}

// ToBytes encodes MessageMeta to []byte representation
func (m MessageMeta) ToBytes() ([]byte, error) {
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	err := enc.Encode(m)
	return buf.Bytes(), err
}
