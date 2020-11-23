package ppmq

import (
	"bytes"
	"encoding/gob"
)

// ToBytesMeta returns []byte representation of MessageKey for meta
func (mk MessageKey) ToBytesMeta() []byte {
	return []byte("meta:" + mk)
}

// ToBytesId returns []byte representation of MessageKey for message id
func (mk MessageKey) ToBytesId() []byte {
	return []byte("id:" + mk)
}

// ToBytes convert message to []byte representation
func (m Message) ToBytes() ([]byte, error) {
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	err := enc.Encode(m)
	return buf.Bytes(), err
}
