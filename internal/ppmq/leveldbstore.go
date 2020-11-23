package ppmq

import (
	"bytes"
	"encoding/gob"
	"github.com/jmhodges/levigo"
	"os"
)

// LdbMessageStore uses LevelDB (levigo implementation) to store messages. One instance of LevelDB per Topic
type LdbMessageStore struct {
	path      string
	db        *levigo.DB
	batch     *levigo.WriteBatch
	writeOpts *levigo.WriteOptions
	readOpts  *levigo.ReadOptions
}

// closeCurrentBatch persists current changes. We don't save single message
// but typically batch it up as they come in packs
func (lms *LdbMessageStore) closeCurrentBatch() error {
	if lms.batch != nil {
		defer lms.batch.Close() // free up mem even if we cannot write.
		if err := lms.db.Write(lms.writeOpts, lms.batch); err != nil {
			return err
		}
	}
	return nil
}

// Begin starts a new batch, closes previous batch if it is still open
func (lms *LdbMessageStore) Begin() error {
	_ = lms.closeCurrentBatch() // we can ignore that we cannot write old batch?
	lms.batch = levigo.NewWriteBatch()
	return nil
}

func (lms *LdbMessageStore) Abort() error {
	if lms.batch != nil {
		lms.batch.Clear()
		lms.batch.Close()
		lms.batch = nil
	}
	return nil
}

func (lms *LdbMessageStore) Get(key MessageKey) (Message, MessageMeta, error) {
	m := Message{}
	mm, err := lms.GetMeta(key)
	if err == nil {
		var data []byte
		data, err = lms.db.Get(lms.readOpts, key.ToBytesId())
		if err == nil {
			if data == nil {
				return m, mm, ErrNoSuchMessage // todo: what to do about the fact that meta is there?
			}
			dec := gob.NewDecoder(bytes.NewBuffer(data))
			err = dec.Decode(&m)
		}
	}
	return m, mm, err
}

func (lms *LdbMessageStore) GetMeta(key MessageKey) (MessageMeta, error) {
	mm := MessageMeta{}
	data, err := lms.db.Get(lms.readOpts, key.ToBytesMeta())
	if err == nil {
		if data == nil {
			return mm, ErrNoSuchMessage
		}
		dec := gob.NewDecoder(bytes.NewBuffer(data))
		err = dec.Decode(&mm) //read message
	}
	return mm, err
}

func (lms *LdbMessageStore) Put(m Message, meta MessageMeta) error {
	if lms.batch == nil {
		if err := lms.Begin(); err != nil {
			return err
		}
	}
	data, err := m.ToBytes()
	if err != nil {
		return err
	}
	lms.batch.Put(m.Key.ToBytesId(), data)
	return lms.UpdateMeta(m.Key, meta)

}

func (lms *LdbMessageStore) UpdateMeta(key MessageKey, meta MessageMeta) error {
	if lms.batch == nil {
		if err := lms.Begin(); err != nil {
			return err
		}
	}
	data, err := meta.ToBytes()
	if err != nil {
		return err
	}
	lms.batch.Put(key.ToBytesMeta(), data)
	return nil
}

func (lms *LdbMessageStore) Delete(key MessageKey) error {
	if lms.batch == nil {
		if err := lms.Begin(); err != nil {
			return err
		}
	}
	lms.batch.Delete([]byte(key))
	return nil
}

func (lms *LdbMessageStore) Exists(key MessageKey) (bool, error) {
	_, err := lms.GetMeta(key)

	if err == ErrNoSuchMessage {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return true, nil
}

func (lms *LdbMessageStore) Sync() error {
	err := lms.closeCurrentBatch()
	lms.batch = nil // remove old batch no matter what
	return err
}

func (lms *LdbMessageStore) Close() error {
	lms.db.Close()
	lms.db = nil
	return nil
}

func (lms *LdbMessageStore) Init() error {
	if _, err := os.Stat(lms.path); os.IsNotExist(err) {
		if err := os.MkdirAll(lms.path, 0700); err != nil {
			return err
		}
	}
	opts := levigo.NewOptions()
	opts.SetCreateIfMissing(true)

	db, err := levigo.Open(lms.path, opts)
	if err != nil {
		return err
		// todo: add repair db
	}
	lms.db = db
	lms.writeOpts = levigo.NewWriteOptions()
	lms.writeOpts.SetSync(false)
	lms.readOpts = levigo.NewReadOptions()
	return nil
}

// AckWrapper is used so we can use Gob encoder to store slice of AckPairs
type AckWrapper struct {
	Ack []AckPair
}

// ackKey returns the key under which slice of AckPair will be stored in leveldb
func ackKey(cqName string) []byte {
	return []byte("ack:" + cqName)
}

func (lms *LdbMessageStore) LoadAck(cqName string) ([]AckPair, error) {
	w := AckWrapper{}
	data, err := lms.db.Get(lms.readOpts, ackKey(cqName))
	if err == nil {
		if data == nil { // If pairs weren't initialized yet, initialize them
			return make([]AckPair, 0), nil
		}
		dec := gob.NewDecoder(bytes.NewBuffer(data))
		err = dec.Decode(&w) //read AckWrapper
	}
	return w.Ack, err
}

func (lms *LdbMessageStore) SaveAck(cqName string, ack []AckPair) error {
	if lms.batch == nil {
		if err := lms.Begin(); err != nil {
			return err
		}
	}
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	err := enc.Encode(AckWrapper{ack})
	if err != nil {
		return err
	}
	lms.batch.Put(ackKey(cqName), buf.Bytes())
	return nil
}
