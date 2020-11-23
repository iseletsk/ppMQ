package ppmq

// MemoryMessageStore is non-persistent implementation of Message & Ack store used for debugging & dev purposes.
type MemoryMessageStore struct {
	Messages map[MessageKey]Message
	Meta     map[MessageKey]MessageMeta
}

func (mms *MemoryMessageStore) Init() error {
	mms.Messages = make(map[MessageKey]Message)
	mms.Meta = make(map[MessageKey]MessageMeta)
	return nil
}

func (mms *MemoryMessageStore) Close() error {
	return nil
}

func (mms *MemoryMessageStore) Get(key MessageKey) (Message, MessageMeta, error) {
	m, ok := mms.Messages[key]
	if !ok {
		return Message{}, MessageMeta{}, ErrNoSuchMessage
	}
	mm, ok := mms.Meta[key]
	if !ok {
		return Message{}, MessageMeta{}, ErrNoSuchMessage
	}
	return m, mm, nil
}

func (mms *MemoryMessageStore) GetMeta(key MessageKey) (MessageMeta, error) {
	mm, ok := mms.Meta[key]
	if !ok {
		return MessageMeta{}, ErrNoSuchMessage
	}
	return mm, nil
}

func (mms *MemoryMessageStore) Put(m Message, meta MessageMeta) error {
	mms.Messages[m.Key] = m
	mms.Meta[m.Key] = meta
	return nil
}

func (mms *MemoryMessageStore) UpdateMeta(key MessageKey, meta MessageMeta) error {
	mms.Meta[key] = meta
	return nil
}

func (mms *MemoryMessageStore) Exists(key MessageKey) (bool, error) {
	_, ok := mms.Messages[key]
	return ok, nil
}

func (mms *MemoryMessageStore) Delete(key MessageKey) error {
	delete(mms.Messages, key)
	delete(mms.Meta, key)
	return nil
}
func (mms *MemoryMessageStore) Sync() error {
	return nil
}

func (mms *MemoryMessageStore) Abort() error {
	return nil
}

func (mms *MemoryMessageStore) LoadAck(cqName string) ([]AckPair, error) {
	return make([]AckPair, 0), nil
}

func (mms *MemoryMessageStore) SaveAck(cqName string, _ []AckPair) error {
	return nil
}
