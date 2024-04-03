package txn

import (
	"bytes"
	"errors"
	"fmt"
)

type KeyValuePair struct {
	key   []byte
	value []byte
}

func newKeyValuePair(key []byte, value []byte) *KeyValuePair {
	return &KeyValuePair{key: key, value: value}
}

func (pair KeyValuePair) getKey() []byte {
	return pair.key
}

func (pair KeyValuePair) getValue() []byte {
	return pair.value
}

type Batch struct {
	pairs []KeyValuePair
}

func NewBatch() *Batch {
	return &Batch{}
}

func (batch *Batch) Get(key []byte) ([]byte, bool) {
	for _, pair := range batch.pairs {
		if bytes.Compare(pair.key, key) == 0 {
			return pair.value, true
		}
	}
	return nil, false
}

func (batch *Batch) Contains(key []byte) bool {
	_, ok := batch.Get(key)
	return ok
}

func (batch *Batch) Add(key, value []byte) error {
	if batch.Contains(key) {
		return errors.New("batch already contains the key")
	}
	fmt.Println("[Add] Writing: ", string(key), string(value))
	batch.pairs = append(batch.pairs, *newKeyValuePair(key, value))
	return nil
}

func (batch *Batch) IsEmpty() bool {
	return len(batch.pairs) == 0
}

func (timestampedBatch TimestampedBatch) AllPairs() []KeyValuePair {
	return timestampedBatch.batch.pairs
}

func (timestampedBatch TimestampedBatch) getCommitCallback() func() {
	return timestampedBatch.commitCallback
}

func (batch *Batch) ToTimestampedBatch(commitTimestamp uint64, commitCallback func()) TimestampedBatch {
	return TimestampedBatch{
		batch:          batch,
		timestamp:      commitTimestamp,
		doneChannel:    make(chan struct{}),
		commitCallback: commitCallback,
	}
}

type TimestampedBatch struct {
	batch          *Batch
	timestamp      uint64
	doneChannel    chan struct{}
	commitCallback func()
}
