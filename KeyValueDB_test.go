package main

import (
	"IsoTransact/txn"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestGetsTheValueOfANonExistingKey(t *testing.T) {
	db := NewKeyValueDB(10)
	_ = db.Get(func(transaction *txn.ReadOnlyTransaction) {
		_, exists := transaction.Get([]byte("non-existing"))
		assert.Equal(t, false, exists)
	})
}

func TestGetsTheValueOfAnExistingKey(t *testing.T) {
	db := NewKeyValueDB(10)
	waitChannel, err := db.PutOrUpdate(func(transaction *txn.ReadWriteTransaction) {
		_ = transaction.PutOrUpdate([]byte("HDD"), []byte("Hard disk"))
	})
	assert.Nil(t, err)
	<-waitChannel

	waitChannel, err = db.PutOrUpdate(func(transaction *txn.ReadWriteTransaction) {
		_ = transaction.PutOrUpdate([]byte("HDD"), []byte("Hard disk drive"))
	})
	assert.Nil(t, err)
	<-waitChannel

	_ = db.Get(func(transaction *txn.ReadOnlyTransaction) {
		value, exists := transaction.Get([]byte("HDD"))
		assert.Equal(t, true, exists)
		assert.Equal(t, []byte("Hard disk"), value.Slice())
	})
}
