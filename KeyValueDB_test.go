package main

import (
	"IsoTransact/txn"
	"IsoTransact/txn/errors"
	"github.com/stretchr/testify/assert"
	"strconv"
	"sync"
	"testing"
	"time"
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

func TestPutsMultipleKeyValuesInATransaction(t *testing.T) {
	db := NewKeyValueDB(10)
	waitChannel, err := db.PutOrUpdate(func(transaction *txn.ReadWriteTransaction) {
		for count := 1; count <= 100; count++ {
			_ = transaction.PutOrUpdate([]byte("Key:"+strconv.Itoa(count)), []byte("Value:"+strconv.Itoa(count)))
		}
	})
	assert.Nil(t, err)
	<-waitChannel

	waitChannel, err = db.PutOrUpdate(func(transaction *txn.ReadWriteTransaction) {
		for count := 1; count <= 100; count++ {
			_ = transaction.PutOrUpdate([]byte("Key:"+strconv.Itoa(count)), []byte("Value#"+strconv.Itoa(count)))
		}
	})
	assert.Nil(t, err)
	<-waitChannel

	_ = db.Get(func(transaction *txn.ReadOnlyTransaction) {
		for count := 1; count <= 100; count++ {
			value, exists := transaction.Get([]byte("Key:" + strconv.Itoa(count)))
			assert.Equal(t, true, exists)
			assert.Equal(t, []byte("Value:"+strconv.Itoa(count)), value.Slice())
		}
	})
}

func TestInvolvesConflictingTransactions(t *testing.T) {
	db := NewKeyValueDB(10)

	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		_, err := db.PutOrUpdate(func(transaction *txn.ReadWriteTransaction) {
			delayCommit := func() {
				time.Sleep(25 * time.Millisecond)
			}
			_, _ = transaction.Get([]byte("HDD"))
			_ = transaction.PutOrUpdate([]byte("SSD"), []byte("Solid state drive"))
			delayCommit()
		})
		assert.Error(t, err)
		assert.Equal(t, errors.ConflictErr, err)
	}()

	go func() {
		defer wg.Done()
		waitChannelTwo, err := db.PutOrUpdate(func(transaction *txn.ReadWriteTransaction) {
			delayCommit := func() {
				time.Sleep(10 * time.Millisecond)
			}
			_ = transaction.PutOrUpdate([]byte("HDD"), []byte("Hard disk"))
			delayCommit()
		})
		assert.Nil(t, err)
		<-waitChannelTwo
	}()
	wg.Wait()
}

func TestCommitTransactionAndCheckTheCommittedTransactionsInOracle(t *testing.T) {
	db := NewKeyValueDB(10)
	waitChannel, err := db.PutOrUpdate(func(transaction *txn.ReadWriteTransaction) {
		_ = transaction.PutOrUpdate([]byte("HDD"), []byte("Hard disk"))
	})
	assert.Nil(t, err)
	<-waitChannel

	time.Sleep(10 * time.Millisecond) //allow transactionBeginTimestamp mark to be processed

	_, err = db.PutOrUpdate(func(transaction *txn.ReadWriteTransaction) {})
	assert.Error(t, err)
	assert.Equal(t, errors.EmptyTxnError, err)

	time.Sleep(10 * time.Millisecond) //allow transactionBeginTimestamp mark to be processed

	waitChannel, err = db.PutOrUpdate(func(transaction *txn.ReadWriteTransaction) {
		_ = transaction.PutOrUpdate([]byte("isolation"), []byte("Snapshot"))
	})
	assert.Nil(t, err)
	<-waitChannel

	assert.Equal(t, 1, db.oracle.CommittedTransactionLength())
}

func TestAttemptsToGetFromAStoppedDb(t *testing.T) {
	db := NewKeyValueDB(10)
	db.Stop()

	err := db.Get(func(transaction *txn.ReadOnlyTransaction) {
		_, _ = transaction.Get([]byte("non-existing"))
	})

	assert.Error(t, err)
	assert.Equal(t, DbAlreadyStoppedErr, err)
}

func TestAttemptsToPutInAStoppedDb(t *testing.T) {
	db := NewKeyValueDB(10)
	db.Stop()

	_, err := db.PutOrUpdate(func(transaction *txn.ReadWriteTransaction) {
		_ = transaction.PutOrUpdate([]byte("isolation"), []byte("Snapshot"))
	})

	assert.Error(t, err)
	assert.Equal(t, DbAlreadyStoppedErr, err)
}

func TestStopsTheDbConcurrently(t *testing.T) {
	db := NewKeyValueDB(10)

	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		defer wg.Done()
		db.Stop()
	}()
	go func() {
		defer wg.Done()
		db.Stop()
	}()

	wg.Wait()
	err := db.Get(func(transaction *txn.ReadOnlyTransaction) {
		_, _ = transaction.Get([]byte("HDD"))
	})
	assert.Error(t, err)
	assert.Equal(t, DbAlreadyStoppedErr, err)
}
