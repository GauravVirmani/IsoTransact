package main

import (
	"IsoTransact/mvcc"
	"IsoTransact/txn"
	"errors"
	"sync/atomic"
)

var DbAlreadyStoppedErr = errors.New("Db is stopped, can not perform the operation")

type KeyValueDB struct {
	stopped atomic.Bool
	oracle  *txn.Oracle
}

func NewKeyValueDB(skipListMaxLevel uint8) *KeyValueDB {
	return &KeyValueDB{
		oracle: txn.NewOracle(txn.NewTransactionExecutor(mvcc.NewMemTable(skipListMaxLevel))),
	}
}

func (db *KeyValueDB) Get(callback func(transaction *txn.ReadOnlyTransaction)) error {
	if db.stopped.Load() {
		return DbAlreadyStoppedErr
	}
	transaction := txn.NewReadOnlyTransaction(db.oracle)
	defer transaction.FinishBeginTimestampForReadonlyTransaction()

	callback(transaction)
	return nil
}

func (db *KeyValueDB) PutOrUpdate(callback func(transaction *txn.ReadWriteTransaction)) (<-chan struct{}, error) {
	if db.stopped.Load() {
		return nil, DbAlreadyStoppedErr
	}
	transaction := txn.NewReadWriteTransaction(db.oracle)
	defer transaction.FinishBeginTimestampForReadWriteTransaction()

	callback(transaction)
	return transaction.Commit()
}

func (db *KeyValueDB) Stop() {
	if db.stopped.CompareAndSwap(false, true) {
		db.oracle.Stop()
	}
}
