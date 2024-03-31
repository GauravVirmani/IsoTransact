package txn

import (
	"IsoTransact/mvcc"
	"errors"
)

type ReadWriteTransaction struct {
	beginTimestamp uint64
	memTable       *mvcc.MemTable
	batch          *Batch
	reads          [][]byte
	oracle         *Oracle
}

func NewReadWriteTransaction(oracle *Oracle) *ReadWriteTransaction {
	return &ReadWriteTransaction{
		beginTimestamp: oracle.beginTimestamp(),
		batch:          NewBatch(),
		oracle:         oracle,
		memTable:       oracle.transactionExecutor.memtable,
	}
}

func (transaction *ReadWriteTransaction) Get(key []byte) (mvcc.Value, bool) {
	if value, ok := transaction.batch.Get(key); ok {
		return mvcc.NewValue(value), true
	}
	transaction.reads = append(transaction.reads, key)

	versionedKey := mvcc.NewVersionedKey(key, transaction.beginTimestamp)
	return transaction.memTable.Get(*versionedKey)
}

func (transaction *ReadWriteTransaction) PutOrUpdate(key []byte, value []byte) error {
	err := transaction.batch.Add(key, value)
	if err != nil {
		return err
	}
	return nil
}

func (transaction *ReadWriteTransaction) Commit() (<-chan struct{}, error) {
	if transaction.batch.IsEmpty() {
		return nil, errors.New("empty write batch, nothing to commit")
	}

	// Send the transaction to the executor in the increasing order of the commitTimestamp.
	// If a commit with the commitTimestamp 102 is applied, it is assumed that the commit with commitTimestamp 101 is already available.
	transaction.oracle.executorLock.Lock()
	defer transaction.oracle.executorLock.Unlock()

	commitTimestamp, err := transaction.oracle.maybeCommitTimestampFor(transaction)
	if err != nil {
		return nil, err
	}
	commitCallback := func() {
		transaction.oracle.commitTimestampMark.Finish(commitTimestamp)
	}
	return transaction.oracle.transactionExecutor.Submit(transaction.batch.ToTimestampedBatch(commitTimestamp, commitCallback)), nil
}

func (transaction *ReadWriteTransaction) FinishBeginTimestampForReadWriteTransaction() {
	transaction.oracle.finishBeginTimestampForReadWriteTransaction(transaction)
}
