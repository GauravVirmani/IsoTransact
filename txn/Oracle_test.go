package txn

import (
	"IsoTransact/mvcc"
	"IsoTransact/txn/errors"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestGetsTheBeginTimestamp(t *testing.T) {
	memTable := mvcc.NewMemTable(10)
	oracle := NewOracle(NewTransactionExecutor(memTable))
	assert.Equal(t, uint64(0), oracle.beginTimestamp())
}

func TestGetsTheBeginTimestampAfterACommit(t *testing.T) {
	memTable := mvcc.NewMemTable(10)
	oracle := NewOracle(NewTransactionExecutor(memTable))

	transaction := NewReadWriteTransaction(oracle)
	transaction.Get([]byte("HDD"))

	commitTimestamp, _ := oracle.maybeCommitTimestampFor(transaction)
	oracle.commitTimestampMark.Finish(commitTimestamp)

	assert.Equal(t, uint64(1), commitTimestamp)
	assert.Equal(t, uint64(1), oracle.beginTimestamp())
	assert.Equal(t, uint64(1), oracle.beginTimestamp())
}

func TestGetsCommitTimestampFor2Transactions(t *testing.T) {
	memTable := mvcc.NewMemTable(10)
	oracle := NewOracle(NewTransactionExecutor(memTable))

	aTransaction := NewReadWriteTransaction(oracle)
	aTransaction.Get([]byte("HDD"))

	commitTimestamp, _ := oracle.maybeCommitTimestampFor(aTransaction)
	oracle.commitTimestampMark.Finish(commitTimestamp)

	assert.Equal(t, uint64(1), commitTimestamp)

	anotherTransaction := NewReadWriteTransaction(oracle)
	anotherTransaction.Get([]byte("SSD"))

	commitTimestamp, _ = oracle.maybeCommitTimestampFor(anotherTransaction)
	oracle.commitTimestampMark.Finish(commitTimestamp)

	assert.Equal(t, uint64(2), commitTimestamp)
}

func TestGetsCommitTimestampFor2TransactionsGivenOneTransactionReadTheKeyThatTheOtherWrites(t *testing.T) {
	memTable := mvcc.NewMemTable(10)
	oracle := NewOracle(NewTransactionExecutor(memTable))

	aTransaction := NewReadWriteTransaction(oracle)
	_ = aTransaction.PutOrUpdate([]byte("HDD"), []byte("Hard disk"))

	commitTimestamp, _ := oracle.maybeCommitTimestampFor(aTransaction)
	oracle.commitTimestampMark.Finish(commitTimestamp)

	assert.Equal(t, uint64(1), commitTimestamp)
	assert.Equal(t, 1, len(oracle.committedTransactions))

	anotherTransaction := NewReadWriteTransaction(oracle)
	anotherTransaction.Get([]byte("HDD"))

	commitTimestamp, _ = oracle.maybeCommitTimestampFor(anotherTransaction)
	oracle.commitTimestampMark.Finish(commitTimestamp)

	assert.Equal(t, uint64(2), commitTimestamp)
}

func TestErrorsForOneTransaction(t *testing.T) {
	memTable := mvcc.NewMemTable(10)
	oracle := NewOracle(NewTransactionExecutor(memTable))

	aTransaction := NewReadWriteTransaction(oracle)
	_ = aTransaction.PutOrUpdate([]byte("HDD"), []byte("Hard disk"))

	commitTimestamp, _ := oracle.maybeCommitTimestampFor(aTransaction)
	oracle.commitTimestampMark.Finish(commitTimestamp)

	assert.Equal(t, uint64(1), commitTimestamp)
	assert.Equal(t, 1, len(oracle.committedTransactions))

	anotherTransaction := NewReadWriteTransaction(oracle)
	_ = anotherTransaction.PutOrUpdate([]byte("HDD"), []byte("Hard disk drive"))
	anotherTransaction.Get([]byte("HDD"))

	thirdTransaction := NewReadWriteTransaction(oracle)
	thirdTransaction.Get([]byte("HDD"))

	commitTimestamp, _ = oracle.maybeCommitTimestampFor(anotherTransaction)
	oracle.commitTimestampMark.Finish(commitTimestamp)

	assert.Equal(t, uint64(2), commitTimestamp)

	_, err := oracle.maybeCommitTimestampFor(thirdTransaction)
	assert.Error(t, err)
	assert.Equal(t, errors.ConflictErr, err)
}
