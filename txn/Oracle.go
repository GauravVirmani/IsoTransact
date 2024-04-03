package txn

import (
	errors2 "IsoTransact/txn/errors"
	"context"
	"sync"
)

type CommittedTransaction struct {
	commitTimestamp uint64
	transaction     *ReadWriteTransaction
}

type Oracle struct {
	timeStampGeneratorLock sync.Mutex
	nextTimestamp          uint64
	committedTransactions  []CommittedTransaction

	beginTimestampMark  *TransactionTimestampMark
	commitTimestampMark *TransactionTimestampMark

	transactionExecutor *TransactionExecutor
	executorLock        sync.Mutex
}

func NewOracle(transactionExecutor *TransactionExecutor) *Oracle {
	oracle := &Oracle{
		nextTimestamp:       1,
		transactionExecutor: transactionExecutor,
		beginTimestampMark:  NewTransactionTimestampMark(),
		commitTimestampMark: NewTransactionTimestampMark(),
	}

	oracle.beginTimestampMark.Finish(oracle.nextTimestamp - 1)
	oracle.commitTimestampMark.Finish(oracle.nextTimestamp - 1)
	return oracle
}

func (oracle *Oracle) beginTimestamp() uint64 {
	oracle.timeStampGeneratorLock.Lock()
	beginTimestamp := oracle.nextTimestamp - 1
	oracle.beginTimestampMark.Begin(beginTimestamp)
	oracle.timeStampGeneratorLock.Unlock()

	//Before returning the beginTimestamp, the system waits to
	//ensure that all the commits till beginTimestamp are applied.
	//NOTE: The wait here is on commitTimestampMark not beginTimestampMark
	_ = oracle.commitTimestampMark.WaitForMark(context.Background(), beginTimestamp)
	return beginTimestamp
}

func (oracle *Oracle) maybeCommitTimestampFor(rwTransaction *ReadWriteTransaction) (uint64, error) {
	oracle.timeStampGeneratorLock.Lock()
	defer oracle.timeStampGeneratorLock.Unlock()

	if oracle.hasConflictFor(rwTransaction) {
		return 0, errors2.ConflictErr
	}
	//ending begin phase
	oracle.finishBeginTimestampForReadWriteTransaction(rwTransaction)
	oracle.cleanupCommittedTransactions()

	commitTimestamp := oracle.nextTimestamp
	oracle.nextTimestamp = oracle.nextTimestamp + 1

	//start commit phase
	oracle.commitTimestampMark.Begin(commitTimestamp)
	oracle.trackReadyToCommitTimestamp(rwTransaction, commitTimestamp)
	return commitTimestamp, nil
}

func (oracle *Oracle) hasConflictFor(transaction *ReadWriteTransaction) bool {
	for _, committedTransaction := range oracle.committedTransactions {
		//There is temporal overlap
		if committedTransaction.commitTimestamp > transaction.beginTimestamp {
			//Check for spatial overlap between committed transactions written value and current transactions read value
			for _, key := range transaction.reads {
				if committedTransaction.transaction.batch.Contains(key) {
					return true
				}
			}
		}
	}
	return false
}

// Begin phase of RW transaction ends here
func (oracle *Oracle) finishBeginTimestampForReadWriteTransaction(transaction *ReadWriteTransaction) {
	oracle.beginTimestampMark.Finish(transaction.beginTimestamp)
}

func (oracle *Oracle) cleanupCommittedTransactions() {
	updatedCommittedTransactions := make([]CommittedTransaction, 0)
	maxBeginTransactionTimestamp := oracle.beginTimestampMark.DoneTill()

	for _, transaction := range oracle.committedTransactions {
		if transaction.commitTimestamp > maxBeginTransactionTimestamp {
			updatedCommittedTransactions = append(updatedCommittedTransactions, transaction)
		}
	}
	oracle.committedTransactions = updatedCommittedTransactions
}

func (oracle *Oracle) trackReadyToCommitTimestamp(transaction *ReadWriteTransaction, timestamp uint64) {
	oracle.committedTransactions = append(oracle.committedTransactions, CommittedTransaction{
		transaction:     transaction,
		commitTimestamp: timestamp,
	})
}

func (oracle *Oracle) finishBeginTimestampForReadonlyTransaction(transaction *ReadOnlyTransaction) {
	oracle.beginTimestampMark.Finish(transaction.beginTimestamp)
}

func (oracle *Oracle) Stop() {
	oracle.beginTimestampMark.Stop()
	oracle.commitTimestampMark.Stop()
	oracle.transactionExecutor.Stop()
}

func (oracle *Oracle) CommittedTransactionLength() int {
	return len(oracle.committedTransactions)
}
