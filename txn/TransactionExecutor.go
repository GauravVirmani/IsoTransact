package txn

import (
	"IsoTransact/mvcc"
)

type TransactionExecutor struct {
	batchChannel chan TimestampedBatch
	stopChannel  chan struct{}
	memtable     *mvcc.MemTable
}

func NewTransactionExecutor(memtable *mvcc.MemTable) *TransactionExecutor {
	transactionExecutor := &TransactionExecutor{
		batchChannel: make(chan TimestampedBatch),
		stopChannel:  make(chan struct{}),
		memtable:     memtable,
	}
	go transactionExecutor.spin()
	return transactionExecutor
}

func (executor *TransactionExecutor) spin() {
	for {
		select {
		case timestampedBatch := <-executor.batchChannel:
			executor.applyToStorage(timestampedBatch)
			executor.markApplied(timestampedBatch)
		}
	}
}

func (executor *TransactionExecutor) applyToStorage(timestampedBatch TimestampedBatch) {
	for _, keyValuePair := range timestampedBatch.AllPairs() {
		executor.memtable.PutOrUpdate(
			*mvcc.NewVersionedKey(keyValuePair.getKey(), timestampedBatch.timestamp),
			mvcc.NewValue(keyValuePair.getValue()),
		)
	}
	timestampedBatch.commitCallback()
}

func (executor *TransactionExecutor) markApplied(batch TimestampedBatch) {
	batch.doneChannel <- struct{}{}
	close(batch.doneChannel)
}

func (executor *TransactionExecutor) Submit(batch TimestampedBatch) <-chan struct{} {
	executor.batchChannel <- batch
	return batch.doneChannel
}

func (executor *TransactionExecutor) Stop() {
	executor.stopChannel <- struct{}{}
}
