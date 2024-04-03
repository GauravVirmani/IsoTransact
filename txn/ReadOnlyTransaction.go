package txn

import "IsoTransact/mvcc"

type ReadOnlyTransaction struct {
	beginTimestamp uint64
	memTable       *mvcc.MemTable
	oracle         *Oracle
}

func NewReadOnlyTransaction(oracle *Oracle) *ReadOnlyTransaction {
	return &ReadOnlyTransaction{
		beginTimestamp: oracle.beginTimestamp(),
		oracle:         oracle,
		memTable:       oracle.transactionExecutor.memtable,
	}
}

func (transaction *ReadOnlyTransaction) Get(key []byte) (mvcc.Value, bool) {
	versionedKey := mvcc.NewVersionedKey(key, transaction.beginTimestamp)
	return transaction.memTable.Get(*versionedKey)
}

func (transaction *ReadOnlyTransaction) FinishBeginTimestampForReadonlyTransaction() {
	transaction.oracle.finishBeginTimestampForReadonlyTransaction(transaction)
}

func (transaction *ReadOnlyTransaction) PrintTable() {
	transaction.memTable.PrintTable()
}
