package txn

import "IsoTransact/mvcc"

type ReadOnlyTransaction struct {
	beginTimestamp uint64
	memTable       *mvcc.MemTable
	oracle         *Oracle
}

func (transaction *ReadOnlyTransaction) Get(key []byte) (mvcc.Value, bool) {
	versionedKey := mvcc.NewVersionedKey(key, transaction.beginTimestamp)
	return transaction.memTable.Get(*versionedKey)
}

func (transaction *ReadOnlyTransaction) FinishBeginTimestampForReadonlyTransaction() {
	transaction.oracle.finishBeginTimestampForReadonlyTransaction(transaction)
}
