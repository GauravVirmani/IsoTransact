package mvcc

import (
	"IsoTransact/mvcc/utils"
	"sync"
)

type MemTable struct {
	lock           sync.RWMutex
	head           *SkipListNode
	levelGenerator utils.LevelGenerator
}

func NewMemTable(maxLevel uint8) *MemTable {
	return &MemTable{
		lock:           *new(sync.RWMutex),
		head:           NewSkipListNode(emptyVersionedKey(), emptyValue(), maxLevel),
		levelGenerator: *utils.NewLevelGenerator(maxLevel),
	}
}

func (memTable *MemTable) PutOrUpdate(key VersionedKey, value Value) {
	memTable.lock.Lock()
	defer memTable.lock.Unlock()

	memTable.head.putOrUpdate(key, value, memTable.levelGenerator)
}

func (memTable *MemTable) Get(key VersionedKey) (Value, bool) {
	memTable.lock.RLock()
	defer memTable.lock.RUnlock()

	return memTable.head.get(key)
}
