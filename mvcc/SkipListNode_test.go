package mvcc

import (
	"IsoTransact/mvcc/utils"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestPutsAKeyValueAndGetByKeyInNode(t *testing.T) {
	const maxLevel = 8
	sentinelNode := NewSkipListNode(emptyVersionedKey(), emptyValue(), maxLevel)

	key := NewVersionedKey([]byte("HDD"), 1)
	value := NewValue([]byte("Hard disk"))

	sentinelNode.putOrUpdate(*key, value, *utils.NewLevelGenerator(maxLevel))

	value, ok := sentinelNode.get(*NewVersionedKey([]byte("HDD"), 2))
	assert.Equal(t, true, ok)
	assert.Equal(t, []byte("Hard disk"), value.Slice())
}

func TestUpdatesTheSameKeyWithADifferentVersion(t *testing.T) {
	const maxLevel = 8
	sentinelNode := NewSkipListNode(emptyVersionedKey(), emptyValue(), maxLevel)

	levelGenerator := *utils.NewLevelGenerator(maxLevel)
	sentinelNode.putOrUpdate(*NewVersionedKey([]byte("HDD"), 1), NewValue([]byte("Hard disk")), levelGenerator)
	sentinelNode.putOrUpdate(*NewVersionedKey([]byte("HDD"), 2), NewValue([]byte("Hard disk drive")), levelGenerator)

	value, ok := sentinelNode.get(*NewVersionedKey([]byte("HDD"), 3))
	assert.Equal(t, true, ok)
	assert.Equal(t, []byte("Hard disk drive"), value.Slice())
}

func TestGetsTheValueOfAKeyWithTheNearestVersion(t *testing.T) {
	const maxLevel = 8
	sentinelNode := NewSkipListNode(emptyVersionedKey(), emptyValue(), maxLevel)

	levelGenerator := *utils.NewLevelGenerator(maxLevel)
	sentinelNode.putOrUpdate(*NewVersionedKey([]byte("HDD"), 1), NewValue([]byte("Hard disk")), levelGenerator)
	sentinelNode.putOrUpdate(*NewVersionedKey([]byte("HDD"), 2), NewValue([]byte("Hard disk drive")), levelGenerator)

	value, ok := sentinelNode.get(*NewVersionedKey([]byte("HDD"), 10))
	assert.Equal(t, true, ok)
	assert.Equal(t, []byte("Hard disk drive"), value.Slice())
}

func TestGetsTheValueForNonExistingKey(t *testing.T) {
	const maxLevel = 8
	sentinelNode := NewSkipListNode(emptyVersionedKey(), emptyValue(), maxLevel)

	levelGenerator := *utils.NewLevelGenerator(maxLevel)
	sentinelNode.putOrUpdate(*NewVersionedKey([]byte("HDD"), 1), NewValue([]byte("Hard disk")), levelGenerator)
	sentinelNode.putOrUpdate(*NewVersionedKey([]byte("HDD"), 2), NewValue([]byte("Hard disk drive")), levelGenerator)

	_, ok := sentinelNode.get(*NewVersionedKey([]byte("Storage"), 1))
	assert.Equal(t, false, ok)
}
