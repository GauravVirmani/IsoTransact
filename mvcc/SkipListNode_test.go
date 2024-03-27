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
