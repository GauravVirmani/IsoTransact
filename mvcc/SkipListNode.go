package mvcc

import "IsoTransact/mvcc/utils"

type SkipListNode struct {
	key   VersionedKey
	value Value
	tower []*SkipListNode
}

func NewSkipListNode(key VersionedKey, value Value, level uint8) *SkipListNode {
	return &SkipListNode{key: key, value: value, tower: make([]*SkipListNode, level)}
}

func (node *SkipListNode) putOrUpdate(keyToInsert VersionedKey, value Value, levelGenerator utils.LevelGenerator) bool {
	current := node
	precedingNodes := make([]*SkipListNode, len(node.tower))

	for level := len(current.tower) - 1; level >= 0; level-- {
		//move right at the current level
		for current.tower[level] != nil && current.tower[level].key.compare(keyToInsert) < 0 {
			current = current.tower[level]
		}
		//move down in the tower
		precedingNodes[level] = current //store the node just less than the keyToInsert
	}

	//key already exists
	if current.tower[0] != nil && current.tower[0].key.compare(keyToInsert) == 0 {
		return false
	}

	//key does not exist already
	newLevel := levelGenerator.Generate()
	newNode := NewSkipListNode(keyToInsert, value, newLevel)
	for level := uint8(0); level < newLevel; level++ {
		newNode.tower[level] = precedingNodes[level].tower[level]
		precedingNodes[level].tower[level] = newNode
	}

	return true
}

func (node *SkipListNode) get(key VersionedKey) (Value, bool) {
	node, ok := node.matchingNode(key)
	if ok {
		return node.value, true
	}
	return emptyValue(), false
}

func (node *SkipListNode) matchingNode(keyToMatch VersionedKey) (*SkipListNode, bool) {
	current := node
	lastNodeWithTheKey := node

	for level := len(node.tower) - 1; level >= 0; level-- {
		//move right
		for current.tower[level] != nil && current.tower[level].key.compare(keyToMatch) < 0 {
			current = current.tower[level]
			lastNodeWithTheKey = current
		}
		//move down in the tower
	}

	if current != nil && current.key.matchesKeyPrefix(keyToMatch.getKey()) {
		return current, true
	}

	if lastNodeWithTheKey != nil && lastNodeWithTheKey.key.matchesKeyPrefix(keyToMatch.getKey()) {
		return lastNodeWithTheKey, true
	}
	return nil, false
}
