package mvcc

import "bytes"

type VersionedKey struct {
	key     []byte
	version uint64
}

func NewVersionedKey(key []byte, version uint64) *VersionedKey {
	return &VersionedKey{key: key, version: version}
}

func emptyVersionedKey() VersionedKey {
	return VersionedKey{}
}

func (versionedKey VersionedKey) getKey() []byte {
	return versionedKey.key
}

func (versionedKey VersionedKey) getVersion() uint64 {
	return versionedKey.version
}

func (versionedKey VersionedKey) compare(other VersionedKey) int {
	comparisonResult := bytes.Compare(versionedKey.getKey(), other.getKey())
	if comparisonResult == 0 {
		thisVersion, otherVersion := versionedKey.getVersion(), other.getVersion()
		if thisVersion == otherVersion {
			return 0
		}
		if thisVersion < otherVersion {
			return -1
		}
		return 1
	}
	return comparisonResult
}

func (versionedKey VersionedKey) matchesKeyPrefix(key []byte) bool {
	return bytes.Compare(versionedKey.getKey(), key) == 0
}

func (versionedKey VersionedKey) asString() string {
	return string(versionedKey.getKey())
}
