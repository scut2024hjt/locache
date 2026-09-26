package consistenthash

import "hash/crc32"

// Config controls the hash ring. Replicas is fixed for every member so that
// independently running nodes build the same ring from the same membership.
type Config struct {
	Replicas int
	HashFunc func(data []byte) uint32
}

var DefaultConfig = Config{
	Replicas: 100,
	HashFunc: crc32.ChecksumIEEE,
}
