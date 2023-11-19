package utils

import "hash/fnv"

func GetPartitionId(key string, n int64) int64 {
	if n <= 0 {
		return -1
	}

	h := fnv.New64a()
	_, err := h.Write([]byte(key))
	if err != nil {
		return -1
	}
	hashValue := h.Sum64()

	// Convert the hash to a partition ID in the range [1, n]
	partitionId := int64(hashValue%uint64(n)) + 1
	return partitionId
}
