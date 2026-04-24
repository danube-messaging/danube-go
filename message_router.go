package danube

import (
	"sync/atomic"
)

type messageRouter struct {
	partitions    int32
	lastPartition int32
}

func newMessageRouter(partitions int32) *messageRouter {
	return &messageRouter{
		partitions:    partitions,
		lastPartition: partitions - 1,
	}
}

func (router *messageRouter) roundRobin() int32 {
	// Atomically get the current value of lastPartition
	last := atomic.LoadInt32(&router.lastPartition)

	// Calculate the next partition and update atomically
	next := (last + 1) % router.partitions
	atomic.StoreInt32(&router.lastPartition, next)

	return next
}

// keyRoute routes by hashing the routing key to a deterministic partition.
// Ensures all messages with the same key always go to the same partition,
// which is required for per-key ordering on partitioned Key-Shared topics.
func (router *messageRouter) keyRoute(routingKey string) int32 {
	hash := fnv1aHash(routingKey)
	return int32(hash % uint64(router.partitions))
}

func fnv1aHash(key string) uint64 {
	var hash uint64 = 0xcbf29ce484222325
	for i := 0; i < len(key); i++ {
		hash ^= uint64(key[i])
		hash *= 0x100000001b3
	}
	return hash
}
