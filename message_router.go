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
