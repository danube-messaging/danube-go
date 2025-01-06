package danube

import (
	"github.com/danube-messaging/danube-go/proto"
)

// ConfigDispatchStrategy represents the dispatch strategy for a topic
type ConfigDispatchStrategy struct {
	// Using embedded struct to simulate enum-like behavior
	NonReliable     bool
	ReliableOptions *ReliableOptions
}

// ReliableOptions represents configuration options for reliable dispatch strategy
type ReliableOptions struct {
	SegmentSize     int64
	StorageType     StorageType
	RetentionPolicy RetentionPolicy
	RetentionPeriod uint64
}

// NewReliableOptions creates a new ReliableOptions instance
func NewReliableOptions(segmentSize int64, storageType StorageType, retentionPolicy RetentionPolicy, retentionPeriod uint64) *ReliableOptions {
	return &ReliableOptions{
		SegmentSize:     segmentSize,
		StorageType:     storageType,
		RetentionPolicy: retentionPolicy,
		RetentionPeriod: retentionPeriod,
	}
}

// RetentionPolicy represents the retention policy for messages in the topic
type RetentionPolicy int

const (
	RetainUntilAck RetentionPolicy = iota
	RetainUntilExpire
)

// StorageType represents the storage type for messages in the topic
type StorageType struct {
	Type     StorageTypeEnum
	Location string // Used for Disk path or S3 bucket name
}

// StorageTypeEnum represents the type of storage
type StorageTypeEnum int

const (
	InMemory StorageTypeEnum = iota
	Disk
	S3
)

// NewConfigDispatchStrategy creates a new ConfigDispatchStrategy instance
func NewConfigDispatchStrategy() *ConfigDispatchStrategy {
	return &ConfigDispatchStrategy{
		NonReliable: true,
	}
}

// NewReliableDispatchStrategy creates a new reliable ConfigDispatchStrategy instance
func NewReliableDispatchStrategy(options *ReliableOptions) *ConfigDispatchStrategy {
	return &ConfigDispatchStrategy{
		NonReliable:     false,
		ReliableOptions: options,
	}
}

// ToProtoDispatchStrategy converts ConfigDispatchStrategy to proto.TopicDispatchStrategy
func (c *ConfigDispatchStrategy) ToProtoDispatchStrategy() *proto.TopicDispatchStrategy {
	if c.NonReliable {
		return &proto.TopicDispatchStrategy{
			Strategy:        0,
			ReliableOptions: nil,
		}
	}

	// Handle reliable strategy
	var storageBackend proto.StorageBackend
	var storagePath string

	switch c.ReliableOptions.StorageType.Type {
	case InMemory:
		storageBackend = proto.StorageBackend_IN_MEMORY
		storagePath = ""
	case Disk:
		storageBackend = proto.StorageBackend_DISK
		storagePath = c.ReliableOptions.StorageType.Location
	case S3:
		storageBackend = proto.StorageBackend_S3
		storagePath = c.ReliableOptions.StorageType.Location
	}

	var retentionPolicy proto.RetentionPolicy
	switch c.ReliableOptions.RetentionPolicy {
	case RetainUntilAck:
		retentionPolicy = proto.RetentionPolicy_RetainUntilAck
	case RetainUntilExpire:
		retentionPolicy = proto.RetentionPolicy_RetainUntilExpire
	}

	return &proto.TopicDispatchStrategy{
		Strategy: 1,
		ReliableOptions: &proto.ReliableOptions{
			SegmentSize:     uint64(c.ReliableOptions.SegmentSize),
			StorageBackend:  storageBackend,
			StoragePath:     storagePath,
			RetentionPolicy: retentionPolicy,
			RetentionPeriod: c.ReliableOptions.RetentionPeriod,
		},
	}
}
