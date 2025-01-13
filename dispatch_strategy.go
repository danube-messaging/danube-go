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
	RetentionPolicy RetentionPolicy
	RetentionPeriod uint64
}

// NewReliableOptions creates a new ReliableOptions instance
func NewReliableOptions(segmentSize int64, retentionPolicy RetentionPolicy, retentionPeriod uint64) *ReliableOptions {
	return &ReliableOptions{
		SegmentSize:     segmentSize,
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
			RetentionPolicy: retentionPolicy,
			RetentionPeriod: c.ReliableOptions.RetentionPeriod,
		},
	}
}
