package danube

import (
	"github.com/danube-messaging/danube-go/proto"
)

// ConfigDispatchStrategy represents the dispatch strategy for a topic.
// It now directly maps to the proto.DispatchStrategy enum.
type ConfigDispatchStrategy struct {
	Strategy proto.DispatchStrategy
}

// NewConfigDispatchStrategy creates a new ConfigDispatchStrategy instance
// with NonReliable as default.
func NewConfigDispatchStrategy() *ConfigDispatchStrategy {
	return &ConfigDispatchStrategy{
		Strategy: proto.DispatchStrategy_NonReliable,
	}
}

// NewReliableDispatchStrategy creates a new reliable ConfigDispatchStrategy instance.
func NewReliableDispatchStrategy() *ConfigDispatchStrategy {
	return &ConfigDispatchStrategy{
		Strategy: proto.DispatchStrategy_Reliable,
	}
}

// ToProtoDispatchStrategy converts ConfigDispatchStrategy to proto.DispatchStrategy enum value
func (c *ConfigDispatchStrategy) ToProtoDispatchStrategy() proto.DispatchStrategy {
	if c == nil {
		return proto.DispatchStrategy_NonReliable
	}
	return c.Strategy
}
