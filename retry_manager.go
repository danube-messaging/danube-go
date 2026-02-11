package danube

import (
	"math/rand"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	defaultMaxRetries    = 5
	defaultBaseBackoffMs = 200
	defaultMaxBackoffMs  = 5000
)

type retryManager struct {
	maxRetries    int
	baseBackoffMs int64
	maxBackoffMs  int64
}

func newRetryManager(maxRetries int, baseBackoffMs int64, maxBackoffMs int64) retryManager {
	if maxRetries <= 0 {
		maxRetries = defaultMaxRetries
	}
	if baseBackoffMs <= 0 {
		baseBackoffMs = defaultBaseBackoffMs
	}
	if maxBackoffMs <= 0 {
		maxBackoffMs = defaultMaxBackoffMs
	}
	return retryManager{
		maxRetries:    maxRetries,
		baseBackoffMs: baseBackoffMs,
		maxBackoffMs:  maxBackoffMs,
	}
}

func (rm retryManager) maxRetriesValue() int {
	return rm.maxRetries
}

func (rm retryManager) isRetryable(err error) bool {
	st, ok := status.FromError(err)
	if !ok {
		return false
	}
	switch st.Code() {
	case codes.Unavailable, codes.DeadlineExceeded, codes.ResourceExhausted:
		return true
	default:
		return false
	}
}

func (rm retryManager) calculateBackoff(attempt int) time.Duration {
	linear := rm.baseBackoffMs * int64(attempt+1)
	if linear > rm.maxBackoffMs {
		linear = rm.maxBackoffMs
	}
	min := linear / 2
	if min <= 0 {
		min = 1
	}
	jitter := rand.Int63n(linear-min+1) + min
	return time.Duration(jitter) * time.Millisecond
}
