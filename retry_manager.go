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

// RetryManager centralizes retry/backoff behavior.
type RetryManager struct {
	maxRetries    int
	baseBackoffMs int64
	maxBackoffMs  int64
}

func NewRetryManager(maxRetries int, baseBackoffMs int64, maxBackoffMs int64) RetryManager {
	if maxRetries <= 0 {
		maxRetries = defaultMaxRetries
	}
	if baseBackoffMs <= 0 {
		baseBackoffMs = defaultBaseBackoffMs
	}
	if maxBackoffMs <= 0 {
		maxBackoffMs = defaultMaxBackoffMs
	}
	return RetryManager{
		maxRetries:    maxRetries,
		baseBackoffMs: baseBackoffMs,
		maxBackoffMs:  maxBackoffMs,
	}
}

func (rm RetryManager) MaxRetries() int {
	return rm.maxRetries
}

func (rm RetryManager) IsRetryable(err error) bool {
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

func (rm RetryManager) CalculateBackoff(attempt int) time.Duration {
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
