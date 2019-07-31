package mhdynamo

import (
	"math"
	"math/rand"
	"time"
)

// These set the lower and upper bounds for the delay between attempts to retry
// an API operation.
const (
	minBackoffInterval = 10 * time.Millisecond
	maxBackoffInterval = 20 * time.Second
)

// Seed the random number generator with the current time. This is good enough.
func init() {
	rand.Seed(time.Now().UnixNano())
}

// retryInterval gives an exponentially increasing interval between attempts.
// It includes some random variance and is bounded to only return values
// between minBackoffInterval and maxBackoffInterval.
func retryInterval(attempt int) time.Duration {
	// 2^n results in 2, 4, 8, 16, 32 ms etc.
	n := int(math.Pow(2, float64(attempt)))

	// Add 1 to 20 ms for variance. Reduces chance of multiple instances calling
	// in synchronized waves.
	n += rand.Intn(20) + 1

	// Convert to milliseconds.
	delay := time.Duration(n) * time.Millisecond

	// Enforce lower and upper bounds.
	if delay < minBackoffInterval {
		delay = minBackoffInterval
	} else if delay > maxBackoffInterval {
		delay = maxBackoffInterval
	}
	return delay
}
