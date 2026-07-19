package worker

import "time"

const (
	retryCountLimit = 5
	baseBackoff     = 1 * time.Second

	batchCapacity    = 50
	batchFillTimeout = time.Millisecond * 200
	maxConcurrency   = 8
)
