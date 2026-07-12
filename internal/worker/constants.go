package worker

import "time"

const (
	retryCountLimit = 5
	baseBackoff     = 1 * time.Second
)
