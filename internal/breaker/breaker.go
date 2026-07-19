package breaker

import "time"

type State string

const (
	Closed   State = "closed"
	Open     State = "open"
	HalfOpen State = "half_open"
)

type Breaker struct {
	state            State
	failuresCount    int
	openedAt         time.Time
	now              func() time.Time
	cooldown         time.Duration
	failureThreshold int
}

func NewBreaker(failureThreshold int, cooldown time.Duration) *Breaker {
	return &Breaker{
		state:            Closed,
		failuresCount:    0,
		now:              time.Now,
		cooldown:         cooldown,
		failureThreshold: failureThreshold,
	}
}

func (b *Breaker) Allow() bool {
	switch b.state {
	case Open:
		if b.now().Sub(b.openedAt) >= b.cooldown {
			b.state = HalfOpen
			return true
		} else {
			return false
		}
	case HalfOpen:
		return false
	default:
		return true
	}
}

func (b *Breaker) RecordSuccess() {
	b.failuresCount = 0
	if b.state == HalfOpen {
		b.state = Closed
	}
}

func (b *Breaker) RecordFailure() {
	b.failuresCount++
	if b.state == HalfOpen || (b.state == Closed && b.failuresCount >= b.failureThreshold) {
		b.state = Open
		b.openedAt = b.now()
	}
}
