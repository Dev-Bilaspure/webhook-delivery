package breaker

import (
	"testing"
	"time"
)

func TestNewBreaker(t *testing.T) {
	b := NewBreaker(3, time.Minute)

	if b.state != Closed {
		t.Fatalf("state = %v, want %v", b.state, Closed)
	}

	if b.failuresCount != 0 {
		t.Fatalf("failures = %d, want 0", b.failuresCount)
	}

	if b.failureThreshold != 3 {
		t.Fatalf("threshold = %d, want 3", b.failureThreshold)
	}
}

func TestAllow(t *testing.T) {
	now := time.Date(2026, 7, 20, 12, 0, 0, 0, time.UTC)

	tests := []struct {
		name      string
		breaker   Breaker
		allow     bool
		wantState State
	}{
		{
			name: "closed allows",
			breaker: Breaker{
				state: Closed,
				now:   func() time.Time { return now },
			},
			allow:     true,
			wantState: Closed,
		},
		{
			name: "half open blocks",
			breaker: Breaker{
				state: HalfOpen,
				now:   func() time.Time { return now },
			},
			allow:     false,
			wantState: HalfOpen,
		},
		{
			name: "open before cooldown",
			breaker: Breaker{
				state:    Open,
				openedAt: now.Add(-5 * time.Second),
				cooldown: 10 * time.Second,
				now:      func() time.Time { return now },
			},
			allow:     false,
			wantState: Open,
		},
		{
			name: "open after cooldown",
			breaker: Breaker{
				state:    Open,
				openedAt: now.Add(-15 * time.Second),
				cooldown: 10 * time.Second,
				now:      func() time.Time { return now },
			},
			allow:     true,
			wantState: HalfOpen,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.breaker.Allow()

			if got != tt.allow {
				t.Fatalf("Allow() = %v, want %v", got, tt.allow)
			}

			if tt.breaker.state != tt.wantState {
				t.Fatalf("state = %v, want %v", tt.breaker.state, tt.wantState)
			}
		})
	}
}

func TestRecordSuccess(t *testing.T) {
	tests := []struct {
		name      string
		state     State
		wantState State
	}{
		{"closed", Closed, Closed},
		{"half open", HalfOpen, Closed},
		{"open", Open, Open},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b := Breaker{
				state:         tt.state,
				failuresCount: 5,
			}

			b.RecordSuccess()

			if b.failuresCount != 0 {
				t.Fatal("failures should be reset")
			}

			if b.state != tt.wantState {
				t.Fatalf("state = %v, want %v", b.state, tt.wantState)
			}
		})
	}
}

func TestRecordFailure(t *testing.T) {
	now := time.Date(2026, 7, 20, 12, 0, 0, 0, time.UTC)

	tests := []struct {
		name          string
		breaker       Breaker
		wantState     State
		wantFailures  int
		wantOpenedSet bool
	}{
		{
			name: "below threshold",
			breaker: Breaker{
				state:            Closed,
				failuresCount:    1,
				failureThreshold: 3,
				now:              func() time.Time { return now },
			},
			wantState:     Closed,
			wantFailures:  2,
			wantOpenedSet: false,
		},
		{
			name: "threshold reached",
			breaker: Breaker{
				state:            Closed,
				failuresCount:    2,
				failureThreshold: 3,
				now:              func() time.Time { return now },
			},
			wantState:     Open,
			wantFailures:  3,
			wantOpenedSet: true,
		},
		{
			name: "half open failure",
			breaker: Breaker{
				state: HalfOpen,
				now:   func() time.Time { return now },
			},
			wantState:     Open,
			wantFailures:  1,
			wantOpenedSet: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.breaker.RecordFailure()

			if tt.breaker.state != tt.wantState {
				t.Fatalf("state = %v, want %v", tt.breaker.state, tt.wantState)
			}

			if tt.breaker.failuresCount != tt.wantFailures {
				t.Fatalf("failures = %d, want %d",
					tt.breaker.failuresCount,
					tt.wantFailures)
			}

			if tt.wantOpenedSet && !tt.breaker.openedAt.Equal(now) {
				t.Fatal("openedAt not set correctly")
			}
		})
	}
}
