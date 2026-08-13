// Package receiver holds the test receiver's fault injection and delivery bookkeeping.
package receiver

import (
	"errors"
	"fmt"
	"net/http"
	"sync"
	"time"
)

type Mode string

const (
	ModeOK     Mode = "ok"
	ModeStatus Mode = "status"
	ModeHang   Mode = "hang"
	ModeFlap   Mode = "flap"
)

type Fault struct {
	Mode       Mode   `json:"mode"`
	Status     int    `json:"status,omitempty"`
	RetryAfter string `json:"retryAfter,omitempty"`
	Delay      string `json:"delay,omitempty"`
	Failures   int    `json:"failures,omitempty"`
	Successes  int    `json:"successes,omitempty"`
}

type Outcome struct {
	Status     int
	RetryAfter time.Duration
	Delay      time.Duration
}

type Injector struct {
	mu         sync.Mutex
	mode       Mode
	status     int
	retryAfter time.Duration
	delay      time.Duration
	failures   int
	successes  int
	seen       int
}

func NewInjector() *Injector {
	return &Injector{mode: ModeOK}
}

func (i *Injector) Set(f Fault) error {
	retryAfter, err := optionalDuration(f.RetryAfter)
	if err != nil {
		return fmt.Errorf("invalid retryAfter: %w", err)
	}

	delay, err := optionalDuration(f.Delay)
	if err != nil {
		return fmt.Errorf("invalid delay: %w", err)
	}

	switch f.Mode {
	case ModeOK:
	case ModeStatus:
		if err := validStatus(f.Status); err != nil {
			return err
		}
	case ModeHang:
		if delay <= 0 {
			return errors.New("hang requires a positive delay")
		}
	case ModeFlap:
		if f.Failures <= 0 {
			return errors.New("flap requires a positive failures count")
		}
		if f.Successes < 0 {
			return errors.New("flap requires a non-negative successes count")
		}
		if err := validStatus(f.Status); err != nil {
			return err
		}
	default:
		return fmt.Errorf("unknown mode %q", f.Mode)
	}

	i.mu.Lock()
	defer i.mu.Unlock()

	i.mode = f.Mode
	i.status = f.Status
	i.retryAfter = retryAfter
	i.delay = delay
	i.failures = f.Failures
	i.successes = f.Successes
	i.seen = 0

	return nil
}

func (i *Injector) Next() Outcome {
	i.mu.Lock()
	defer i.mu.Unlock()

	i.seen++

	switch i.mode {
	case ModeStatus:
		return Outcome{Status: i.status, RetryAfter: i.retryAfter}

	case ModeHang:
		return Outcome{Status: http.StatusOK, Delay: i.delay}

	case ModeFlap:
		if i.flapping() {
			return Outcome{Status: i.status, RetryAfter: i.retryAfter}
		}
		return Outcome{Status: http.StatusOK}

	default:
		return Outcome{Status: http.StatusOK}
	}
}

func (i *Injector) Mode() Mode {
	i.mu.Lock()
	defer i.mu.Unlock()

	return i.mode
}

// seen is already incremented for the current request, so request N sees seen == N.
func (i *Injector) flapping() bool {
	if i.successes == 0 {
		return i.seen <= i.failures
	}
	return (i.seen-1)%(i.failures+i.successes) < i.failures
}

func validStatus(status int) error {
	if status < 100 || status > 599 {
		return fmt.Errorf("status %d out of range", status)
	}
	return nil
}

func optionalDuration(v string) (time.Duration, error) {
	if v == "" {
		return 0, nil
	}
	return time.ParseDuration(v)
}
