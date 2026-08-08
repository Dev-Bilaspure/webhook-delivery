package worker

import (
	"context"
	"encoding/json"
	"sync"
	"testing"
	"time"

	kafkago "github.com/segmentio/kafka-go"

	"github.com/dev-bilaspure/webhook-delivery/internal/breaker"
	"github.com/dev-bilaspure/webhook-delivery/internal/config"
	"github.com/dev-bilaspure/webhook-delivery/internal/event"
)

func testConfig() config.Config {
	return config.Config{
		MaxConcurrency:          1,
		MaxConcurrencyPerHost:   1,
		RetryCountLimit:         5,
		BaseBackoff:             time.Second,
		BreakerFailureThreshold: 5,
		BreakerCooldown:         time.Minute,
	}
}

func testMessage(t *testing.T, key string, nextRetryAt time.Time) kafkago.Message {
	t.Helper()

	value, err := json.Marshal(event.RetryEvent{
		Event: event.Event{
			ID:          "event-1",
			EndpointURL: "http://example.com/webhook/1",
		},
		NextRetryAt: nextRetryAt,
	})
	if err != nil {
		t.Fatalf("failed to marshal retry event: %v", err)
	}

	return kafkago.Message{Key: []byte(key), Value: value}
}

func TestDeliverGroupFailsWhenRetryWaitIsCancelled(t *testing.T) {
	w := &Worker{workerType: RetryWorker, cfg: testConfig()}

	msgs := []kafkago.Message{testMessage(t, "key-1", time.Now().Add(time.Hour))}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := w.deliverGroup(
		ctx,
		msgs,
		map[string]chan struct{}{},
		&sync.Mutex{},
		make(chan struct{}, w.cfg.MaxConcurrency),
		map[string]*breaker.Breaker{},
	)
	if err == nil {
		t.Fatal("deliverGroup returned nil for an undelivered group; the batch would be committed")
	}
}

func TestDeliverGroupUnblocksFromSaturatedSemaphore(t *testing.T) {
	w := &Worker{workerType: DeliveryWorker, cfg: testConfig()}

	hostSem := make(chan struct{}, w.cfg.MaxConcurrencyPerHost)
	hostSem <- struct{}{}

	msgs := []kafkago.Message{testMessage(t, "key-1", time.Now())}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	done := make(chan error, 1)
	go func() {
		done <- w.deliverGroup(
			ctx,
			msgs,
			map[string]chan struct{}{"example.com": hostSem},
			&sync.Mutex{},
			make(chan struct{}, w.cfg.MaxConcurrency),
			map[string]*breaker.Breaker{},
		)
	}()

	select {
	case err := <-done:
		if err == nil {
			t.Fatal("deliverGroup returned nil for an undelivered group; the batch would be committed")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("deliverGroup is stuck acquiring a saturated semaphore after cancellation")
	}
}
