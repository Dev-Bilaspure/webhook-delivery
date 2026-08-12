package worker

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	kafkago "github.com/segmentio/kafka-go"

	"github.com/dev-bilaspure/webhook-delivery/internal/breaker"
	"github.com/dev-bilaspure/webhook-delivery/internal/config"
	"github.com/dev-bilaspure/webhook-delivery/internal/delivery"
	"github.com/dev-bilaspure/webhook-delivery/internal/event"
)

type recordingPublisher struct {
	mu       sync.Mutex
	messages []kafkago.Message
}

func (p *recordingPublisher) Publish(ctx context.Context, key string, value []byte) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.messages = append(p.messages, kafkago.Message{Key: []byte(key), Value: value})
	return nil
}

func (p *recordingPublisher) events(t *testing.T) []event.RetryEvent {
	t.Helper()

	p.mu.Lock()
	defer p.mu.Unlock()

	events := make([]event.RetryEvent, 0, len(p.messages))
	for _, msg := range p.messages {
		retryEvent := event.RetryEvent{}
		if err := json.Unmarshal(msg.Value, &retryEvent); err != nil {
			t.Fatalf("failed to unmarshal published message: %v", err)
		}
		events = append(events, retryEvent)
	}
	return events
}

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

func testMessageTo(t *testing.T, key, id, endpointURL string, nextRetryAt time.Time) kafkago.Message {
	t.Helper()

	value, err := json.Marshal(event.RetryEvent{
		Event: event.Event{
			ID:          id,
			EndpointURL: endpointURL,
		},
		NextRetryAt: nextRetryAt,
	})
	if err != nil {
		t.Fatalf("failed to marshal retry event: %v", err)
	}

	return kafkago.Message{Key: []byte(key), Value: value}
}

func testMessage(t *testing.T, key string, nextRetryAt time.Time) kafkago.Message {
	t.Helper()

	return testMessageTo(t, key, "event-1", "http://example.com/webhook/1", nextRetryAt)
}

func TestDeliverGroupDefersQueuedMessagesAfterAFailure(t *testing.T) {
	var received int

	endpoint := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		received++
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer endpoint.Close()

	retryProducer := &recordingPublisher{}

	w := &Worker{
		deliverer:     delivery.NewDeliverer(time.Second),
		retryProducer: retryProducer,
		dlqProducer:   &recordingPublisher{},
		workerType:    DeliveryWorker,
		cfg:           testConfig(),
	}

	msgs := make([]kafkago.Message, 0, 3)
	for _, id := range []string{"event-1", "event-2", "event-3"} {
		msgs = append(msgs, testMessageTo(t, "key-1", id, endpoint.URL, time.Now()))
	}

	err := w.deliverGroup(
		context.Background(),
		msgs,
		map[string]chan struct{}{},
		&sync.Mutex{},
		make(chan struct{}, w.cfg.MaxConcurrency),
		map[string]*breaker.Breaker{},
	)
	if err != nil {
		t.Fatalf("deliverGroup returned %v, want nil; every message has a durable home", err)
	}

	if received != 1 {
		t.Fatalf("endpoint received %d requests, want 1; messages behind the failure were delivered out of order", received)
	}

	published := retryProducer.events(t)
	if len(published) != 3 {
		t.Fatalf("published %d messages to retries, want 3", len(published))
	}

	for i, want := range []string{"event-1", "event-2", "event-3"} {
		if published[i].Event.ID != want {
			t.Fatalf("retries[%d] = %s, want %s; ordering not preserved", i, published[i].Event.ID, want)
		}
	}

	if published[0].RetryCount != 1 {
		t.Fatalf("attempted message has RetryCount %d, want 1", published[0].RetryCount)
	}

	for _, e := range published[1:] {
		if e.RetryCount != 0 {
			t.Fatalf("deferred message %s has RetryCount %d, want 0; it was never attempted", e.Event.ID, e.RetryCount)
		}
		if !e.NextRetryAt.Equal(published[0].NextRetryAt) {
			t.Fatalf("deferred message %s is not scheduled with the message it queued behind", e.Event.ID)
		}
	}
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
