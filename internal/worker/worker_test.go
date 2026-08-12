package worker

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
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

func TestDeliverGroupDeadLettersPermanentRejections(t *testing.T) {
	var received []string

	endpoint := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		received = append(received, r.Header.Get("Idempotency-Key"))
		w.WriteHeader(http.StatusBadRequest)
	}))
	defer endpoint.Close()

	retryProducer := &recordingPublisher{}
	dlqProducer := &recordingPublisher{}

	cfg := testConfig()
	cfg.BreakerFailureThreshold = 1

	w := &Worker{
		deliverer:     delivery.NewDeliverer(time.Second),
		retryProducer: retryProducer,
		dlqProducer:   dlqProducer,
		workerType:    DeliveryWorker,
		cfg:           cfg,
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
		t.Fatalf("deliverGroup returned %v, want nil", err)
	}

	if len(received) != 3 {
		t.Fatalf("endpoint received %d requests, want 3; a rejected payload tripped the breaker on a healthy host", len(received))
	}

	if len(dlqProducer.messages) != 3 {
		t.Fatalf("dead-lettered %d messages, want 3", len(dlqProducer.messages))
	}

	if len(retryProducer.messages) != 0 {
		t.Fatalf("published %d messages to retries, want 0; a permanent rejection cannot succeed on retry", len(retryProducer.messages))
	}
}

func TestDeliverGroupKeepsRetryBudgetWhenBreakerIsOpen(t *testing.T) {
	var received int

	endpoint := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		received++
		w.WriteHeader(http.StatusOK)
	}))
	defer endpoint.Close()

	u, err := url.Parse(endpoint.URL)
	if err != nil {
		t.Fatalf("failed to parse endpoint url: %v", err)
	}

	cfg := testConfig()
	cfg.BaseBackoff = time.Second
	cfg.BreakerCooldown = 30 * time.Second
	cfg.BreakerFailureThreshold = 1

	openBreaker := breaker.NewBreaker(cfg.BreakerFailureThreshold, cfg.BreakerCooldown)
	openBreaker.RecordFailure()

	retryProducer := &recordingPublisher{}
	dlqProducer := &recordingPublisher{}

	w := &Worker{
		deliverer:     delivery.NewDeliverer(time.Second),
		retryProducer: retryProducer,
		dlqProducer:   dlqProducer,
		workerType:    DeliveryWorker,
		cfg:           cfg,
	}

	msgs := make([]kafkago.Message, 0, 3)
	for _, id := range []string{"event-1", "event-2", "event-3"} {
		msgs = append(msgs, testMessageTo(t, "key-1", id, endpoint.URL, time.Now()))
	}

	before := time.Now().UTC()

	if err := w.deliverGroup(
		context.Background(),
		msgs,
		map[string]chan struct{}{},
		&sync.Mutex{},
		make(chan struct{}, cfg.MaxConcurrency),
		map[string]*breaker.Breaker{u.Host: openBreaker},
	); err != nil {
		t.Fatalf("deliverGroup returned %v, want nil", err)
	}

	if received != 0 {
		t.Fatalf("endpoint received %d requests, want 0; an open breaker must not attempt delivery", received)
	}

	published := retryProducer.events(t)
	if len(published) != 3 {
		t.Fatalf("published %d messages to retries, want 3", len(published))
	}

	for _, e := range published {
		if e.RetryCount != 0 {
			t.Fatalf("message %s has RetryCount %d, want 0; a refused message spent no attempt", e.Event.ID, e.RetryCount)
		}

		if e.NextRetryAt.Before(before.Add(cfg.BreakerCooldown / 2)) {
			t.Fatalf("message %s is due at %v, too soon; it should wait for the breaker cooldown, not the backoff ladder", e.Event.ID, e.NextRetryAt)
		}
	}

	if len(dlqProducer.messages) != 0 {
		t.Fatalf("dead-lettered %d messages, want 0", len(dlqProducer.messages))
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
