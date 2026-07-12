package worker

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/dev-bilaspure/webhook-delivery/internal/delivery"
	"github.com/dev-bilaspure/webhook-delivery/internal/event"
	"github.com/dev-bilaspure/webhook-delivery/internal/kafka"
)

type WorkerType string

const (
	DeliveryWorker WorkerType = "delivery_worker"
	RetryWorker    WorkerType = "retry_worker"
	DLQWorker      WorkerType = "dlq_worker"
)

type Worker struct {
	consumer      *kafka.Consumer
	deliverer     *delivery.Deliverer
	retryProducer *kafka.Producer
	dlqProducer   *kafka.Producer
	workerType    WorkerType
}

func NewWorker(consumer *kafka.Consumer, deliverer *delivery.Deliverer, retryProducer *kafka.Producer, dlqProducer *kafka.Producer, workerType WorkerType) *Worker {
	return &Worker{
		consumer:      consumer,
		deliverer:     deliverer,
		retryProducer: retryProducer,
		dlqProducer:   dlqProducer,
		workerType:    workerType,
	}
}

func (w *Worker) Run(ctx context.Context) {
	for {
		msg, err := w.consumer.Fetch(ctx)
		if err != nil {
			if ctx.Err() != nil {
				break
			}
			log.Printf("failed to fetch message: %v", err)
			continue
		}

		var retryEvent event.RetryEvent

		if err := json.Unmarshal(msg.Value, &retryEvent); err != nil {
			log.Printf("failed to Unmarshal msg.Value: %v", err)
			w.consumer.Commit(ctx, msg)
			continue
		}

		if w.workerType == RetryWorker {
			if err := waitUntil(ctx, retryEvent.NextRetryAt); err != nil {
				break
			}
		}

		if err := w.deliverer.Deliver(ctx, retryEvent.Event); err != nil {
			if err := w.handleFailure(ctx, string(msg.Key), &retryEvent); err != nil {
				log.Printf("failed to handle retry: %v", err)
				continue
			}
			log.Printf("failed to deliver msg for Key %s: %v", msg.Key, err)
		} else {
			log.Printf("delivered %s to %s", retryEvent.Event.ID, retryEvent.Event.EndpointURL)
		}

		w.consumer.Commit(ctx, msg)
	}
}

func (w *Worker) handleFailure(ctx context.Context, key string, retryEvent *event.RetryEvent) error {
	retryEvent.RetryCount++

	if retryEvent.RetryCount >= retryCountLimit {
		retryEventBytes, err := json.Marshal(retryEvent)
		if err != nil {
			return fmt.Errorf("failed to marshal retry event: %w", err)
		}
		if err := w.dlqProducer.Publish(ctx, key, retryEventBytes); err != nil {
			return fmt.Errorf("failed to publish message to dlq: %w", err)
		}

		log.Printf(
			"message %s moved to dead-letter queue after %d retries",
			retryEvent.Event.ID,
			retryEvent.RetryCount,
		)

		return nil
	}

	retryEvent.NextRetryAt = getNextRetryAt(retryEvent.RetryCount)

	retryEventBytes, err := json.Marshal(retryEvent)
	if err != nil {
		return fmt.Errorf("failed to marshal retry event: %w", err)
	}

	if err := w.retryProducer.Publish(ctx, key, retryEventBytes); err != nil {
		return fmt.Errorf("failed to publish retry message %w", err)
	}

	log.Printf(
		"scheduled retry #%d for event %s at %s",
		retryEvent.RetryCount,
		retryEvent.Event.ID,
		retryEvent.NextRetryAt.Format(time.RFC3339),
	)

	return nil
}

func getNextRetryAt(retryCount int) time.Time {
	backoff := baseBackoff * time.Duration(1<<retryCount)
	return time.Now().UTC().Add(backoff)
}

func waitUntil(ctx context.Context, t time.Time) error {
	if !t.After(time.Now()) {
		return nil
	}

	timer := time.NewTimer(time.Until(t))
	defer timer.Stop()

	select {
	case <-timer.C:
		return nil

	case <-ctx.Done():
		return ctx.Err()
	}
}
