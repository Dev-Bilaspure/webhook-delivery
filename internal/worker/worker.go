package worker

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/url"
	"sync"
	"time"

	kafkago "github.com/segmentio/kafka-go"

	"github.com/dev-bilaspure/webhook-delivery/internal/breaker"
	"github.com/dev-bilaspure/webhook-delivery/internal/config"
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
	cfg           config.Config
}

func NewWorker(consumer *kafka.Consumer, deliverer *delivery.Deliverer, retryProducer *kafka.Producer, dlqProducer *kafka.Producer, workerType WorkerType, cfg config.Config) *Worker {
	return &Worker{
		consumer:      consumer,
		deliverer:     deliverer,
		retryProducer: retryProducer,
		dlqProducer:   dlqProducer,
		workerType:    workerType,
		cfg:           cfg,
	}
}

func (w *Worker) Run(ctx context.Context) {
	hostBreakerMap := make(map[string]*breaker.Breaker)
	mu := sync.Mutex{}
	for {
		wg := sync.WaitGroup{}

		batchMessages, err := w.fetchBatchMessages(ctx)
		if err != nil {
			if ctx.Err() != nil {
				break
			}
			log.Printf("failed to fetch the message batch: %v", err)
			continue
		}
		if len(batchMessages) == 0 {
			continue
		}
		messageGroups := w.groupMessages(batchMessages)

		globalSem := make(chan struct{}, w.cfg.MaxConcurrency)
		perHostSem := make(map[string]chan struct{})

		isGroupSuccessChan := make(chan bool, len(messageGroups))

		for _, msgs := range messageGroups {
			wg.Add(1)
			go func(msgs []kafkago.Message) {
				defer func() {
					wg.Done()
				}()
				if err := w.deliverGroup(ctx, msgs, perHostSem, &mu, globalSem, hostBreakerMap); err != nil {
					isGroupSuccessChan <- false
				} else {
					isGroupSuccessChan <- true
				}
			}(msgs)
		}
		wg.Wait()
		close(isGroupSuccessChan)

		isBatchSuccess := true
		for isGroupSuccess := range isGroupSuccessChan {
			if !isGroupSuccess {
				isBatchSuccess = false
			}
		}
		if !isBatchSuccess {
			continue
		}
		if err := w.consumer.Commit(ctx, batchMessages...); err != nil {
			log.Printf("failed to commit batch: %v", err)
		}
	}
}

func (w *Worker) deliverGroup(
	ctx context.Context,
	messages []kafkago.Message,
	perHostSem map[string]chan struct{},
	mu *sync.Mutex,
	globalSem chan struct{},
	hostBreakerMap map[string]*breaker.Breaker,
) error {
	for _, msg := range messages {
		retryEvent := event.RetryEvent{}

		err := json.Unmarshal(msg.Value, &retryEvent)
		if err != nil {
			if err := w.sendToDLQ(ctx, &msg); err != nil {
				return err
			}
			log.Printf("failed to unmarshal message: %v", err)
			continue
		}

		if w.workerType == RetryWorker {
			if err := waitUntil(ctx, retryEvent.NextRetryAt); err != nil {
				break
			}
		}

		u, err := url.Parse(retryEvent.Event.EndpointURL)
		if err != nil {
			if err := w.sendToDLQ(ctx, &msg); err != nil {
				return err
			}
			log.Printf("failed to parse the URL for url `%v`: %v", retryEvent.Event.EndpointURL, err)
			continue
		}
		host := u.Host

		mu.Lock()
		hostBreaker, isBreakerExists := hostBreakerMap[host]
		if !isBreakerExists {
			hostBreakerMap[host] = breaker.NewBreaker(w.cfg.BreakerFailureThreshold, w.cfg.BreakerCooldown)
			hostBreaker = hostBreakerMap[host]
		}
		allowed := hostBreaker.Allow()
		mu.Unlock()

		if !allowed {
			if err := w.handleFailure(ctx, string(msg.Key), &retryEvent); err != nil {
				return err
			}
			log.Printf("breaker open for %v; routed to retries", host)
			continue
		}

		if err := func() error {
			mu.Lock()
			hostChan, ok := perHostSem[host]
			if !ok {
				perHostSem[host] = make(chan struct{}, w.cfg.MaxConcurrencyPerHost)
				hostChan = perHostSem[host]
			}
			mu.Unlock()

			hostChan <- struct{}{}
			defer func() {
				<-hostChan
			}()

			globalSem <- struct{}{}
			defer func() {
				<-globalSem
			}()

			if err := w.deliverer.Deliver(ctx, retryEvent.Event); err != nil {
				mu.Lock()
				hostBreaker.RecordFailure()
				mu.Unlock()
				if err := w.handleFailure(ctx, string(msg.Key), &retryEvent); err != nil {
					return err
				}
				log.Printf("failed to deliver msg for Key %s: %v", msg.Key, err)
			} else {
				mu.Lock()
				hostBreaker.RecordSuccess()
				mu.Unlock()
				log.Printf("delivered %s to %s", retryEvent.Event.ID, retryEvent.Event.EndpointURL)
			}
			return nil
		}(); err != nil {
			return err
		}
	}
	return nil
}

func (w *Worker) fetchBatchMessages(ctx context.Context) ([]kafkago.Message, error) {
	fillCtx, cancel := context.WithTimeout(ctx, w.cfg.BatchFillTimeout)
	defer cancel()
	batchMessages := make([]kafkago.Message, 0, w.cfg.BatchCapacity)

	for len(batchMessages) < w.cfg.BatchCapacity {
		msg, err := w.consumer.Fetch(fillCtx)
		if err != nil {
			if ctx.Err() != nil {
				return nil, ctx.Err() // parent context cancelled
			}
			if fillCtx.Err() != nil {
				break // fill timeout expired
			}
			return nil, err // some other kafka error
		}
		batchMessages = append(batchMessages, msg)
	}
	return batchMessages, nil
}

func (w *Worker) sendToDLQ(ctx context.Context, msg *kafkago.Message) error {
	if err := w.dlqProducer.Publish(ctx, string(msg.Key), msg.Value); err != nil {
		return fmt.Errorf("failed to publish message to dlq: %w", err)
	}
	return nil
}

func (w *Worker) groupMessages(messages []kafkago.Message) map[string][]kafkago.Message {
	groups := make(map[string][]kafkago.Message)
	for _, msg := range messages {
		groups[string(msg.Key)] = append(groups[string(msg.Key)], msg)
	}
	return groups
}

func (w *Worker) handleFailure(ctx context.Context, key string, retryEvent *event.RetryEvent) error {
	retryEvent.RetryCount++

	if retryEvent.RetryCount >= w.cfg.RetryCountLimit {
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

	retryEvent.NextRetryAt = w.getNextRetryAt(retryEvent.RetryCount)

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

func (w *Worker) getNextRetryAt(retryCount int) time.Time {
	backoff := w.cfg.BaseBackoff * time.Duration(1<<retryCount)
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
