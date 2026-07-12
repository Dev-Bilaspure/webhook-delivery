package main

import (
	"context"
	"encoding/json"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/dev-bilaspure/webhook-delivery/internal/delivery"
	"github.com/dev-bilaspure/webhook-delivery/internal/event"
	"github.com/dev-bilaspure/webhook-delivery/internal/kafka"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	consumer := kafka.NewConsumer([]string{"localhost:9092"}, "events", "delivery-worker")
	defer consumer.Close()

	deliverer := delivery.NewDeliverer()

	for {
		msg, err := consumer.Fetch(ctx)
		if err != nil {
			if ctx.Err() != nil {
				break
			}
			log.Printf("fetch error %v", err)
			continue
		}

		var e event.Event

		if err := json.Unmarshal(msg.Value, &e); err != nil {
			log.Printf("failed decoding msg.Value %v", err)
			consumer.Commit(ctx, msg)
			continue
		}

		if err := deliverer.Deliver(ctx, e); err != nil {
			log.Printf("failed to deliver msg for Key %s: %v", msg.Key, err)
		} else {
			log.Printf("delivered %s to %s", e.ID, e.EndpointURL)
		}

		consumer.Commit(ctx, msg)
	}

	log.Println("worker shut down cleanly")
}
