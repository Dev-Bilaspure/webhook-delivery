package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/dev-bilaspure/webhook-delivery/internal/delivery"
	"github.com/dev-bilaspure/webhook-delivery/internal/kafka"
	"github.com/dev-bilaspure/webhook-delivery/internal/worker"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	consumer := kafka.NewConsumer([]string{"localhost:9092"}, kafka.RetryTopic, kafka.RetryWorkerGroup)
	defer consumer.Close()

	deliveryWorker := worker.NewWorker(
		consumer,
		delivery.NewDeliverer(),
		kafka.NewProducer(
			[]string{"localhost:9092"},
			kafka.RetryTopic,
		),
		kafka.NewProducer(
			[]string{"localhost:9092"},
			kafka.DLQTopic,
		),
		worker.RetryWorker,
	)

	deliveryWorker.Run(ctx)

	log.Println("worker shut down cleanly")
}
