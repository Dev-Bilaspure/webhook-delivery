package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/dev-bilaspure/webhook-delivery/internal/config"
	"github.com/dev-bilaspure/webhook-delivery/internal/delivery"
	"github.com/dev-bilaspure/webhook-delivery/internal/kafka"
	"github.com/dev-bilaspure/webhook-delivery/internal/worker"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	cfg := config.Load()

	consumer := kafka.NewConsumer(cfg.KafkaBrokers, cfg.RetriesTopic, cfg.RetryGroup)
	defer consumer.Close()

	retryProducer := kafka.NewProducer(cfg.KafkaBrokers, cfg.RetriesTopic, cfg.ProducerBatchTimeout)
	dlqProducer := kafka.NewProducer(cfg.KafkaBrokers, cfg.DLQTopic, cfg.ProducerBatchTimeout)

	retryWorker := worker.NewWorker(
		consumer,
		delivery.NewDeliverer(cfg.DeliveryTimeout),
		retryProducer,
		dlqProducer,
		worker.RetryWorker,
		cfg,
	)

	retryWorker.Run(ctx)
	log.Println("shutdown signal received")

	if err := retryProducer.Close(); err != nil {
		log.Printf("retry producer close error: %v", err)
	}

	if err := dlqProducer.Close(); err != nil {
		log.Printf("dlq producer close error: %v", err)
	}

	log.Println("worker shut down cleanly")
}
