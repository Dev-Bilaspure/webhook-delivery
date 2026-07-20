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

	consumer := kafka.NewConsumer(cfg.KafkaBrokers, cfg.EventsTopic, cfg.DeliveryGroup)
	defer consumer.Close()

	deliveryWorker := worker.NewWorker(
		consumer,
		delivery.NewDeliverer(cfg.DeliveryTimeout),
		kafka.NewProducer(
			cfg.KafkaBrokers,
			cfg.RetriesTopic,
		),
		kafka.NewProducer(
			cfg.KafkaBrokers,
			cfg.DLQTopic,
		),
		worker.DeliveryWorker,
		cfg,
	)

	deliveryWorker.Run(ctx)

	log.Println("worker shut down cleanly")
}
