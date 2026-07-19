package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/dev-bilaspure/webhook-delivery/internal/httpapi"
	"github.com/dev-bilaspure/webhook-delivery/internal/kafka"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	producer := kafka.NewProducer(
		[]string{"localhost:9092"},
		kafka.EventTopic,
	)

	apiServer := httpapi.NewServer(producer)

	mux := http.NewServeMux()

	mux.HandleFunc("GET /healthz", httpapi.HealthCheck)
	mux.HandleFunc("POST /events", apiServer.CreateEvent)

	server := &http.Server{
		Addr:              ":8000",
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       10 * time.Second,
		WriteTimeout:      15 * time.Second,
		IdleTimeout:       60 * time.Second,
	}

	go func() {
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("server error: %v", err)
		}
	}()
	log.Printf("server is listening on port :8000")

	<-ctx.Done()
	log.Println("shutdown signal received")

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := server.Shutdown(shutdownCtx); err != nil {
		log.Printf("graceful shutdown timed out: %v", err)
	}

	if err := producer.Close(); err != nil {
		log.Printf("producer close error: %v", err)
	}

	log.Println("api shutdown cleanly")
}
