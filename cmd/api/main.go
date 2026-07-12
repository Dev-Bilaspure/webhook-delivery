package main

import (
	"log"
	"net/http"

	"github.com/dev-bilaspure/webhook-delivery/internal/httpapi"
	"github.com/dev-bilaspure/webhook-delivery/internal/kafka"
)

func main() {
	mux := http.NewServeMux()

	producer := kafka.NewProducer(
		[]string{"localhost:9092"},
		kafka.EventTopic,
	)

	defer producer.Close()

	server := httpapi.NewServer(producer)

	mux.HandleFunc("GET /healthz", httpapi.HealthCheck)
	mux.HandleFunc("POST /events", server.CreateEvent)

	log.Println("Server running on :8000")
	err := http.ListenAndServe(":8000", mux)

	if err != nil {
		log.Fatalf("Error starting server, %v", err)
	}
}
