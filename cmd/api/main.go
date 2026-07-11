package main

import (
	"log"
	"net/http"

	"github.com/dev-bilaspure/webhook-delivery/internal/httpapi"
)

func main() {
	mux := http.NewServeMux()

	mux.HandleFunc("GET /healthz", httpapi.HealthCheck)
	mux.HandleFunc("POST /events", httpapi.CreateEvent)

	log.Println("Server running on :8000")
	err := http.ListenAndServe("localhost:8000", mux)

	if err != nil {
		log.Fatalf("Error starting server, %v", err)
	}
}
