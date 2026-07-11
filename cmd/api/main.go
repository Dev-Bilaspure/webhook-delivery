package main

import (
	"encoding/json"
	"log"
	"net/http"
)

type HealthResponse struct {
	Health string `json:"health"`
}

func main() {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		healthResponse := HealthResponse{
			Health: "ok",
		}
		json.NewEncoder(w).Encode(healthResponse)

	})
	log.Println("Server running on :8000")
	err := http.ListenAndServe("localhost:8000", mux)

	if err != nil {
		log.Fatalf("Error starting server, %v", err)
	}
}
