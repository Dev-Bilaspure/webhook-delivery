package main

import (
	"log"
	"net/http"
	"sync"

	"github.com/dev-bilaspure/webhook-delivery/internal/httpapi"
)

func main() {
	mux := http.NewServeMux()

	eventKeyStore := make(map[string]bool)
	dedupedStore := make(map[string]bool)
	var mu sync.RWMutex

	mux.HandleFunc("POST /webhook/{id}", func(w http.ResponseWriter, r *http.Request) {
		idempotency := r.Header.Get("Idempotency-Key")

		mu.Lock()
		_, exists := eventKeyStore[idempotency]
		if exists {
			dedupedStore[idempotency] = true
		} else {
			eventKeyStore[idempotency] = true
		}
		mu.Unlock()

		if err := httpapi.WriteJSON(w, http.StatusOK, map[string]bool{"success": true}); err != nil {
			log.Printf("failed to encode response: %v", err.Error())
		}
	})

	mux.HandleFunc("GET /store", func(w http.ResponseWriter, r *http.Request) {
		mu.RLock()
		eventKeyStoreSnapshot := make(map[string]bool, len(eventKeyStore))
		for k, v := range eventKeyStore {
			eventKeyStoreSnapshot[k] = v
		}

		dedupedStoreSnapshot := make(map[string]bool, len(dedupedStore))
		for k, v := range dedupedStore {
			dedupedStoreSnapshot[k] = v
		}
		mu.RUnlock()

		if err := httpapi.WriteJSON(w, http.StatusOK, map[string]any{
			"eventKeyStore": eventKeyStoreSnapshot,
			"dedupedStore":  dedupedStoreSnapshot,
		}); err != nil {
			log.Printf("failed to encode response: %v", err.Error())
		}
	})

	server := &http.Server{
		Addr:    ":8080",
		Handler: mux,
	}

	log.Printf("Server listening on %s", server.Addr)

	if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		log.Fatalf("server error: %v", err)
	}
}
