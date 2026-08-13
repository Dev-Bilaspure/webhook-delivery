package main

import (
	"context"
	"errors"
	"log"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/dev-bilaspure/webhook-delivery/internal/config"
	"github.com/dev-bilaspure/webhook-delivery/internal/receiver"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	cfg := config.Load()

	store := receiver.NewStore()
	servers := make([]*http.Server, 0, len(cfg.ReceiverAddrs))

	for _, addr := range cfg.ReceiverAddrs {
		servers = append(servers, &http.Server{
			Addr:    addr,
			Handler: receiver.NewServer(store, addr).Handler(),
			// No read/write/idle timeouts: hang mode must outlive the worker's delivery
			// timeout, and idle connections must survive so the worker's pool is what
			// gets measured.
			ReadHeaderTimeout: 5 * time.Second,
		})
	}

	for _, server := range servers {
		go func() {
			log.Printf("receiver listening on %s", server.Addr)

			if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
				log.Printf("server error on %s: %v", server.Addr, err)
			}
		}()
	}

	<-ctx.Done()
	log.Println("shutdown signal received")

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	wg := sync.WaitGroup{}

	for _, server := range servers {
		wg.Add(1)
		go func() {
			defer wg.Done()

			if err := server.Shutdown(shutdownCtx); err != nil {
				log.Printf("graceful shutdown timed out on %s: %v", server.Addr, err)
			}
		}()
	}

	wg.Wait()

	log.Println("receiver shut down cleanly")
}
