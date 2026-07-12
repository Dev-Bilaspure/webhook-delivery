package main

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log"
	"net/http"
	"time"

	"github.com/dev-bilaspure/webhook-delivery/internal/httpapi"
	"github.com/google/uuid"
)

func main() {
	const (
		count                     = 2000
		webhookDeliveryServiceUrl = "http://localhost:8000/events"
	)

	client := &http.Client{
		Timeout: 10 * time.Second,
	}

	for i := 0; i < count; i++ {
		payload := map[string]int{
			"countID": i + 1,
		}

		payloadBytes, err := json.Marshal(payload)
		if err != nil {
			log.Printf("failed marshal payload: %v", err)
			continue
		}

		event := httpapi.CreateEventRequest{
			EndpointURL: "http://localhost:8080/webhook/" + uuid.New().String(),
			Payload:     payloadBytes,
		}

		eventBytes, err := json.Marshal(event)
		if err != nil {
			log.Printf("failed marshal event: %v", err)
			continue
		}

		req, err := http.NewRequestWithContext(
			context.Background(),
			http.MethodPost,
			webhookDeliveryServiceUrl,
			bytes.NewReader(eventBytes),
		)
		if err != nil {
			log.Printf("failed to make request: %v", err)
			continue
		}

		req.Header.Set("Content-Type", "application/json")

		resp, err := client.Do(req)
		if err != nil {
			log.Printf("failed to fire event delivery request: %v", err)
			continue
		}

		func() {
			defer resp.Body.Close()
			_, _ = io.Copy(io.Discard, resp.Body)
		}()

		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			log.Printf("error delivering msg with statusCode: %v", resp.StatusCode)
			continue
		}
	}
}
