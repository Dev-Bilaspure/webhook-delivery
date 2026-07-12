package delivery

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/dev-bilaspure/webhook-delivery/internal/event"
)

type Deliverer struct {
	client *http.Client
}

func NewDeliverer() *Deliverer {
	client := http.Client{
		Timeout: 10 * time.Second,
	}
	return &Deliverer{
		client: &client,
	}
}

func (d *Deliverer) Deliver(ctx context.Context, e event.Event) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, e.EndpointURL, bytes.NewReader(e.Payload))
	if err != nil {
		return fmt.Errorf("error creating delivery request, %w", err)
	}

	req.Header.Set("Content-Type", "application/json")

	resp, err := d.client.Do(req)
	if err != nil {
		return fmt.Errorf("error firing delivery request, %w", err)
	}

	defer func() {
		io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
	}()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("error delivering msg with statusCode: %v", resp.StatusCode)
	}

	return nil
}
