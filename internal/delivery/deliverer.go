package delivery

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/dev-bilaspure/webhook-delivery/internal/event"
)

type StatusError struct {
	StatusCode int
}

func (e *StatusError) Error() string {
	return fmt.Sprintf("error delivering msg with statusCode: %v", e.StatusCode)
}

func IsPermanent(err error) bool {
	var statusErr *StatusError
	if !errors.As(err, &statusErr) {
		return false
	}

	if statusErr.StatusCode < 400 || statusErr.StatusCode >= 500 {
		return false
	}

	return statusErr.StatusCode != http.StatusRequestTimeout &&
		statusErr.StatusCode != http.StatusTooManyRequests
}

type Deliverer struct {
	client *http.Client
}

func NewDeliverer(timeout time.Duration, maxIdleConns, maxIdleConnsPerHost int) *Deliverer {
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.MaxIdleConns = maxIdleConns
	transport.MaxIdleConnsPerHost = maxIdleConnsPerHost

	client := http.Client{
		Timeout:   timeout,
		Transport: transport,
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
	req.Header.Set("Idempotency-Key", e.ID)

	resp, err := d.client.Do(req)
	if err != nil {
		return fmt.Errorf("error firing delivery request, %w", err)
	}

	defer func() {
		io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
	}()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return &StatusError{StatusCode: resp.StatusCode}
	}

	return nil
}
