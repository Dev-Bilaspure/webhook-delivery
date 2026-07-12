package event

import (
	"encoding/json"
	"errors"
	"net/url"
	"time"
)

type Event struct {
	ID          string          `json:"id"`
	EndpointURL string          `json:"endpointURL"`
	Payload     json.RawMessage `json:"payload"`
	CreatedAt   time.Time       `json:"createdAt"`
}

type RetryEvent struct {
	Event       Event     `json:"event"`
	RetryCount  int       `json:"retryCount"`
	NextRetryAt time.Time `json:"nextRetry"`
}

func (e Event) ValidateUrl() error {
	if e.EndpointURL == "" {
		return errors.New("endpoint URL is required")
	}

	u, err := url.Parse(e.EndpointURL)
	if err != nil {
		return err
	}

	if u.Scheme != "http" && u.Scheme != "https" {
		return errors.New("URL must use http or https")
	}

	if u.Host == "" {
		return errors.New("URL must include a host")
	}

	return nil
}
