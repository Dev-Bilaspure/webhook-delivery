package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/dev-bilaspure/webhook-delivery/internal/event"
	"github.com/dev-bilaspure/webhook-delivery/internal/httpapi"
	"github.com/dev-bilaspure/webhook-delivery/internal/receiver"
)

type options struct {
	apiURL      string
	endpoints   []string
	events      int
	keys        int
	concurrency int
	timeout     time.Duration
}

type job struct {
	orderingKey string
	seq         int
	endpointURL string
}

type results struct {
	mu     sync.Mutex
	ids    []string
	failed int
}

func (r *results) accepted(id string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.ids = append(r.ids, id)
}

func (r *results) rejected() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.failed++
}

func main() {
	opts, err := parseOptions()
	if err != nil {
		log.Fatal(err)
	}

	// Sized to the offered concurrency so the generator is never the bottleneck being measured.
	client := &http.Client{
		Timeout: opts.timeout,
		Transport: &http.Transport{
			MaxIdleConns:        opts.concurrency * 2,
			MaxIdleConnsPerHost: opts.concurrency,
		},
	}

	log.Printf(
		"submitting %d events across %d ordering keys and %d hosts at concurrency %d",
		opts.events, opts.keys, len(opts.endpoints), opts.concurrency,
	)

	if opts.concurrency > opts.keys {
		log.Printf(
			"concurrency %d exceeds %d ordering keys; effective concurrency is %d",
			opts.concurrency, opts.keys, opts.keys,
		)
	}

	res := &results{ids: make([]string, 0, opts.events)}

	start := time.Now()

	wg := sync.WaitGroup{}
	for worker := 0; worker < opts.concurrency; worker++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			submitKeys(client, opts, worker, res)
		}()
	}
	wg.Wait()

	elapsed := time.Since(start)

	report(res, elapsed)
}

func parseOptions() (options, error) {
	apiURL := flag.String("api", "http://localhost:8000/events", "URL of the events endpoint")
	endpoints := flag.String("endpoints", "http://localhost:8080,http://localhost:8081,http://localhost:8082", "comma-separated receiver base URLs, one per destination host")
	events := flag.Int("events", 2000, "number of events to submit")
	keys := flag.Int("keys", 50, "number of distinct ordering keys")
	concurrency := flag.Int("concurrency", 20, "number of concurrent submitters")
	timeout := flag.Duration("timeout", 10*time.Second, "per-request timeout")

	flag.Parse()

	opts := options{
		apiURL:      *apiURL,
		events:      *events,
		keys:        *keys,
		concurrency: *concurrency,
		timeout:     *timeout,
	}

	for _, endpoint := range strings.Split(*endpoints, ",") {
		if trimmed := strings.TrimSpace(endpoint); trimmed != "" {
			opts.endpoints = append(opts.endpoints, strings.TrimSuffix(trimmed, "/"))
		}
	}

	if len(opts.endpoints) == 0 {
		return options{}, errors.New("at least one endpoint is required")
	}
	if opts.events < 1 {
		return options{}, errors.New("events must be at least 1")
	}
	if opts.keys < 1 {
		return options{}, errors.New("keys must be at least 1")
	}
	if opts.concurrency < 1 {
		return options{}, errors.New("concurrency must be at least 1")
	}

	return opts, nil
}

func submitKeys(client *http.Client, opts options, worker int, res *results) {
	seqs := make(map[string]int)

	for n := 0; n < opts.events; n++ {
		keyIndex := n % opts.keys
		if keyIndex%opts.concurrency != worker {
			continue
		}

		key := fmt.Sprintf("cust-%d", keyIndex)
		seqs[key]++

		endpoint := opts.endpoints[keyIndex%len(opts.endpoints)]

		id, err := submit(client, opts.apiURL, job{
			orderingKey: key,
			seq:         seqs[key],
			endpointURL: endpoint + "/webhook/" + key,
		})
		if err != nil {
			res.rejected()
			log.Printf("failed to submit %s#%d: %v", key, seqs[key], err)
			continue
		}

		res.accepted(id)
	}
}

func submit(client *http.Client, apiURL string, j job) (string, error) {
	payload, err := json.Marshal(receiver.Payload{
		OrderingKey: j.orderingKey,
		Seq:         j.seq,
		SentAt:      time.Now().UTC(),
	})
	if err != nil {
		return "", fmt.Errorf("failed to marshal payload: %w", err)
	}

	body, err := json.Marshal(httpapi.CreateEventRequest{
		EndpointURL: j.endpointURL,
		OrderingKey: j.orderingKey,
		Payload:     payload,
	})
	if err != nil {
		return "", fmt.Errorf("failed to marshal event: %w", err)
	}

	req, err := http.NewRequestWithContext(context.Background(), http.MethodPost, apiURL, bytes.NewReader(body))
	if err != nil {
		return "", fmt.Errorf("failed to build request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := client.Do(req)
	if err != nil {
		return "", err
	}
	defer func() {
		_, _ = io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
	}()

	if resp.StatusCode != http.StatusAccepted {
		return "", fmt.Errorf("unexpected status %d", resp.StatusCode)
	}

	created := event.Event{}
	if err := json.NewDecoder(resp.Body).Decode(&created); err != nil {
		return "", fmt.Errorf("failed to decode response: %w", err)
	}

	return created.ID, nil
}

func report(res *results, elapsed time.Duration) {
	res.mu.Lock()
	defer res.mu.Unlock()

	fmt.Printf("\nsubmitted        %d\n", len(res.ids))
	fmt.Printf("accept errors    %d\n", res.failed)
	fmt.Printf("duration         %s\n", elapsed.Round(time.Millisecond))
	fmt.Printf("accept rate      %.0f/s\n", float64(len(res.ids))/elapsed.Seconds())
}
