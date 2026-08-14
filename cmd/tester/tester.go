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
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/dev-bilaspure/webhook-delivery/internal/event"
	"github.com/dev-bilaspure/webhook-delivery/internal/httpapi"
	"github.com/dev-bilaspure/webhook-delivery/internal/receiver"
)

const drainPoll = time.Second

type options struct {
	apiURL       string
	statsURL     string
	endpoints    []string
	events       int
	keys         int
	concurrency  int
	timeout      time.Duration
	wait         bool
	drainQuiet   time.Duration
	drainTimeout time.Duration
	profile      loadProfile
	rate         int
}

type job struct {
	orderingKey string
	seq         int
	endpointURL string
	sentAt      time.Time
}

type results struct {
	mu     sync.Mutex
	ids    []string
	failed int
}

type loadProfile string

const (
	burstProfile  loadProfile = "burst"
	steadyProfile loadProfile = "steady"
)

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

func (r *results) submitted() []string {
	r.mu.Lock()
	defer r.mu.Unlock()

	ids := make([]string, len(r.ids))
	copy(ids, r.ids)

	return ids
}

func main() {
	opts, err := parseOptions()
	if err != nil {
		log.Fatal(err)
	}

	inFlight := opts.concurrency
	if opts.profile == steadyProfile {
		inFlight = opts.keys
	}

	client := &http.Client{
		Timeout: opts.timeout,
		Transport: &http.Transport{
			MaxIdleConns:        inFlight * 2,
			MaxIdleConnsPerHost: inFlight,
		},
	}

	if opts.profile == steadyProfile {
		log.Printf(
			"submitting %d events across %d ordering keys and %d hosts at %d/s",
			opts.events, opts.keys, len(opts.endpoints), opts.rate,
		)
	} else {
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
	}

	res := &results{ids: make([]string, 0, opts.events)}

	collisions := 0
	start := time.Now()

	if opts.profile == steadyProfile {
		collisions = submitSteady(client, opts, res)
	} else {
		wg := sync.WaitGroup{}

		for worker := 0; worker < opts.concurrency; worker++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				submitKeys(client, opts, worker, res)
			}()
		}
		wg.Wait()
	}

	elapsed := time.Since(start)

	report(res, opts, collisions, elapsed)

	if !opts.wait {
		return
	}

	stats, missing, reason, drainedAfter := waitForDrain(client, opts, res.submitted())

	reportDelivery(stats, missing, drainedAfter, reason)
}

func parseOptions() (options, error) {
	apiURL := flag.String("api", "http://localhost:8000/events", "URL of the events endpoint")
	endpoints := flag.String("endpoints", "http://localhost:8080,http://localhost:8081,http://localhost:8082", "comma-separated receiver base URLs, one per destination host")
	events := flag.Int("events", 2000, "number of events to submit")
	keys := flag.Int("keys", 50, "number of distinct ordering keys")
	concurrency := flag.Int("concurrency", 20, "number of concurrent submitters")
	timeout := flag.Duration("timeout", 10*time.Second, "per-request timeout")
	statsURL := flag.String("stats", "http://localhost:8080", "receiver base URL to read results from")
	wait := flag.Bool("wait", true, "wait for delivery to drain and report receiver metrics")
	drainQuiet := flag.Duration("drain-quiet", 20*time.Second, "stop waiting after this long with no new deliveries")
	drainTimeout := flag.Duration("drain-timeout", 5*time.Minute, "stop waiting after this long regardless")
	profile := flag.String("profile", "burst", "Request load profile, burst or steady")
	rate := flag.Int("rate", 500, "events per second for the steady profile")

	flag.Parse()

	opts := options{
		apiURL:       *apiURL,
		statsURL:     strings.TrimSuffix(strings.TrimSpace(*statsURL), "/"),
		events:       *events,
		keys:         *keys,
		concurrency:  *concurrency,
		timeout:      *timeout,
		wait:         *wait,
		drainQuiet:   *drainQuiet,
		drainTimeout: *drainTimeout,
		profile:      loadProfile(*profile),
		rate:         *rate,
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
	if opts.profile != burstProfile && opts.profile != steadyProfile {
		return options{}, errors.New("unknown profile")
	}
	if opts.profile == steadyProfile && opts.rate < 1 {
		return options{}, errors.New("invalid request rate")
	}

	return opts, nil
}

func submitSteady(client *http.Client, opts options, res *results) int {
	wg := sync.WaitGroup{}
	seqs := make(map[string]int)

	free := make(chan int, opts.keys)
	for k := 0; k < opts.keys; k++ {
		free <- k
	}

	sent, collisions := 0, 0

	interval := time.Second / time.Duration(opts.rate)
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for sent < opts.events {
		scheduled := <-ticker.C

		select {
		case k := <-free:
			key := fmt.Sprintf("cust-%d", k)
			seqs[key]++
			sent++

			endpoint := opts.endpoints[k%len(opts.endpoints)]
			j := job{
				orderingKey: key,
				seq:         seqs[key],
				endpointURL: endpoint + "/webhook/" + key,
				sentAt:      scheduled,
			}

			wg.Add(1)
			go func() {
				defer wg.Done()
				defer func() {
					free <- k
				}()

				id, err := submit(client, opts.apiURL, j)
				if err != nil {
					res.rejected()
					log.Printf("failed to submit %s#%d: %v", key, j.seq, err)
					return
				}

				res.accepted(id)
			}()
		default:
			collisions++
		}
	}

	wg.Wait()

	return collisions
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
			sentAt:      time.Now().UTC(),
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
		SentAt:      j.sentAt,
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

func waitForDrain(client *http.Client, opts options, submitted []string) (receiver.Stats, []string, string, time.Duration) {
	start := time.Now()
	deadline := start.Add(opts.drainTimeout)
	quietSince := start
	last := -1

	stats := receiver.Stats{}
	missing := submitted

	for {
		outstanding, idsErr := missingIDs(client, opts, submitted)
		if idsErr != nil {
			log.Printf("failed to read delivered ids: %v", idsErr)
		} else {
			missing = outstanding
		}

		fetched, statsErr := fetchStats(client, opts.statsURL)
		if statsErr != nil {
			log.Printf("failed to read receiver stats: %v", statsErr)
		} else {
			stats = fetched

			if stats.Deliveries != last {
				last = stats.Deliveries
				quietSince = time.Now()
			}
		}

		if idsErr == nil && statsErr == nil && len(missing) == 0 {
			return stats, missing, "every submitted event arrived", time.Since(start)
		}

		if time.Since(quietSince) >= opts.drainQuiet {
			return stats, missing, fmt.Sprintf("no new deliveries for %s", opts.drainQuiet), time.Since(start)
		}

		if time.Now().After(deadline) {
			return stats, missing, fmt.Sprintf("gave up after %s", opts.drainTimeout), time.Since(start)
		}

		time.Sleep(drainPoll)
	}
}

func missingIDs(client *http.Client, opts options, submitted []string) ([]string, error) {
	arrived, err := fetchKeys(client, opts.statsURL)
	if err != nil {
		return nil, err
	}

	seen := make(map[string]bool, len(arrived))
	for _, id := range arrived {
		seen[id] = true
	}

	missing := make([]string, 0)
	for _, id := range submitted {
		if !seen[id] {
			missing = append(missing, id)
		}
	}

	return missing, nil
}

func fetchStats(client *http.Client, statsURL string) (receiver.Stats, error) {
	response := receiver.StatsResponse{}
	if err := getJSON(client, statsURL+"/stats", &response); err != nil {
		return receiver.Stats{}, err
	}

	return response.Stats, nil
}

func fetchKeys(client *http.Client, statsURL string) ([]string, error) {
	keys := []string{}
	if err := getJSON(client, statsURL+"/keys", &keys); err != nil {
		return nil, err
	}

	return keys, nil
}

func getJSON(client *http.Client, url string, into any) error {
	resp, err := client.Get(url)
	if err != nil {
		return err
	}
	defer func() {
		_, _ = io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
	}()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("unexpected status %d from %s", resp.StatusCode, url)
	}

	return json.NewDecoder(resp.Body).Decode(into)
}

func reportDelivery(stats receiver.Stats, missing []string, drainedAfter time.Duration, reason string) {
	fmt.Printf("\ndrained after    %s  (%s)\n", drainedAfter.Round(time.Millisecond), reason)
	fmt.Printf("arrived          %d  (unique %d, duplicates %d)\n", stats.Deliveries, stats.UniqueEvents, stats.Duplicates)
	fmt.Printf("  accepted       %d\n", stats.Accepted)
	fmt.Printf("  refused        %d\n", stats.Rejected)

	fmt.Printf("unaccounted      %d", len(missing))
	if len(missing) > 0 {
		fmt.Printf("  %s  (cross-check against dead-letter depth)", sample(missing, 5))
	}
	fmt.Println()

	fmt.Printf("\nfirst-attempt    %s\n", latency(stats.FirstAttempt))
	fmt.Printf("retried          %s\n", latency(stats.Retried))
	fmt.Printf("peak delivery    %d/s\n", peak(stats.Throughput))

	fmt.Printf("\nordering         %d inversions across %d keys\n", stats.Inversions, stats.OrderingKeys)
	fmt.Printf("per host         %s\n", perHost(stats.ByAddr))
}

func latency(l receiver.Latency) string {
	if l.Count == 0 {
		return "no samples"
	}

	return fmt.Sprintf(
		"n=%-6d p50 %.1fms  p95 %.1fms  p99 %.1fms  max %.1fms",
		l.Count, l.P50Ms, l.P95Ms, l.P99Ms, l.MaxMs,
	)
}

func peak(buckets []receiver.Bucket) int {
	highest := 0
	for _, bucket := range buckets {
		if bucket.Count > highest {
			highest = bucket.Count
		}
	}

	return highest
}

func perHost(byAddr map[string]receiver.AddrStats) string {
	addrs := make([]string, 0, len(byAddr))
	for addr := range byAddr {
		addrs = append(addrs, addr)
	}
	sort.Strings(addrs)

	parts := make([]string, 0, len(addrs))
	for _, addr := range addrs {
		parts = append(parts, fmt.Sprintf("%s=%d", addr, byAddr[addr].Deliveries))
	}

	return strings.Join(parts, "  ")
}

func sample(ids []string, limit int) string {
	if len(ids) > limit {
		return strings.Join(ids[:limit], " ") + " ..."
	}

	return strings.Join(ids, " ")
}

func report(res *results, opts options, collisions int, elapsed time.Duration) {
	res.mu.Lock()
	defer res.mu.Unlock()

	fmt.Printf("\nsubmitted        %d\n", len(res.ids))
	fmt.Printf("accept errors    %d\n", res.failed)
	fmt.Printf("duration         %s\n", elapsed.Round(time.Millisecond))
	fmt.Printf("accept rate      %.0f/s\n", float64(len(res.ids))/elapsed.Seconds())

	if opts.profile == steadyProfile {
		fmt.Printf("key collisions   %d  (offered %d/s)\n", collisions, opts.rate)
	}
}
