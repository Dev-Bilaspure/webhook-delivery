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
	"os"
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
	json         bool
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

type result struct {
	Profile     string         `json:"profile"`
	Events      int            `json:"events"`
	Keys        int            `json:"keys"`
	Hosts       int            `json:"hosts"`
	Concurrency int            `json:"concurrency"`
	Rate        int            `json:"rate"`
	Submitted   int            `json:"submitted"`
	AcceptErrs  int            `json:"acceptErrors"`
	SubmitMs    float64        `json:"submitMs"`
	AcceptRate  float64        `json:"acceptRatePerSec"`
	Collisions  int            `json:"keyCollisions"`
	Drained     bool           `json:"drained"`
	DrainedMs   float64        `json:"drainedMs"`
	DrainReason string         `json:"drainReason"`
	Unaccounted int            `json:"unaccounted"`
	MissingIDs  string         `json:"missingIds,omitempty"`
	Stats       receiver.Stats `json:"stats"`
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

	run := result{
		Profile:     string(opts.profile),
		Events:      opts.events,
		Keys:        opts.keys,
		Hosts:       len(opts.endpoints),
		Concurrency: opts.concurrency,
		Rate:        opts.rate,
		Submitted:   len(res.submitted()),
		AcceptErrs:  res.failed,
		SubmitMs:    millis(elapsed),
		AcceptRate:  float64(len(res.submitted())) / elapsed.Seconds(),
		Collisions:  collisions,
	}

	if opts.wait {
		stats, missing, reason, drainedAfter := waitForDrain(client, opts, res.submitted())

		run.Drained = true
		run.DrainedMs = millis(drainedAfter)
		run.DrainReason = reason
		run.Unaccounted = len(missing)
		run.MissingIDs = sample(missing, 10)
		run.Stats = stats
	}

	if opts.json {
		if err := json.NewEncoder(os.Stdout).Encode(run); err != nil {
			log.Fatalf("failed to encode result: %v", err)
		}
		return
	}

	report(run)
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
	asJSON := flag.Bool("json", false, "emit the result as a single JSON object instead of a report")

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
		json:         *asJSON,
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

func report(r result) {
	fmt.Printf("\nsubmitted        %d\n", r.Submitted)
	fmt.Printf("accept errors    %d\n", r.AcceptErrs)
	fmt.Printf("duration         %.0fms\n", r.SubmitMs)
	fmt.Printf("accept rate      %.0f/s\n", r.AcceptRate)

	if r.Profile == string(steadyProfile) {
		fmt.Printf("key collisions   %d  (offered %d/s)\n", r.Collisions, r.Rate)
	}

	if !r.Drained {
		return
	}

	s := r.Stats

	fmt.Printf("\ndrained after    %.0fms  (%s)\n", r.DrainedMs, r.DrainReason)
	fmt.Printf("arrived          %d  (unique %d, duplicates %d)\n", s.Deliveries, s.UniqueEvents, s.Duplicates)
	fmt.Printf("  accepted       %d\n", s.Accepted)
	fmt.Printf("  refused        %d\n", s.Rejected)

	fmt.Printf("unaccounted      %d", r.Unaccounted)
	if r.Unaccounted > 0 {
		fmt.Printf("  %s  (cross-check against dead-letter depth)", r.MissingIDs)
	}
	fmt.Println()

	fmt.Printf("\nfirst-attempt    %s\n", latency(s.FirstAttempt))
	fmt.Printf("retried          %s\n", latency(s.Retried))
	fmt.Printf("peak delivery    %d/s\n", peak(s.Throughput))

	fmt.Printf("\nordering         %d inversions across %d keys\n", s.Inversions, s.OrderingKeys)
	fmt.Printf("per host         %s\n", perHost(s.ByAddr))
}

func millis(d time.Duration) float64 {
	return float64(d) / float64(time.Millisecond)
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
