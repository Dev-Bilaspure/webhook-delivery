package receiver

import (
	"math"
	"sort"
	"strings"
	"sync"
	"time"
)

type Sample struct {
	Key          string
	OrderingKey  string
	Seq          int
	Addr         string
	Status       int
	ArrivedAt    time.Time
	Latency      time.Duration
	FirstAttempt bool
}

type Store struct {
	mu      sync.Mutex
	samples []Sample
	seen    map[string]int
}

type Latency struct {
	Count int     `json:"count"`
	P50Ms float64 `json:"p50Ms"`
	P95Ms float64 `json:"p95Ms"`
	P99Ms float64 `json:"p99Ms"`
	MaxMs float64 `json:"maxMs"`
}

type Bucket struct {
	Second int `json:"second"`
	Count  int `json:"count"`
}

type AddrStats struct {
	Deliveries int      `json:"deliveries"`
	Throughput []Bucket `json:"throughput"`
}

type Stats struct {
	Deliveries   int                  `json:"deliveries"`
	Accepted     int                  `json:"accepted"`
	Rejected     int                  `json:"rejected"`
	UniqueEvents int                  `json:"uniqueEvents"`
	Duplicates   int                  `json:"duplicates"`
	FirstAttempt Latency              `json:"firstAttempt"`
	Retried      Latency              `json:"retried"`
	Inversions   int                  `json:"inversions"`
	OrderingKeys int                  `json:"orderingKeys"`
	Throughput   []Bucket             `json:"throughput"`
	ByAddr       map[string]AddrStats `json:"byAddr"`
}

func NewStore() *Store {
	return &Store{seen: make(map[string]int)}
}

func (s *Store) Record(sample Sample) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.seen[sample.Key]++
	sample.FirstAttempt = s.seen[sample.Key] == 1
	s.samples = append(s.samples, sample)
}

func (s *Store) Reset() {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.samples = nil
	s.seen = make(map[string]int)
}

func (s *Store) Keys(prefix string) []string {
	s.mu.Lock()
	defer s.mu.Unlock()

	seen := make(map[string]bool, len(s.seen))
	for _, sample := range s.samples {
		if strings.HasPrefix(sample.OrderingKey, prefix) {
			seen[sample.Key] = true
		}
	}

	keys := make([]string, 0, len(seen))
	for k := range seen {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	return keys
}

func (s *Store) Stats(prefix string) Stats {
	s.mu.Lock()
	samples := make([]Sample, 0, len(s.samples))
	for _, sample := range s.samples {
		if strings.HasPrefix(sample.OrderingKey, prefix) {
			samples = append(samples, sample)
		}
	}
	s.mu.Unlock()

	unique := make(map[string]bool, len(samples))
	for _, sample := range samples {
		unique[sample.Key] = true
	}

	stats := Stats{
		Deliveries:   len(samples),
		UniqueEvents: len(unique),
		Duplicates:   len(samples) - len(unique),
		ByAddr:       make(map[string]AddrStats),
	}

	if len(samples) == 0 {
		return stats
	}

	sort.Slice(samples, func(i, j int) bool {
		return samples[i].ArrivedAt.Before(samples[j].ArrivedAt)
	})
	origin := samples[0].ArrivedAt

	var first, retried []time.Duration
	byAddr := make(map[string][]Sample)

	for _, sample := range samples {
		if sample.Status >= 200 && sample.Status < 300 {
			stats.Accepted++
		} else {
			stats.Rejected++
		}

		if sample.Latency > 0 {
			if sample.FirstAttempt {
				first = append(first, sample.Latency)
			} else {
				retried = append(retried, sample.Latency)
			}
		}
		byAddr[sample.Addr] = append(byAddr[sample.Addr], sample)
	}

	stats.FirstAttempt = summarise(first)
	stats.Retried = summarise(retried)
	stats.Throughput = bucket(samples, origin)

	for addr, addrSamples := range byAddr {
		stats.ByAddr[addr] = AddrStats{
			Deliveries: len(addrSamples),
			Throughput: bucket(addrSamples, origin),
		}
	}

	stats.Inversions, stats.OrderingKeys = inversions(samples)

	return stats
}

func inversions(samples []Sample) (int, int) {
	highest := make(map[string]int)
	count := 0

	for _, sample := range samples {
		if sample.OrderingKey == "" || !sample.FirstAttempt {
			continue
		}

		if seen, ok := highest[sample.OrderingKey]; ok && sample.Seq < seen {
			count++
			continue
		}
		highest[sample.OrderingKey] = sample.Seq
	}

	return count, len(highest)
}

func bucket(samples []Sample, origin time.Time) []Bucket {
	counts := make(map[int]int)
	for _, sample := range samples {
		counts[int(sample.ArrivedAt.Sub(origin).Seconds())]++
	}

	seconds := make([]int, 0, len(counts))
	for second := range counts {
		seconds = append(seconds, second)
	}
	sort.Ints(seconds)

	buckets := make([]Bucket, 0, len(seconds))
	for _, second := range seconds {
		buckets = append(buckets, Bucket{Second: second, Count: counts[second]})
	}

	return buckets
}

func summarise(latencies []time.Duration) Latency {
	if len(latencies) == 0 {
		return Latency{}
	}

	sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })

	return Latency{
		Count: len(latencies),
		P50Ms: millis(percentile(latencies, 0.50)),
		P95Ms: millis(percentile(latencies, 0.95)),
		P99Ms: millis(percentile(latencies, 0.99)),
		MaxMs: millis(latencies[len(latencies)-1]),
	}
}

func percentile(sorted []time.Duration, p float64) time.Duration {
	i := int(math.Ceil(p*float64(len(sorted)))) - 1
	if i < 0 {
		i = 0
	}
	if i >= len(sorted) {
		i = len(sorted) - 1
	}
	return sorted[i]
}

func millis(d time.Duration) float64 {
	return float64(d) / float64(time.Millisecond)
}
