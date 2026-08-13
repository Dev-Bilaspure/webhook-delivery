package receiver

import (
	"fmt"
	"testing"
	"time"
)

var base = time.Date(2026, 8, 13, 12, 0, 0, 0, time.UTC)

func TestPercentiles(t *testing.T) {
	tests := []struct {
		name      string
		latencies []int
		wantP50   float64
		wantP95   float64
		wantP99   float64
		wantMax   float64
	}{
		{
			name:      "ten samples, p95 and p99 land on the same one",
			latencies: []int{12, 8, 45, 11, 9, 210, 13, 10, 14, 11},
			wantP50:   11,
			wantP95:   210,
			wantP99:   210,
			wantMax:   210,
		},
		{
			name:      "hundred samples separate the tail",
			latencies: rangeOf(1, 100),
			wantP50:   50,
			wantP95:   95,
			wantP99:   99,
			wantMax:   100,
		},
		{
			name:      "single sample",
			latencies: []int{7},
			wantP50:   7,
			wantP95:   7,
			wantP99:   7,
			wantMax:   7,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := NewStore()
			for n, ms := range tt.latencies {
				s.Record(Sample{
					Key:       fmt.Sprintf("event-%d", n),
					Addr:      ":8080",
					ArrivedAt: base.Add(time.Duration(n) * time.Millisecond),
					Latency:   time.Duration(ms) * time.Millisecond,
				})
			}

			got := s.Stats().FirstAttempt

			if got.Count != len(tt.latencies) {
				t.Fatalf("count = %d, want %d", got.Count, len(tt.latencies))
			}
			if got.P50Ms != tt.wantP50 {
				t.Fatalf("p50 = %v, want %v", got.P50Ms, tt.wantP50)
			}
			if got.P95Ms != tt.wantP95 {
				t.Fatalf("p95 = %v, want %v", got.P95Ms, tt.wantP95)
			}
			if got.P99Ms != tt.wantP99 {
				t.Fatalf("p99 = %v, want %v", got.P99Ms, tt.wantP99)
			}
			if got.MaxMs != tt.wantMax {
				t.Fatalf("max = %v, want %v", got.MaxMs, tt.wantMax)
			}
		})
	}
}

func TestFirstAttemptIsSeparatedFromRetries(t *testing.T) {
	s := NewStore()

	s.Record(Sample{Key: "event-1", Addr: ":8080", ArrivedAt: base, Latency: 12 * time.Millisecond})
	s.Record(Sample{Key: "event-1", Addr: ":8080", ArrivedAt: base.Add(2 * time.Second), Latency: 2014 * time.Millisecond})

	stats := s.Stats()

	if stats.Deliveries != 2 {
		t.Fatalf("deliveries = %d, want 2", stats.Deliveries)
	}
	if stats.UniqueEvents != 1 {
		t.Fatalf("uniqueEvents = %d, want 1", stats.UniqueEvents)
	}
	if stats.Duplicates != 1 {
		t.Fatalf("duplicates = %d, want 1", stats.Duplicates)
	}

	if stats.FirstAttempt.Count != 1 || stats.FirstAttempt.P50Ms != 12 {
		t.Fatalf("firstAttempt = %+v, want one sample at 12ms", stats.FirstAttempt)
	}

	if stats.Retried.Count != 1 || stats.Retried.P50Ms != 2014 {
		t.Fatalf("retried = %+v, want one sample at 2014ms; backoff must not reach the headline", stats.Retried)
	}
}

func TestThroughputBuckets(t *testing.T) {
	s := NewStore()

	offsets := []time.Duration{
		100 * time.Millisecond,
		400 * time.Millisecond,
		1200 * time.Millisecond,
		2900 * time.Millisecond,
		2950 * time.Millisecond,
	}

	for n, offset := range offsets {
		s.Record(Sample{
			Key:       fmt.Sprintf("event-%d", n),
			Addr:      ":8080",
			ArrivedAt: base.Add(offset),
		})
	}

	want := []Bucket{{Second: 0, Count: 2}, {Second: 1, Count: 1}, {Second: 2, Count: 2}}
	got := s.Stats().Throughput

	if len(got) != len(want) {
		t.Fatalf("throughput = %+v, want %+v", got, want)
	}
	for n := range want {
		if got[n] != want[n] {
			t.Fatalf("throughput = %+v, want %+v", got, want)
		}
	}
}

func TestPerAddressCounts(t *testing.T) {
	s := NewStore()

	addrs := []string{":8080", ":8080", ":8080", ":8081", ":8082", ":8082"}
	for n, addr := range addrs {
		s.Record(Sample{
			Key:       fmt.Sprintf("event-%d", n),
			Addr:      addr,
			ArrivedAt: base.Add(time.Duration(n) * time.Millisecond),
		})
	}

	byAddr := s.Stats().ByAddr

	for addr, want := range map[string]int{":8080": 3, ":8081": 1, ":8082": 2} {
		if got := byAddr[addr].Deliveries; got != want {
			t.Fatalf("byAddr[%s] = %d, want %d", addr, got, want)
		}
	}
}

func TestAcceptedAndRejectedCounts(t *testing.T) {
	s := NewStore()

	statuses := []int{200, 200, 204, 400, 500, 429}
	for n, status := range statuses {
		s.Record(Sample{
			Key:       fmt.Sprintf("event-%d", n),
			Addr:      ":8080",
			Status:    status,
			ArrivedAt: base.Add(time.Duration(n) * time.Millisecond),
		})
	}

	stats := s.Stats()

	if stats.Deliveries != 6 {
		t.Fatalf("deliveries = %d, want 6", stats.Deliveries)
	}
	if stats.Accepted != 3 {
		t.Fatalf("accepted = %d, want 3; only 2xx counts as accepted", stats.Accepted)
	}
	if stats.Rejected != 3 {
		t.Fatalf("rejected = %d, want 3", stats.Rejected)
	}
	if stats.Accepted+stats.Rejected != stats.Deliveries {
		t.Fatalf("accepted + rejected = %d, want %d; every arrival has an outcome",
			stats.Accepted+stats.Rejected, stats.Deliveries)
	}
}

func TestInversions(t *testing.T) {
	tests := []struct {
		name  string
		seqs  map[string][]int
		want  int
		wantK int
	}{
		{
			name:  "in order",
			seqs:  map[string][]int{"cust-1": {1, 2, 3, 4}},
			want:  0,
			wantK: 1,
		},
		{
			name:  "one late arrival",
			seqs:  map[string][]int{"cust-1": {1, 3, 2, 4}},
			want:  1,
			wantK: 1,
		},
		{
			name:  "reversed",
			seqs:  map[string][]int{"cust-1": {4, 3, 2, 1}},
			want:  3,
			wantK: 1,
		},
		{
			name:  "keys are independent",
			seqs:  map[string][]int{"cust-1": {1, 2}, "cust-2": {2, 1}},
			want:  1,
			wantK: 2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := NewStore()
			arrival := 0

			for key, seqs := range tt.seqs {
				for _, seq := range seqs {
					s.Record(Sample{
						Key:         fmt.Sprintf("%s-%d", key, seq),
						OrderingKey: key,
						Seq:         seq,
						Addr:        ":8080",
						ArrivedAt:   base.Add(time.Duration(arrival) * time.Millisecond),
					})
					arrival++
				}
			}

			stats := s.Stats()

			if stats.Inversions != tt.want {
				t.Fatalf("inversions = %d, want %d", stats.Inversions, tt.want)
			}
			if stats.OrderingKeys != tt.wantK {
				t.Fatalf("orderingKeys = %d, want %d", stats.OrderingKeys, tt.wantK)
			}
		})
	}
}

func TestRetriesAreNotCountedAsInversions(t *testing.T) {
	s := NewStore()

	for n, seq := range []int{1, 2, 3} {
		s.Record(Sample{
			Key:         fmt.Sprintf("event-%d", seq),
			OrderingKey: "cust-1",
			Seq:         seq,
			Addr:        ":8080",
			ArrivedAt:   base.Add(time.Duration(n) * time.Millisecond),
		})
	}

	s.Record(Sample{
		Key:         "event-1",
		OrderingKey: "cust-1",
		Seq:         1,
		Addr:        ":8080",
		ArrivedAt:   base.Add(time.Second),
	})

	if got := s.Stats().Inversions; got != 0 {
		t.Fatalf("inversions = %d, want 0; a redelivery is at-least-once working, not a violation", got)
	}
}

func TestResetClearsSamples(t *testing.T) {
	s := NewStore()

	s.Record(Sample{Key: "event-1", Addr: ":8080", ArrivedAt: base, Latency: time.Millisecond})
	s.Reset()

	stats := s.Stats()

	if stats.Deliveries != 0 || stats.UniqueEvents != 0 {
		t.Fatalf("stats = %+v, want empty", stats)
	}

	if len(s.Keys()) != 0 {
		t.Fatalf("keys = %v, want empty", s.Keys())
	}

	s.Record(Sample{Key: "event-2", Addr: ":8080", ArrivedAt: base, Latency: 5 * time.Millisecond})

	if got := s.Stats().FirstAttempt.Count; got != 1 {
		t.Fatalf("firstAttempt count = %d, want 1; a key recorded after reset is a first attempt", got)
	}
}

func rangeOf(from, to int) []int {
	values := make([]int, 0, to-from+1)
	for n := from; n <= to; n++ {
		values = append(values, n)
	}
	return values
}
