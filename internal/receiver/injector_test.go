package receiver

import (
	"net/http"
	"strings"
	"testing"
	"time"
)

func pattern(i *Injector, requests int) string {
	var b strings.Builder
	for n := 0; n < requests; n++ {
		if i.Next().Status == http.StatusOK {
			b.WriteByte('.')
		} else {
			b.WriteByte('x')
		}
	}
	return b.String()
}

func TestFlapPattern(t *testing.T) {
	tests := []struct {
		name  string
		fault Fault
		want  string
	}{
		{
			name:  "fails then recovers for good",
			fault: Fault{Mode: ModeFlap, Status: 500, Failures: 3},
			want:  "xxx.......",
		},
		{
			name:  "cycles three failures and two successes",
			fault: Fault{Mode: ModeFlap, Status: 500, Failures: 3, Successes: 2},
			want:  "xxx..xxx..",
		},
		{
			name:  "alternates",
			fault: Fault{Mode: ModeFlap, Status: 500, Failures: 1, Successes: 1},
			want:  "x.x.x.x.x.",
		},
		{
			name:  "cycles two failures and three successes",
			fault: Fault{Mode: ModeFlap, Status: 500, Failures: 2, Successes: 3},
			want:  "xx...xx...",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			i := NewInjector()
			if err := i.Set(tt.fault); err != nil {
				t.Fatalf("Set() = %v, want nil", err)
			}

			if got := pattern(i, len(tt.want)); got != tt.want {
				t.Fatalf("pattern = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestSetRestartsTheFlapCycle(t *testing.T) {
	fault := Fault{Mode: ModeFlap, Status: 500, Failures: 2}

	i := NewInjector()
	if err := i.Set(fault); err != nil {
		t.Fatalf("Set() = %v, want nil", err)
	}

	if got := pattern(i, 4); got != "xx.." {
		t.Fatalf("pattern = %q, want %q", got, "xx..")
	}

	if err := i.Set(fault); err != nil {
		t.Fatalf("Set() = %v, want nil", err)
	}

	if got := pattern(i, 4); got != "xx.." {
		t.Fatalf("pattern after reconfigure = %q, want %q", got, "xx..")
	}
}

func TestOutcomePerMode(t *testing.T) {
	tests := []struct {
		name  string
		fault Fault
		want  Outcome
	}{
		{
			name:  "ok",
			fault: Fault{Mode: ModeOK},
			want:  Outcome{Status: http.StatusOK},
		},
		{
			name:  "server error",
			fault: Fault{Mode: ModeStatus, Status: http.StatusInternalServerError},
			want:  Outcome{Status: http.StatusInternalServerError},
		},
		{
			name:  "throttled with retry after",
			fault: Fault{Mode: ModeStatus, Status: http.StatusTooManyRequests, RetryAfter: "2s"},
			want:  Outcome{Status: http.StatusTooManyRequests, RetryAfter: 2 * time.Second},
		},
		{
			name:  "hang replies late",
			fault: Fault{Mode: ModeHang, Delay: "15s"},
			want:  Outcome{Status: http.StatusOK, Delay: 15 * time.Second},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			i := NewInjector()
			if err := i.Set(tt.fault); err != nil {
				t.Fatalf("Set() = %v, want nil", err)
			}

			if got := i.Next(); got != tt.want {
				t.Fatalf("Next() = %+v, want %+v", got, tt.want)
			}
		})
	}
}

func TestSetRejectsInvalidFaults(t *testing.T) {
	tests := []struct {
		name  string
		fault Fault
	}{
		{"unknown mode", Fault{Mode: "banana"}},
		{"status out of range", Fault{Mode: ModeStatus, Status: 42}},
		{"status missing", Fault{Mode: ModeStatus}},
		{"hang without delay", Fault{Mode: ModeHang}},
		{"hang with unparseable delay", Fault{Mode: ModeHang, Delay: "soon"}},
		{"flap without failures", Fault{Mode: ModeFlap, Status: 500}},
		{"flap with negative successes", Fault{Mode: ModeFlap, Status: 500, Failures: 1, Successes: -1}},
		{"flap without status", Fault{Mode: ModeFlap, Failures: 1}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			i := NewInjector()

			if err := i.Set(tt.fault); err == nil {
				t.Fatal("Set() = nil, want an error")
			}

			if i.Mode() != ModeOK {
				t.Fatalf("mode = %v, want %v; a rejected fault must not be applied", i.Mode(), ModeOK)
			}
		})
	}
}
