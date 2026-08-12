package delivery

import (
	"errors"
	"fmt"
	"testing"
)

func TestIsPermanent(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{"bad request", &StatusError{StatusCode: 400}, true},
		{"unauthorized", &StatusError{StatusCode: 401}, true},
		{"not found", &StatusError{StatusCode: 404}, true},
		{"gone", &StatusError{StatusCode: 410}, true},
		{"request timeout", &StatusError{StatusCode: 408}, false},
		{"too many requests", &StatusError{StatusCode: 429}, false},
		{"internal server error", &StatusError{StatusCode: 500}, false},
		{"bad gateway", &StatusError{StatusCode: 502}, false},
		{"service unavailable", &StatusError{StatusCode: 503}, false},
		{"redirect", &StatusError{StatusCode: 302}, false},
		{"wrapped bad request", fmt.Errorf("deliver: %w", &StatusError{StatusCode: 400}), true},
		{"transport error", errors.New("connection refused"), false},
		{"nil", nil, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := IsPermanent(tt.err); got != tt.want {
				t.Fatalf("IsPermanent(%v) = %v, want %v", tt.err, got, tt.want)
			}
		})
	}
}
