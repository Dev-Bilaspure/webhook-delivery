package receiver

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func newTestServer() (*Server, http.Handler) {
	srv := NewServer(NewStore(), ":8080")
	return srv, srv.Handler()
}

func webhookRequest(t *testing.T, key string, body any) *http.Request {
	t.Helper()

	encoded, err := json.Marshal(body)
	if err != nil {
		t.Fatalf("failed to marshal body: %v", err)
	}

	req := httptest.NewRequest(http.MethodPost, "/webhook/abc", bytes.NewReader(encoded))
	req.Header.Set("Idempotency-Key", key)

	return req
}

func controlRequest(body string) *http.Request {
	return httptest.NewRequest(http.MethodPost, "/control", strings.NewReader(body))
}

func do(handler http.Handler, req *http.Request) *httptest.ResponseRecorder {
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	return rec
}

func TestControlChangesDeliveryBehaviour(t *testing.T) {
	srv, handler := newTestServer()

	if got := do(handler, webhookRequest(t, "event-1", Payload{})).Code; got != http.StatusOK {
		t.Fatalf("status = %d, want %d before any fault is set", got, http.StatusOK)
	}

	if got := do(handler, controlRequest(`{"mode":"status","status":500}`)).Code; got != http.StatusOK {
		t.Fatalf("control status = %d, want %d", got, http.StatusOK)
	}

	if got := do(handler, webhookRequest(t, "event-2", Payload{})).Code; got != http.StatusInternalServerError {
		t.Fatalf("status = %d, want %d after the fault is set", got, http.StatusInternalServerError)
	}

	if got := do(handler, controlRequest(`{"mode":"ok"}`)).Code; got != http.StatusOK {
		t.Fatalf("control status = %d, want %d", got, http.StatusOK)
	}

	if got := do(handler, webhookRequest(t, "event-3", Payload{})).Code; got != http.StatusOK {
		t.Fatalf("status = %d, want %d after recovering", got, http.StatusOK)
	}

	if got := srv.store.Stats().Deliveries; got != 3 {
		t.Fatalf("deliveries = %d, want 3; refused deliveries still arrived", got)
	}
}

func TestControlRejectsBadRequestsWithoutChangingTheFault(t *testing.T) {
	tests := []struct {
		name string
		body string
	}{
		{"malformed json", `{"mode":`},
		{"unknown mode", `{"mode":"banana"}`},
		{"status out of range", `{"mode":"status","status":42}`},
		{"hang without delay", `{"mode":"hang"}`},
		{"unparseable delay", `{"mode":"hang","delay":"soon"}`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			srv, handler := newTestServer()

			if got := do(handler, controlRequest(tt.body)).Code; got != http.StatusBadRequest {
				t.Fatalf("control status = %d, want %d", got, http.StatusBadRequest)
			}

			if got := srv.inj.Mode(); got != ModeOK {
				t.Fatalf("mode = %v, want %v; a rejected fault must not be applied", got, ModeOK)
			}

			if got := do(handler, webhookRequest(t, "event-1", Payload{})).Code; got != http.StatusOK {
				t.Fatalf("status = %d, want %d; deliveries must be unaffected", got, http.StatusOK)
			}
		})
	}
}

func TestDeliverRecordsThePayloadMetadata(t *testing.T) {
	srv, handler := newTestServer()

	sent := time.Now().Add(-500 * time.Millisecond)
	body := Payload{OrderingKey: "cust-7", Seq: 42, SentAt: sent}

	if got := do(handler, webhookRequest(t, "event-1", body)).Code; got != http.StatusOK {
		t.Fatalf("status = %d, want %d", got, http.StatusOK)
	}

	if len(srv.store.samples) != 1 {
		t.Fatalf("recorded %d samples, want 1", len(srv.store.samples))
	}

	sample := srv.store.samples[0]

	if sample.Key != "event-1" {
		t.Fatalf("key = %q, want %q; taken from the Idempotency-Key header", sample.Key, "event-1")
	}
	if sample.OrderingKey != "cust-7" {
		t.Fatalf("orderingKey = %q, want %q", sample.OrderingKey, "cust-7")
	}
	if sample.Seq != 42 {
		t.Fatalf("seq = %d, want 42", sample.Seq)
	}
	if sample.Addr != ":8080" {
		t.Fatalf("addr = %q, want %q", sample.Addr, ":8080")
	}
	if !sample.FirstAttempt {
		t.Fatal("firstAttempt = false, want true")
	}
	if sample.Latency < 500*time.Millisecond || sample.Latency > 5*time.Second {
		t.Fatalf("latency = %v, want roughly 500ms", sample.Latency)
	}
}

func TestDeliverAcceptsABodyWithoutMetadata(t *testing.T) {
	srv, handler := newTestServer()

	if got := do(handler, webhookRequest(t, "event-1", map[string]string{"hello": "world"})).Code; got != http.StatusOK {
		t.Fatalf("status = %d, want %d; an unrecognised body is still a delivery", got, http.StatusOK)
	}

	if len(srv.store.samples) != 1 {
		t.Fatalf("recorded %d samples, want 1", len(srv.store.samples))
	}

	sample := srv.store.samples[0]

	if sample.Latency != 0 {
		t.Fatalf("latency = %v, want 0; there was no sentAt to measure against", sample.Latency)
	}
	if sample.OrderingKey != "" {
		t.Fatalf("orderingKey = %q, want empty", sample.OrderingKey)
	}

	if got := srv.store.Stats().FirstAttempt.Count; got != 0 {
		t.Fatalf("firstAttempt count = %d, want 0; an unmeasurable sample must not enter the percentiles", got)
	}
}

func TestHangReturnsWhenTheClientDisconnects(t *testing.T) {
	_, handler := newTestServer()

	if got := do(handler, controlRequest(`{"mode":"hang","delay":"30s"}`)).Code; got != http.StatusOK {
		t.Fatalf("control status = %d, want %d", got, http.StatusOK)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	req := webhookRequest(t, "event-1", Payload{}).WithContext(ctx)

	start := time.Now()
	do(handler, req)
	elapsed := time.Since(start)

	if elapsed > 5*time.Second {
		t.Fatalf("handler took %v; it should abandon the delay when the client gives up", elapsed)
	}
}

func TestResetClearsSamplesButKeepsTheFault(t *testing.T) {
	srv, handler := newTestServer()

	if got := do(handler, controlRequest(`{"mode":"status","status":500}`)).Code; got != http.StatusOK {
		t.Fatalf("control status = %d, want %d", got, http.StatusOK)
	}

	do(handler, webhookRequest(t, "event-1", Payload{}))

	if got := do(handler, httptest.NewRequest(http.MethodPost, "/reset", nil)).Code; got != http.StatusNoContent {
		t.Fatalf("reset status = %d, want %d", got, http.StatusNoContent)
	}

	if got := srv.store.Stats().Deliveries; got != 0 {
		t.Fatalf("deliveries = %d, want 0 after reset", got)
	}

	if got := do(handler, webhookRequest(t, "event-2", Payload{})).Code; got != http.StatusInternalServerError {
		t.Fatalf("status = %d, want %d; reset clears samples, not the configured fault", got, http.StatusInternalServerError)
	}
}

func TestKeysListsEveryRecordedEvent(t *testing.T) {
	_, handler := newTestServer()

	for _, key := range []string{"event-2", "event-1", "event-2"} {
		do(handler, webhookRequest(t, key, Payload{}))
	}

	rec := do(handler, httptest.NewRequest(http.MethodGet, "/keys", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("keys status = %d, want %d", rec.Code, http.StatusOK)
	}

	var keys []string
	if err := json.NewDecoder(rec.Body).Decode(&keys); err != nil {
		t.Fatalf("failed to decode keys: %v", err)
	}

	want := []string{"event-1", "event-2"}
	if len(keys) != len(want) {
		t.Fatalf("keys = %v, want %v; duplicates collapse to one entry", keys, want)
	}
	for n := range want {
		if keys[n] != want[n] {
			t.Fatalf("keys = %v, want %v", keys, want)
		}
	}
}

func TestStatsReportsAddressAndMode(t *testing.T) {
	_, handler := newTestServer()

	if got := do(handler, controlRequest(`{"mode":"flap","status":503,"failures":2}`)).Code; got != http.StatusOK {
		t.Fatalf("control status = %d, want %d", got, http.StatusOK)
	}

	rec := do(handler, httptest.NewRequest(http.MethodGet, "/stats", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("stats status = %d, want %d", rec.Code, http.StatusOK)
	}

	response := statsResponse{}
	if err := json.NewDecoder(rec.Body).Decode(&response); err != nil {
		t.Fatalf("failed to decode stats: %v", err)
	}

	if response.Addr != ":8080" {
		t.Fatalf("addr = %q, want %q", response.Addr, ":8080")
	}
	if response.Mode != ModeFlap {
		t.Fatalf("mode = %v, want %v", response.Mode, ModeFlap)
	}
}
