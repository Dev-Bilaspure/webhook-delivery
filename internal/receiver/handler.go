package receiver

import (
	"encoding/json"
	"io"
	"log"
	"net/http"
	"strconv"
	"time"

	"github.com/dev-bilaspure/webhook-delivery/internal/httpapi"
)

type Server struct {
	store *Store
	inj   *Injector
	addr  string
}

func NewServer(store *Store, addr string) *Server {
	return &Server{
		store: store,
		inj:   NewInjector(),
		addr:  addr,
	}
}

func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()

	mux.HandleFunc("POST /webhook/{id}", s.deliver)
	mux.HandleFunc("POST /control", s.control)
	mux.HandleFunc("GET /stats", s.stats)
	mux.HandleFunc("GET /keys", s.keys)
	mux.HandleFunc("POST /reset", s.reset)

	return mux
}

type payload struct {
	OrderingKey string    `json:"orderingKey"`
	Seq         int       `json:"seq"`
	SentAt      time.Time `json:"sentAt"`
}

type statsResponse struct {
	Addr  string `json:"addr"`
	Mode  Mode   `json:"mode"`
	Stats Stats  `json:"stats"`
}

func (s *Server) deliver(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(r.Body)
	if err != nil {
		httpapi.WriteError(w, http.StatusBadRequest, "failed to read body")
		return
	}

	// Best effort: a hand-rolled body without these fields is still a delivery worth recording.
	p := payload{}
	_ = json.Unmarshal(body, &p)

	sample := Sample{
		Key:         r.Header.Get("Idempotency-Key"),
		OrderingKey: p.OrderingKey,
		Seq:         p.Seq,
		Addr:        s.addr,
		ArrivedAt:   time.Now(),
	}
	if !p.SentAt.IsZero() {
		sample.Latency = sample.ArrivedAt.Sub(p.SentAt)
	}

	s.store.Record(sample)

	outcome := s.inj.Next()

	if outcome.Delay > 0 {
		timer := time.NewTimer(outcome.Delay)
		defer timer.Stop()

		select {
		case <-timer.C:
		case <-r.Context().Done():
			return
		}
	}

	if outcome.RetryAfter > 0 {
		w.Header().Set("Retry-After", strconv.Itoa(int(outcome.RetryAfter.Seconds())))
	}

	w.WriteHeader(outcome.Status)
}

func (s *Server) control(w http.ResponseWriter, r *http.Request) {
	fault := Fault{}

	if err := json.NewDecoder(r.Body).Decode(&fault); err != nil {
		httpapi.WriteError(w, http.StatusBadRequest, "invalid JSON body")
		return
	}

	if err := s.inj.Set(fault); err != nil {
		httpapi.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}

	log.Printf("%s fault set to %s", s.addr, s.inj.Mode())

	if err := httpapi.WriteJSON(w, http.StatusOK, fault); err != nil {
		log.Printf("failed to encode response: %v", err)
	}
}

func (s *Server) stats(w http.ResponseWriter, r *http.Request) {
	response := statsResponse{
		Addr:  s.addr,
		Mode:  s.inj.Mode(),
		Stats: s.store.Stats(),
	}

	if err := httpapi.WriteJSON(w, http.StatusOK, response); err != nil {
		log.Printf("failed to encode response: %v", err)
	}
}

func (s *Server) keys(w http.ResponseWriter, r *http.Request) {
	if err := httpapi.WriteJSON(w, http.StatusOK, s.store.Keys()); err != nil {
		log.Printf("failed to encode response: %v", err)
	}
}

func (s *Server) reset(w http.ResponseWriter, r *http.Request) {
	s.store.Reset()
	w.WriteHeader(http.StatusNoContent)
}
