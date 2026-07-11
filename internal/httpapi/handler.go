package httpapi

import (
	"encoding/json"
	"log"
	"net/http"
	"time"

	"github.com/dev-bilaspure/webhook-delivery/internal/event"
	"github.com/google/uuid"
)

type createEventRequest struct {
	EndpointURL string          `json:"endpointURL"`
	Payload     json.RawMessage `json:"payload"`
}

func CreateEvent(w http.ResponseWriter, r *http.Request) {
	var req createEventRequest

	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON body")
		return
	}

	e := event.Event{
		ID:          uuid.New().String(),
		EndpointURL: req.EndpointURL,
		Payload:     req.Payload,
		CreatedAt:   time.Now().UTC(),
	}

	if err := e.ValidateUrl(); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	if err := writeJson(w, http.StatusAccepted, e); err != nil {
		log.Printf("failed to encode response: %v", err.Error())
	}
}

type healthResponse struct {
	Health string `json:"health"`
}

func HealthCheck(w http.ResponseWriter, r *http.Request) {
	h := healthResponse{
		Health: "ok",
	}
	if err := writeJson(w, http.StatusOK, h); err != nil {
		log.Printf("failed to encode response: %v", err.Error())
	}
}
