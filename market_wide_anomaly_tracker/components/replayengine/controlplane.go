package replayengine

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"time"

	"cloud.google.com/go/civil"
	"github.com/matthewCudby22896/market_wide_anomaly_tracker/components/replayengine/common"
)

func (s *replayEngineServer) handlePause(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "this endpoint only accepts POST requests", http.StatusMethodNotAllowed)
		return
	}

	s.Hub.PauseSimulation()

	returnSuccessWithMessage(w, "simulation paused")
}

func (s *replayEngineServer) handleResume(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "this endpoint only accepts POST requests", http.StatusMethodNotAllowed)
		return
	}

	s.Hub.ResumeSimulation()

	returnSuccessWithMessage(w, "simulation resumed")
}

func (s *replayEngineServer) handleRestart(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "this endpoint only accepts POST requests", http.StatusMethodNotAllowed)
		return
	}

	s.Hub.RestartSimulation()

	returnSuccessWithMessage(w, "simulation restart succesful")
}

type settingsPayload struct {
	Speedup       float32 `json:"speedup"`
	SimulatedDate string  `json:"simulated_date"`
}

func (s *replayEngineServer) handleSettings(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodPost:
		payload := settingsPayload{}
		err := json.NewDecoder(r.Body).Decode(&payload)

		if err != nil {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusBadRequest)
			json.NewEncoder(w).Encode(map[string]string{"message": fmt.Sprintf("failed to decode request body: %s", err.Error())})
		}

		validSettings, err := valid

	case http.MethodGet:
		settings := s.Hub.GetSimulationSettings()

		payload := settingsPayload{
			Speedup:       settings.Speedup,
			SimulatedDate: settings.Date.String(),
		}

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(payload)
		return

	default:
		http.Error(w, "this endpoint only accepts POST and GET requests", http.StatusMethodNotAllowed)
		return
	}
}

// TODO: finish implementing
func validateSettings(payload settingsPayload) (simulationSettings, error) {
	speedup := payload.Speedup
	dateStr := payload.SimulatedDate

	date, err := civil.ParseDate(dateStr)
	if err != nil {
		return simulationSettings{}, fmt.Errorf("failed to parse date `%s` : %w", date, err)
	}
	return simulationSettings{speedup, date}, nil
}

type hydrationReqBody struct {
	Symbol string `json:"symbol"`
	Date   string `json:"date"`
}

func (s *replayEngineServer) handleHydrate(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if r.Method != http.MethodPost {
		http.Error(w, "this endpoint only accepts POST requests", http.StatusMethodNotAllowed)
	}

	body := hydrationReqBody{}
	err := json.NewDecoder(r.Body).Decode(&body)
	if err != nil {
		http.Error(w, "request body was not in expected form", http.StatusBadRequest)
		return
	}

	symbol, date, err := validateHydrateRequest(body)
	if err != nil {
		http.Error(w, fmt.Sprintf("bad request: %s", err), http.StatusBadRequest)
		return
	}

	err = s.Hub.HydrateSymbol(ctx, symbol, date)
	if err != nil {
		switch {
		case errors.Is(err, AlreadyHydratedErr):
			http.Error(w, "symbol is already hydrated", http.StatusBadRequest)
		case errors.Is(err, AlreadyHydratingErr):
			http.Error(w, "symbol is already currently hydrating", http.StatusBadRequest)
		default:
			http.Error(w, "internal server error", http.StatusInternalServerError)
		}
		return
	}

	returnSuccessWithMessage(w, fmt.Sprintf("%s-%s successfully hydrated", symbol, date.String()))
}

func validateHydrateRequest(req hydrationReqBody) (common.Symbol, civil.Date, error) {
	date, err := civil.ParseDate(req.Date)
	if err != nil {
		return "", civil.Date{}, fmt.Errorf("failed to parse date: %w", err)
	}
	if req.Symbol == "" {
		return "", civil.Date{}, fmt.Errorf("no symbol provided")
	}
	symbol := common.Symbol(req.Symbol)

	return symbol, date, nil
}

func returnSuccessWithMessage(w http.ResponseWriter, msg string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(map[string]string{"message": msg})
}
