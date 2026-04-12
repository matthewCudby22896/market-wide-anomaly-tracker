package replayengine

import (
	"context"
	"encoding/json"
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

func (s *replayEngineServer) handleSettings(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodPost:

	case http.MethodGet:

	default:
		http.Error(w, "this endpoint only accepts POST and GET requests", http.StatusMethodNotAllowed)
		return
	}
}

// TODO: Fix tags
type hydrationReqBody struct {
	Symbol string `json:"ticker"`
	Date   string `json:"date"`
}

func (s *replayEngineServer) handleHydrate(w http.ResponseWriter, r *http.Request) {
	ctx, _ := context.WithTimeout(context.Background(), 5*time.Second)
	if r.Method != http.MethodPost {
		http.Error(w, "this endpoint only accepts POST requests", http.StatusMethodNotAllowed)
	}

	body := hydrationReqBody{}
	err := json.NewDecoder(r.Body).Decode(&body)
	if err != nil {
		http.Error(w, "request body was not in expected form", http.StatusBadRequest)
	}

	symbol, date, err := validateHydrateRequest(body)
	if err != nil {
		http.Error(w, "request body was not in expected form", http.StatusBadRequest)
	}

	err = s.Hub.HydrateSymbol(ctx, symbol, date)

	if err != nil {
		switch _err := err.(type) {
		case AlreadyHydratedErr:
			http.Error(
				w,
				fmt.Errorf("symbol already hydrated", _err).Error(),
				http.StatusInternalServerError,
			)

		case AlreadyHydratingErr:
			http.Error(
				w,
				fmt.Errorf("symbol currently hydrating", _err).Error(),
				http.StatusInternalServerError,
			)

		default:
			http.Error(
				w,
				fmt.Errorf("An unexpected error occured: %w", _err).Error(),
				http.StatusInternalServerError,
			)
		}
	}
	returnSuccessWithMessage(w, fmt.Sprintf("%s-%s succesfully hydrated", symbol, date.String()))
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
