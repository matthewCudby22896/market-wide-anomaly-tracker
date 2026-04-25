package replayengine

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"time"

	"cloud.google.com/go/civil"
	"github.com/mcudby/mwat/components/replayengine/common"
)

type settingsPayload struct {
	Timescale      float32 `json:"timescale"`
	SimulationDate string  `json:"simulation-date"`
}

type hydrationRequest struct {
	Symbol string `json:"symbol"`
	Date   string `json:"date"`
}

func (s *replayEngineServer) handlePause(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "this endpoint only accepts POST requests", http.StatusMethodNotAllowed)
		return
	}

	s.Hub.PauseSimulation()

	s.logger.Info("simulation paused")
	successWithMsg(w, "simulation paused")
}

func (s *replayEngineServer) handleResume(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "this endpoint only accepts POST requests", http.StatusMethodNotAllowed)
		return
	}

	s.Hub.ResumeSimulation()

	s.logger.Info("simulation resumed")
	successWithMsg(w, "simulation resumed")
}

func (s *replayEngineServer) handleRestart(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "this endpoint only accepts POST requests", http.StatusMethodNotAllowed)
		return
	}

	s.Hub.RestartSimulation()

	s.logger.Info("simulation restarted")
	successWithMsg(w, "simulation restart succesful")
}

func (s *replayEngineServer) handleSettings(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodPost:
		payload := settingsPayload{}
		err := Decode(r.Body, &payload)
		if err != nil {
			err := fmt.Errorf("failed to marshal request body: %w", err)
			s.logger.Info("failed to update settings", "error", err)
			errorWithMsg(w, err.Error(), http.StatusBadRequest)
			return
		}

		settings, err := validateSettings(payload)
		if err != nil {
			err := fmt.Errorf("provided settings are invalid: %s", err)
			s.logger.Info("failed to update settings", "error", err)
			errorWithMsg(w, err.Error(), http.StatusBadRequest)
			return

		}
		s.Hub.SetSimulationSettings(settings)

		s.logger.Info("settings updated", "simulation.date", settings.Date.String(), "simulation.timescale", settings.Timescale)
		successWithMsg(w, "settings successfully updated")

	case http.MethodGet:
		settings := s.Hub.GetSimulationSettings()

		payload := settingsPayload{
			Timescale:      settings.Timescale,
			SimulationDate: settings.Date.String(),
		}

		s.logger.Info("simulation settings retrieved")
		successWithPayload(w, payload)
		return

	default:
		http.Error(w, "this endpoint only accepts POST and GET requests", http.StatusMethodNotAllowed)
		return
	}
}

func validateSettings(payload settingsPayload) (*simulationSettings, error) {
	date, err := validateDateStr(payload.SimulationDate)
	if err != nil {
		return nil, err
	}

	timescale, err := validateTimeScale(payload.Timescale)
	if err != nil {
		return nil, err
	}

	settings := &simulationSettings{timescale, date}

	return settings, nil
}

func (s *replayEngineServer) handleHydrate(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if r.Method != http.MethodPost {
		errorWithMsg(w, "this endpoint only accepts POST requests", http.StatusMethodNotAllowed)
		return
	}

	body := hydrationRequest{}
	err := Decode(r.Body, &body)
	if err != nil {
		errorWithMsg(w, "request body was not in expected form", http.StatusBadRequest)
		return
	}

	symbol, date, err := validateHydrateRequest(body)
	if err != nil {
		errorWithMsg(w, fmt.Sprintf("bad request: %s", err), http.StatusBadRequest)
		return
	}

	err = s.Hub.HydrateSymbol(ctx, symbol, date)
	if err != nil {
		switch {
		case errors.Is(err, AlreadyHydratedErr):
			errorWithMsg(w, "symbol is already hydrated", http.StatusBadRequest)
		case errors.Is(err, AlreadyHydratingErr):
			errorWithMsg(w, "symbol is already currently hydrating", http.StatusBadRequest)
		default:
			errorWithMsg(w, "internal server error", http.StatusInternalServerError)
		}
		return
	}

	successWithMsg(w, fmt.Sprintf("%s-%s successfully hydrated", symbol, date.String()))
}

func validateHydrateRequest(req hydrationRequest) (common.Symbol, civil.Date, error) {
	date, err := validateDateStr(req.Date)
	if err != nil {
		return "", civil.Date{}, err
	}
	if req.Symbol == "" {
		return "", civil.Date{}, fmt.Errorf("missing/empty field `symbol`")
	}
	symbol := common.Symbol(req.Symbol)

	return symbol, date, nil
}

func validateDateStr(dateStr string) (civil.Date, error) {
	date, err := civil.ParseDate(dateStr)
	if err != nil {
		return civil.Date{}, fmt.Errorf("failed to parse `%s` as an RFC3339 date", date.String())
	}
	if !common.IsWeekday(date) {
		return civil.Date{}, fmt.Errorf("`%s` is not a weekday", date.String())
	}
	return date, nil
}

func validateTimeScale(timescale float32) (float32, error) {
	if timescale < MIN_TIMESCALE {
		return 0, fmt.Errorf("`%f` is below the minimum timescale of `%f`", timescale, MIN_TIMESCALE)
	}
	if timescale > MAX_TIMESCALE {
		return 0, fmt.Errorf("`%f` is above the maximum timescale of `%f`", timescale, MAX_TIMESCALE)
	}
	return timescale, nil
}

func Decode(src io.ReadCloser, dst any) error {
	decoder := json.NewDecoder(src)
	decoder.DisallowUnknownFields()
	return decoder.Decode(dst)
}

func errorWithMsg(w http.ResponseWriter, errMsg string, status int) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(map[string]string{"error": errMsg}); err != nil {
		http.Error(w, "Internal server error", http.StatusInternalServerError)
	}
}

func successWithMsg(w http.ResponseWriter, msg string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	if err := json.NewEncoder(w).Encode(map[string]string{"msg": msg}); err != nil {
		http.Error(w, "Internal server error", http.StatusInternalServerError)
	}
}

func successWithPayload(w http.ResponseWriter, payload any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	if err := json.NewEncoder(w).Encode(payload); err != nil {
		http.Error(w, "Internal server error", http.StatusInternalServerError)
	}
}
