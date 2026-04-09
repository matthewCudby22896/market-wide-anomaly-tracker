package replayengine

import (
	"encoding/json"
	"net/http"
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

func (s *replayEngineServer) handleHydrate(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "this endpoint only accepts POST requests", http.StatusMethodNotAllowed)
	}
}

func returnSuccessWithMessage(w http.ResponseWriter, msg string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(map[string]string{"message" : msg})
}