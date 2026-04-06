package replayengine

import (
	"net/http"
)

func (s *replayEngineServer) handlePause(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "this endpoint only accepts POST requests", http.StatusMethodNotAllowed)
		return
	}

	s.Hub.PauseSimulation()

	w.WriteHeader(http.StatusOK)
}

func (s *replayEngineServer) handleResume(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "this endpoint only accepts POST requests", http.StatusMethodNotAllowed)
		return
	}

	s.Hub.ResumeSimulation()

	w.WriteHeader(http.StatusOK)
}

func (s *replayEngineServer) handleRestart(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "this endpoint only accepts POST requests", http.StatusMethodNotAllowed)
		return
	}
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
