package utils

import (
	"bytes"
	"encoding/json"
	"net/http"
	"testing"

	"github.com/mcudby/mwat/components/replayengine"
	"github.com/stretchr/testify/require"
)

const (
	replayEnginePauseURL    = "http://localhost:9120/simulation/pause"
	replayEngineResumeURL   = "http://localhost:9120/simulation/resume"
	replayEngineRestartURL  = "http://localhost:9120/simulation/restart"
	replayEngineSettingsURL = "http://localhost:9120/control/settings"
)

func RequirePauseSimulation(t *testing.T) {
	resp, err := http.Post(replayEnginePauseURL, "", nil)
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)
	t.Log("replayengine paused")
}

func RequireResumeSimulation(t *testing.T) {
	resp, err := http.Post(replayEngineResumeURL, "", nil)
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)
	t.Log("replayengine resumed")
}

func RequireRestartSimulation(t *testing.T) {
	resp, err := http.Post(replayEngineRestartURL, "", nil)
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)
	t.Log("replayengine resumed")
}

func RequireUpdateSettings(t *testing.T, req replayengine.ReplayEngineSettings) {
	body, err := json.Marshal(req)
	require.NoError(t, err)

	resp, err := http.Post(replayEngineSettingsURL, "application/json", bytes.NewBuffer(body))
	require.NoError(t, err)
	defer resp.Body.Close()

	require.Equal(t, http.StatusOK, resp.StatusCode)
}
