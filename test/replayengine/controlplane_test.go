package test

import (
	"bytes"
	"encoding/json"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/suite"

	"github.com/mcudby/mwat/components/replayengine"
	"github.com/mcudby/mwat/test/replayengine/utils"
)

const (
	postgresDB       = "postgres"
	postgresPassword = "password"

	replayEngineWSURL       = "ws://localhost:9120/ws"
	replayEnginePauseURL    = "http://localhost:9120/simulation/pause"
	replayEngineResumeURL   = "http://localhost:9120/simulation/resume"
	replayEngineRestartURL  = "http://localhost:9120/simulation/restart"
	replayEngineSettingsURL = "http://localhost:9120/control/settings"
)

type controlPlaneTestSuite struct {
	suite.Suite

	database     *utils.TestTimescaleDB
	replayengine *utils.ReplayEngine
}

func (s *controlPlaneTestSuite) SetupSuite() {
	t := s.T()
	s.database = utils.RequireStartTimescaleDB(
		t,
		postgresPassword,
		postgresDB,
	)

	cleanup := func() {
		err := s.database.Cancel()
		if err != nil {
			t.Log(err)
		}
	}

	t.Cleanup(cleanup)

	s.replayengine = utils.RequireInitReplayEngine(
		t,
		s.database.GetContainerEndpoint(),
		postgresPassword,
		postgresDB,
	)
}

func (s *controlPlaneTestSuite) requireReceiveTick(c *utils.TestClient) time.Time {
	tick := struct {
		Tick string `json:"tick"`
	}{}
	err := c.BlockingReceive(&tick)
	s.Require().NoError(err)

	t, err := time.Parse(time.RFC3339, tick.Tick)
	s.Require().NoError(err)

	return t
}

func (s *controlPlaneTestSuite) TestTimestreamSubscription() {
	t := s.T()
	ctx := t.Context()

	// GIVEN the replayengine is running
	cancel := s.replayengine.RequireStartReplayEngine(t)
	t.Cleanup(cancel)

	// AND we have a connected client
	client, err := utils.NewTestClient(ctx, replayEngineWSURL)
	s.Require().NoError(err)

	// AND the client is subbed to the timestream
	client.SubToTimestream()
	utils.RequireResumeSimulation(t)

	// AND the client waits to read 10 ticks
	firstTick := s.requireReceiveTick(client)
	s.T().Log(firstTick)
	for i := 0; i < 8; i++ {
		tick := s.requireReceiveTick(client)
		s.T().Log(tick)
	}
	lastTick := s.requireReceiveTick(client)
	s.T().Log(lastTick)

	// THEN the difference between the first and the last tick is 9 seconds
	s.Require().True(lastTick.After(firstTick))
}

func (s *controlPlaneTestSuite) TestPauseAndResume() {
	t := s.T()
	ctx := t.Context()
	n := 100

	// GIVEN the replay engine is running
	cancel := s.replayengine.RequireStartReplayEngine(t)
	t.Cleanup(cancel)

	// AND a client is connected & subbed to the timestream
	client, err := utils.NewTestClient(ctx, replayEngineWSURL)
	s.Require().NoError(err)
	client.SubToTimestream()

	// AND a pause command is issued
	utils.RequirePauseSimulation(t)

	paused := false
	prev := s.requireReceiveTick(client)
	for i := 0; i < n; i++ {
		curr := s.requireReceiveTick(client)

		s.T().Logf("\ncurr: %s\nprev: %s", curr, prev)

		if time.Time.Equal(curr, prev) {
			paused = true
			break
		}
		prev = curr
	}

	// THEN the simulation pauses within n ticks
	s.Require().Truef(paused, "simulation failed to pause in %d ticks", n)

	// GIVEN a resume command is issued
	utils.RequireResumeSimulation(t)
	prev = s.requireReceiveTick(client)
	for i := 0; i < n; i++ {
		curr := s.requireReceiveTick(client)

		s.T().Logf("\ncurr: %s\nprev: %s", curr, prev)

		if curr.Sub(prev) == time.Second {
			paused = false
			break
		}
		prev = curr
	}

	// THEN the simulation resumes within n ticks
	s.Require().Falsef(paused, "simulation failed to resume in %d ticks", n)

	// GIVEN a pause command is issued
	utils.RequirePauseSimulation(t)
	prev = s.requireReceiveTick(client)
	for i := 0; i < n; i++ {
		curr := s.requireReceiveTick(client)

		s.T().Logf("\ncurr: %s\nprev: %s", curr, prev)

		if time.Time.Equal(curr, prev) {
			paused = true
			break
		}
		prev = curr
	}

	// THEN the simulation pauses within n ticks
	s.Require().Truef(paused, "simulation failed to pause in %d ticks", n)
}

func (s *controlPlaneTestSuite) TestRestart() {
	t := s.T()
	ctx := t.Context()

	// GIVEN the replayengine is running & the simulation is un-paused
	// and a client is connected
	cancel := s.replayengine.RequireStartReplayEngine(t)
	t.Cleanup(cancel)
	client, err := utils.NewTestClient(ctx, replayEngineWSURL)
	client.SubToTimestream()
	s.Require().NoError(err)
	utils.RequireResumeSimulation(t)

	// Let several ticks pass
	s.requireReceiveTick(client)
	s.requireReceiveTick(client)
	s.requireReceiveTick(client)

	// AND the simulation is then paused & restarted
	utils.RequirePauseSimulation(t)
	utils.RequireRestartSimulation(t)

	restarted := false
	n := 100
	for i := 0; i < n; i++ {
		tick := s.requireReceiveTick(client)

		h, m, _ := tick.Clock()
		if h == 9 && m == 30 {
			// It is 9:30 AM
			restarted = true
			break
		}
	}
	// THEN the simulation eventually restarts
	s.Require().Truef(restarted, "simulation failed to restart in %d ticks", n)
}

func (s *controlPlaneTestSuite) TestGetAndUpdateSettings() {
	t := s.T()

	// GIVEN the replayengine is running
	cancel := s.replayengine.RequireStartReplayEngine(t)
	t.Cleanup(cancel)

	expectedInitial := replayengine.ReplayEngineSettings{
		Timescale:      replayengine.DefaultTimescale,
		SimulationDate: replayengine.DefaultDay.String(),
	}

	// AND a Get Settings request is made
	resp, err := http.Get(replayEngineSettingsURL)
	s.Require().NoError(err)
	defer resp.Body.Close()

	var dst replayengine.ReplayEngineSettings
	err = json.NewDecoder(resp.Body).Decode(&dst)
	s.Require().NoError(err)
	s.Equal(expectedInitial, dst)

	// GIVEN an Update settings request is made
	updatedSettings := replayengine.ReplayEngineSettings{
		Timescale:      10.0,
		SimulationDate: "2026-01-01",
	}
	body, err := json.Marshal(updatedSettings)
	s.Require().NoError(err)

	// THEN no error occurs (Performing the update)
	updateResp, err := http.Post(replayEngineSettingsURL, "application/json", bytes.NewBuffer(body))
	s.Require().NoError(err)
	defer updateResp.Body.Close()
	s.Equal(http.StatusOK, updateResp.StatusCode)

	// GIVEN a subsequent Get settings request is made
	finalResp, err := http.Get(replayEngineSettingsURL)
	s.Require().NoError(err)
	defer finalResp.Body.Close()

	// THEN the retrieved settings match those sent in the Update request
	var finalSettings replayengine.ReplayEngineSettings
	err = json.NewDecoder(finalResp.Body).Decode(&finalSettings)
	s.Require().NoError(err)
	s.Equal(updatedSettings, finalSettings)
}

// Test suite entry point
func TestConrolPlane(t *testing.T) {
	suite.Run(t, new(controlPlaneTestSuite))
}
