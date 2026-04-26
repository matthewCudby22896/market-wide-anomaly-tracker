package test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/mcudby/mwat/common"
	testutils "github.com/mcudby/mwat/common/testing"
	"github.com/stretchr/testify/suite"
)

var PROJECT_ROOT = common.GetProjectRoot()

const (
	replayEngineWSURL       = "ws://localhost:9120/ws"
	replayEnginePingURL     = "http://localhost:9120/ping"
	replayEnginePauseURL    = "http://localhost:9120/simulation/pause"
	replayEngineResumeURL   = "http://localhost:9120/simulation/resume"
	replayEngineRestartURL  = "http://localhost:9120/simulation/restart"
	replayEngineSettingsURL = "http://localhost:9120/control/settings"
	replayEngineHydrateURL  = "http://localhost:9120/control/hydrate"

	binaryName = "replayengine_binary"

	REPLAY_ENGINE_HTTP_PORT = "9120"

	POSTGRES_DB       = "postgres"
	POSTGRES_PASSWORD = "password"
)

type controlPlaneTestSuite struct {
	suite.Suite
	testutils.DatabaseSuite

	binaryPath string
}

func (s *controlPlaneTestSuite) SetupSuite() {
	s.DatabaseSuite.SetT(s.T())
	s.DatabaseSuite.SetupSuite()

	s.requireCompileBinary()
}

func (s *controlPlaneTestSuite) AfterTest() {
	s.RequireClearDatabase()
}

func (s *controlPlaneTestSuite) requireCompileBinary() {
	// Create test directory
	dir := s.T().TempDir()
	s.T().Logf("Created test directory `%s`", dir)

	// Compile replay engine
	compileTarget := filepath.Join(PROJECT_ROOT, "components/cmd/replay_engine/main.go")
	binaryPath := filepath.Join(dir, binaryName)

	cmd := exec.Command("go", "build", "-o", binaryPath, compileTarget)
	outBytes, err := cmd.CombinedOutput()
	s.Require().NoErrorf(
		err,
		fmt.Sprintf("Compilation failed, output: \n %s", string(outBytes)),
	)

	s.T().Logf("Succesfully compiled replayengine binary at `%s`", binaryPath)

	s.binaryPath = binaryPath
}

func (s *controlPlaneTestSuite) requireStartReplayEngine() context.CancelFunc {
	s.Require().NotEmptyf(s.DatabaseURL, "dbURL is not set")

	ctx, cancel := context.WithCancel(s.T().Context())

	cmd := exec.CommandContext(ctx, s.binaryPath, "-db-url", s.DatabaseURL, "-port", REPLAY_ENGINE_HTTP_PORT)

	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	// Environment Vars
	env := os.Environ()
	env = append(env, fmt.Sprintf("POSTGRES_PASSWORD=%s", POSTGRES_PASSWORD))
	env = append(env, fmt.Sprintf("POSTGRES_DB=%s", POSTGRES_DB))
	cmd.Env = env

	err := cmd.Start()

	s.Require().NoErrorf(err, "Failed to start replayengine")

	cmd.Cancel = func() error {
		// akin to Ctrl + c
		return cmd.Process.Signal(os.Interrupt)
	}

	go func() {
		err := cmd.Wait()
		select {
		case <-ctx.Done():
			s.T().Logf("Replay engine shutdown on os.Interrupt, err: %v", err)

		default:
			if err != nil {
				s.T().Errorf("Replay engine crashed unexpectedly: %v", err)
			}
		}
	}()

	s.T().Log("waiting for http server")

	ctx, cancel = context.WithTimeout(s.T().Context(), 5*time.Second)
	client := &http.Client{}
	for {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, replayEnginePingURL, nil)
		s.Require().NoError(err)

		resp, err := client.Do(req)
		if err == nil {
			resp.Body.Close()
			if resp.StatusCode == http.StatusNoContent {
				s.T().Log("replayengine is up")
				break
			}
		}

		select {
		case <-ctx.Done():
			s.T().Fatal("timed out waiting for http server to start")
		default:
			time.Sleep(250 * time.Millisecond)
		}
	}

	return cancel
}

func (s *controlPlaneTestSuite) TestStartReplayEngine() {
	cancel := s.requireStartReplayEngine()
	defer cancel()
}

func (s *controlPlaneTestSuite) requireReceiveTick(c *testClient) time.Time {
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
	// GIVEN the replay engine is running
	cancel := s.requireStartReplayEngine()
	defer cancel()

	// AND we have a connected client
	client, err := newTestClient(s.T().Context(), replayEngineWSURL)
	s.Require().NoError(err)

	// AND the client is subbed to the timestream
	client.SubToTimestream()

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
	diff := lastTick.Sub(firstTick)
	s.Require().Equal(9*time.Second, diff)
}

func (s *controlPlaneTestSuite) requirePauseSimulation() {
	resp, err := http.Post(replayEnginePauseURL, "", nil)
	s.Require().NoError(err)
	resp.Body.Close()
	s.T().Log("replayengine paused.")
}

func (s *controlPlaneTestSuite) requireResumeSimulation() {
	resp, err := http.Post(replayEngineResumeURL, "", nil)
	s.Require().NoError(err)
	resp.Body.Close()

	s.T().Log("replayengine resumed.")
}

func (s *controlPlaneTestSuite) requireRestartSimulation() {
	resp, err := http.Post(replayEngineRestartURL, "", nil)
	s.Require().NoError(err)
	resp.Body.Close()

	s.T().Log("replayengine resumed.")
}

func (s *controlPlaneTestSuite) TestPauseAndResume() {
	n := 100

	// GIVEN the replay engine is running
	cancel := s.requireStartReplayEngine()
	defer cancel()

	// AND a client is connected & subbed to the timestream
	client, err := newTestClient(s.T().Context(), replayEngineWSURL)
	s.Require().NoError(err)
	client.SubToTimestream()

	// AND a pause command is issued
	s.requirePauseSimulation()

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
	s.requireResumeSimulation()
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
	s.requirePauseSimulation()
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
	// GIVEN the replayengine is running & the simulation is un-paused
	// and a client is connected
	s.requireStartReplayEngine()
	client, err := newTestClient(s.T().Context(), replayEngineWSURL)
	client.SubToTimestream()
	s.Require().NoError(err)
	s.requireResumeSimulation()

	// Let several ticks pass
	s.requireReceiveTick(client)
	s.requireReceiveTick(client)
	s.requireReceiveTick(client)

	// AND the simulation is then paused & restarted
	s.requirePauseSimulation()
	s.requireRestartSimulation()

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

type Settings struct {
	Timescale      float32 `json:"timescale"`
	SimulationDate string  `json:"simulation-date"`
}

func (s *controlPlaneTestSuite) TestGetAndUpdateSettings() {
	// GIVEN the replayengine is running
	s.requireStartReplayEngine()

	// AND a Get settings request is made
	resp, err := http.Get(replayEngineSettingsURL)
	s.Require().NoError(err)
	defer resp.Body.Close()

	// THEN the retrieved settings are as expected
	expectedInitial := Settings{
		Timescale:      5.0,
		SimulationDate: "2025-03-20",
	}
	var dst Settings
	err = json.NewDecoder(resp.Body).Decode(&dst)
	s.Require().NoError(err)
	s.Equal(expectedInitial, dst)

	// GIVEN an Update settings request is made
	updatedSettings := Settings{
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
	var finalSettings Settings
	err = json.NewDecoder(finalResp.Body).Decode(&finalSettings)
	s.Require().NoError(err)
	s.Equal(updatedSettings, finalSettings)
}

// todo: basic test for Hydrate - this will be more complex
// as requires somehow mocking the MASSIVE.com API

// Test suite entry point
func TestDBTestSuite(t *testing.T) {
	suite.Run(t, new(controlPlaneTestSuite))
}
