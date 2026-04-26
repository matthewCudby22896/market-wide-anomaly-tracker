package test

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/mcudby/mwat/common"
	"github.com/stretchr/testify/suite"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
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

	// replayengine
	binaryPath string

	// database
	databaseURL           string
	databaseConnectionURI string
	dbContainer           testcontainers.DockerContainer
	terminateDB           func() error
	conn                  *pgxpool.Conn
}

func (s *controlPlaneTestSuite) SetupSuite() {
	s.requireCompileBinary()
	s.requireStartTimescaleDB()
	s.requiredInitDatabaseConn()
}

func (s *controlPlaneTestSuite) TearDownSuite() {
	if s.terminateDB != nil {
		err := s.terminateDB()
		if err != nil {
			s.NoError(err)
		}
	}
}

func (s *controlPlaneTestSuite) SetupTest() {

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

// Starts the test timescale db
// Populates:
// - s.databaseURL
// - s.connectionURI
// - s.terminateDB
func (s *controlPlaneTestSuite) requireStartTimescaleDB() {
	ctx := s.T().Context()

	// Start timescale db container
	os.Setenv("TESTCONTAINERS_RYUK_DISABLED", "true")
	timescaleC, err := testcontainers.Run(
		ctx,
		"timescale/timescaledb-ha:pg18",
		testcontainers.WithExposedPorts("5432/tcp"),
		testcontainers.WithEnv(map[string]string{
			"POSTGRES_PASSWORD": POSTGRES_PASSWORD,
			"POSTGRES_DB":       POSTGRES_DB,
		}),
		testcontainers.WithWaitStrategy(
			wait.ForListeningPort("5432/tcp"),
		),
	)
	s.Require().NoError(err)

	// Get url
	url, err := timescaleC.Endpoint(ctx, "")
	s.Assert().NoError(err)

	s.T().Logf("timescaleDB url: %s", url)
	// todo: get database connection
	// Populate suite
	s.databaseURL = url
	s.databaseConnectionURI = fmt.Sprintf("postgres://postgres:password@%s/postgres?sslmode=disable", url)

	s.terminateDB = func() error {
		ctx, _ := context.WithTimeout(s.T().Context(), 20*time.Second)

		return timescaleC.Terminate(ctx)
	}
}

// Populates
// - s.conn
func (s *controlPlaneTestSuite) requiredInitDatabaseConn() {
	pool, err := pgxpool.New(context.Background(), s.databaseConnectionURI)
	s.Require().NoErrorf(err, fmt.Sprintf("failed to initialise pgxpool.Pool: %s", err))

	conn, err := pool.Acquire(s.T().Context())
	s.Require().NoErrorf(err, fmt.Sprintf("failed to initialise pgxpool.Conn: %s", err))

	s.conn = conn
}

func (s *controlPlaneTestSuite) requireStartReplayEngine() context.CancelFunc {
	s.Require().NotEmptyf(s.databaseURL, "dbURL is not set")

	ctx, cancel := context.WithCancel(s.T().Context())

	cmd := exec.CommandContext(ctx, s.binaryPath, "-db-url", s.databaseURL, "-port", REPLAY_ENGINE_HTTP_PORT)

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

func (s *controlPlaneTestSuite) requireClearDatabase() {
	tablesToClear := []string{
		"bars_1sec",
		"hydration_state_1sec",
	}

	for _, table := range tablesToClear {
		stmt := fmt.Sprintf("TRUNCATE TABLE %s;", strings.Join(tablesToClear, ", "))

		_, err := s.conn.Exec(s.T().Context(), stmt)

		s.Require().NoErrorf(err, "Failed to truncate table `%s`: %s", table, err)
	}
}

func (s *controlPlaneTestSuite) TestStartAndStop() {
	cancel := s.requireStartReplayEngine()
	defer cancel()

	s.requireClearDatabase()
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

	subTimestream := struct {
		Action  string   `json:"action"`
		Symbols []string `json:"symbols"`
	}{
		Action:  "subscribe",
		Symbols: []string{"TIMESTREAM"},
	}
	// AND they send a timestream subscription request
	client.Send(subTimestream)

	// THEN the client receives ticks
	firstTick := s.requireReceiveTick(client)
	s.T().Log(firstTick)
	for i := 0; i < 8; i++ {
		tick := s.requireReceiveTick(client)
		s.T().Log(tick)
	}
	lastTick := s.requireReceiveTick(client)
	s.T().Log(lastTick)

	diff := lastTick.Sub(firstTick)
	_ = diff

	s.T().Log(diff)

	s.Require().Equal(9*time.Second, diff)
}

func (s *controlPlaneTestSuite) TestPause() {

}

// Test suite entry point
func TestDBTestSuite(t *testing.T) {
	suite.Run(t, new(controlPlaneTestSuite))
}
