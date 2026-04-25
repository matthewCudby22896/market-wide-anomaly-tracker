package test

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/mcudby/mwat/common"
	"github.com/stretchr/testify/suite"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

var PROJECT_ROOT = common.GetProjectRoot()

const binaryName = "replayengine_binary"

type controlPlaneTestSuite struct {
	suite.Suite

	// replayengine
	binaryPath string

	// database
	dbURL       string
	dbContainer testcontainers.DockerContainer
	terminateDB func() error
}

func (s *controlPlaneTestSuite) SetupSuite() {
	s.requireCompileBinary()
	s.requireStartTimescaleDB()
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
	// todo: clear timescaledb tables before test starts

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
// - s.dbURL
// - s.terminateDB
func (s *controlPlaneTestSuite) requireStartTimescaleDB() {
	ctx, _ := context.WithCancel(s.T().Context())

	// Start timescale db container
	os.Setenv("TESTCONTAINERS_RYUK_DISABLED", "true")
	timescaleC, err := testcontainers.Run(
		ctx,
		"timescale/timescaledb-ha:pg18",
		testcontainers.WithExposedPorts("5432/tcp"),
		testcontainers.WithEnv(map[string]string{
			"POSTGRES_PASSWORD": "password",
			"POSTGRES_DB":       "postgres",
		}),
		testcontainers.WithWaitStrategy(
			wait.ForListeningPort("5432/tcp"),
		),
	)
	s.Require().NoError(err)

	// Get endpoint
	endpoint, err := timescaleC.Endpoint(ctx, "")
	s.Assert().NoError(err)

	s.T().Logf("timescaleDB endpoint: %s", endpoint)

	// Populate suite
	s.dbURL = fmt.Sprintf("postgres://postgres:password@%s/postgres?sslmode=disable", endpoint)

	s.terminateDB = func() error {
		ctx, _ := context.WithTimeout(s.T().Context(), 20*time.Second)

		return timescaleC.Terminate(ctx)
	}
}

// todo: be able to specify the port s.t. it can connect to the test db
func (s *controlPlaneTestSuite) requireStartReplayEngine() context.CancelFunc {
	ctx, cancel := context.WithCancel(s.T().Context())

	cmd := exec.CommandContext(ctx, s.binaryPath)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	err := cmd.Start()
	s.Require().NoErrorf(err, "Failed to start replayengine")

	cmd.Cancel = func() error {
		// akin to Ctrl + c
		return cmd.Process.Signal(os.Interrupt)
	}

	go func() {
		err := cmd.Wait()
		s.T().Logf("Server exited with error: %v", err)
	}()

	return cancel
}

func (s *controlPlaneTestSuite) TestStartAndStop() {
	cancel := s.requireStartReplayEngine()
	time.Sleep(1 * time.Second)
	cancel()
}

// Test suite entry point
func TestDBTestSuite(t *testing.T) {
	suite.Run(t, new(controlPlaneTestSuite))
}
