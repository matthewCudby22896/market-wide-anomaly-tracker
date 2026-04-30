package test

import (
	"context"
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
	binaryName = "replayengine_binary"

	REPLAY_ENGINE_HTTP_PORT = "9120"

	POSTGRES_DB       = "postgres"
	POSTGRES_PASSWORD = "password"

	replayEnginePingURL = "http://localhost:9120/ping"
)

type baseSuite struct {
	suite.Suite

	testutils.DatabaseSuite

	binaryPath string
}

func (s *baseSuite) SetupSuite() {
	s.DatabaseSuite.SetT(s.T())
	s.DatabaseSuite.SetupSuite()
	s.requireCompileBinary()
}

func (s *baseSuite) TearDownSuite() {
	s.DatabaseSuite.TearDownSuite()
}

func (s *baseSuite) requireCompileBinary() {
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

func (s *baseSuite) requireStartReplayEngine() context.CancelFunc {
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

func (s *baseSuite) TestStartReplayEngine() {
	cancel := s.requireStartReplayEngine()
	defer cancel()
}

func TestBaseSuite(t *testing.T) {
	suite.Run(t, new(baseSuite))
}
