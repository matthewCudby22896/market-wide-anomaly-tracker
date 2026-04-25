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
)

var PROJECT_ROOT = common.GetProjectRoot()

type controlPlaneTestSuite struct {
	suite.Suite

	binaryPath string
}

const binaryName = "replayengine_binary"

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

func (s *controlPlaneTestSuite) SetupSuite() {
	s.requireCompileBinary()
}

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

	time.Sleep(3 * time.Second)

	cancel()
}

func (s *controlPlaneTestSuite) SetupTest() {

}

func TestDBTestSuite(t *testing.T) {
	suite.Run(t, new(controlPlaneTestSuite))
}
