package utils

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	"github.com/mcudby/mwat/common"
	"github.com/stretchr/testify/require"
)

var PROJECT_ROOT = common.GetProjectRoot()

const (
	REPLAY_ENGINE_MAIN      = "components/cmd/replay_engine/main.go"
	REPLAY_ENGINE_BINARY    = "replayengine"
	REPLAY_ENGINE_HTTP_PORT = "9120"
)

// Compiles the replayengine binary and returns the path to its location
func RequireCompileBinary(t *testing.T) string {
	tmp := t.TempDir()

	t.Logf("created test directory `%s`", tmp)

	compileTarget := filepath.Join(PROJECT_ROOT, REPLAY_ENGINE_MAIN)
	dst := filepath.Join(tmp, REPLAY_ENGINE_BINARY)

	cmd := exec.Command("go", "build", "-o", dst, compileTarget)
	outBytes, err := cmd.CombinedOutput()
	require.NoErrorf(
		t,
		err,
		fmt.Sprintf("compilation failed: \n %s", string(outBytes)),
	)

	t.Logf("succesfully compiled replayengine")

	return dst
}

func RequireStartReplayEngine(
	t *testing.T,
	pathToBinary string,
	databaseURL string,
	postgresPassword string,
	postgresDB string,
) {
	ctx, cancel := context.WithCancel(t.Context())

	cmd := exec.CommandContext(
		ctx,
		pathToBinary,
		"-db-url",
		databaseURL,
		"-port",
		REPLAY_ENGINE_HTTP_PORT,
	)

	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	// Environment Vars
	env := os.Environ()
	env = append(env, fmt.Sprintf("POSTGRES_PASSWORD=%s", postgresPassword))
	env = append(env, fmt.Sprintf("POSTGRES_DB=%s", postgresDB))
	cmd.Env = env

	cmd.Cancel = func() error {
		// akin to Ctrl + c
		return cmd.Process.Signal(os.Interrupt)
	}

	err := cmd.Start() // non-blocking
	require.NoError(t, err, "failed to start replayengine")

	go func() {
		err := cmd.Wait()
		select {
		case <-ctx.Done():
			t.Logf("replayengine shutdown on interrupt signal: %s", err)
		default:
			if err != nil {
				t.Logf("replayengine crashed: %s", err)
			}
		}
	}()

	t.Cleanup(cancel)
}
