package utils

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
	"github.com/stretchr/testify/require"
)

var PROJECT_ROOT = common.GetProjectRoot()

const (
	replayEngineMainPath = "components/cmd/replay_engine/main.go"
	replayEngineBinary   = "replayengine"
	replayEngineHTTPPort = "9120"
	replayEnginePingURL  = "http://localhost:9120/ping"
)

type ReplayEngine struct {
	t                *testing.T
	pathToBinary     string
	databaseURL      string
	postgresPassword string
	postgresDB       string
}

func RequireInitReplayEngine(
	t *testing.T,
	databaseURL string,
	postgresPassword string,
	postgresDB string,
) *ReplayEngine {
	path := requireCompileBinary(t)

	return &ReplayEngine{
		t:                t,
		pathToBinary:     path,
		databaseURL:      databaseURL,
		postgresPassword: postgresPassword,
		postgresDB:       postgresDB,
	}
}

// Compiles the replayengine binary and returns the path to its location
func requireCompileBinary(t *testing.T) string {
	tmp := t.TempDir()

	t.Logf("created test directory `%s`", tmp)

	compileTarget := filepath.Join(PROJECT_ROOT, replayEngineMainPath)
	dst := filepath.Join(tmp, replayEngineBinary)

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

func (b *ReplayEngine) RequireStartReplayEngine(t *testing.T) context.CancelFunc {
	ctx, cancel := context.WithCancel(b.t.Context())

	cmd := exec.CommandContext(
		ctx,
		b.pathToBinary,
		"-db-url",
		b.databaseURL,
		"-port",
		replayEngineHTTPPort,
	)

	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	// Environment Vars
	env := os.Environ()
	env = append(env, fmt.Sprintf("POSTGRES_PASSWORD=%s", b.postgresPassword))
	env = append(env, fmt.Sprintf("POSTGRES_DB=%s", b.postgresDB))
	cmd.Env = env

	cmd.Cancel = func() error {
		// akin to Ctrl + c
		return cmd.Process.Signal(os.Interrupt)
	}

	err := cmd.Start() // non-blocking
	require.NoError(b.t, err, "failed to start replayengine")

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

	requireWaitTillPingable(t)

	return cancel
}

func requireWaitTillPingable(t *testing.T) {
	require.Eventually(t,
		func() bool {
			ctx, cancel := context.WithTimeout(context.Background(), 250*time.Millisecond)
			defer cancel()

			req, err := http.NewRequestWithContext(ctx, http.MethodGet, replayEnginePingURL, nil)
			require.NoError(t, err)

			resp, err := http.DefaultClient.Do(req)
			if err != nil {
				return false
			}
			defer resp.Body.Close()

			return resp.StatusCode == http.StatusNoContent
		},
		5*time.Second,
		500*time.Millisecond,
		"replayengine failed to respond to ping",
	)
}
