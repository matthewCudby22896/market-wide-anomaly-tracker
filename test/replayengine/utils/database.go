package utils

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	// "github.com/jackc/pgx/v5/pgxpool"
	// "github.com/stretchr/testify/suite"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

type TestTimescaleDB struct {
	t             *testing.T
	connectionURI string
	cancel        func() error
	conn          *pgxpool.Conn
}

func RequireStartTimescaleDB(
	t *testing.T,
	postgresPassword string,
	postgresDB string,
) *TestTimescaleDB {
	ctx := t.Context()

	container, err := testcontainers.Run(
		ctx,
		"timescale/timescaledb-ha:pg18",
		testcontainers.WithExposedPorts("5432/tcp"),
		testcontainers.WithEnv(map[string]string{
			"POSTGRES_PASSWORD":            postgresPassword,
			"POSTGRES_DB":                  postgresDB,
			"TESTCONTAINERS_RYUK_DISABLED": "true",
		}),
		testcontainers.WithWaitStrategy(
			wait.ForListeningPort("5432/tcp"),
		),
	)
	require.NoError(t, err)

	url, err := container.Endpoint(ctx, "")
	require.NoError(t, err)

	connectionURI := fmt.Sprintf("postgres://postgres:password@%s/postgres?sslmode=disable", url)

	cancel := func() error {
		ctx, cancel := context.WithTimeout(ctx, 20*time.Second)
		defer cancel()

		return container.Terminate(ctx)
	}

	conn := requireGetDatabaseConn(t, connectionURI)

	return &TestTimescaleDB{
		t:             t,
		connectionURI: connectionURI,
		cancel:        cancel,
		conn:          conn,
	}
}

func requireGetDatabaseConn(t *testing.T, connectionURI string) *pgxpool.Conn {
	ctx := t.Context()

	pool, err := pgxpool.New(
		ctx,
		connectionURI,
	)
	require.NoError(t, err)

	conn, err := pool.Acquire(ctx)
	require.NoError(t, err)

	return conn
}

func (db *TestTimescaleDB) RequireClearDatabase() {
	ctx := db.t.Context()
	tablesToClear := []string{
		"bars_1sec",
		"hydration_state_1sec",
	}

	stmt := fmt.Sprintf("TRUNCATE TABLE %s;", strings.Join(tablesToClear, ", "))
	_, err := db.conn.Exec(ctx, stmt)

	require.NoError(db.t, err)
}
