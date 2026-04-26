package testing

import (
	"context"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/suite"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

const (
	POSTGRES_DB       = "postgres"
	POSTGRES_PASSWORD = "password"
)

type DatabaseSuite struct {
	suite.Suite

	DatabaseConnectionURI string
	Conn                  *pgxpool.Conn

	DatabaseURL string
	Container   testcontainers.DockerContainer
	TerminateDB func() error
}

func (s *DatabaseSuite) SetupSuite() {
	s.RequireStartTimescaleDB()
	s.RequireInitDatabaseConn()
}

func (s *DatabaseSuite) TearDownSuite() {
	if s.TerminateDB != nil {
		s.TerminateDB()
	}
}

// Starts the test timescale db
// Populates:
// - s.databaseURL
// - s.connectionURI
// - s.terminateDB
func (s *DatabaseSuite) RequireStartTimescaleDB() {
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
	s.DatabaseURL = url
	s.DatabaseConnectionURI = fmt.Sprintf("postgres://postgres:password@%s/postgres?sslmode=disable", url)

	s.TerminateDB = func() error {
		ctx, _ := context.WithTimeout(s.T().Context(), 20*time.Second)

		return timescaleC.Terminate(ctx)
	}
}

func (s *DatabaseSuite) RequireInitDatabaseConn() {
	pool, err := pgxpool.New(context.Background(), s.DatabaseConnectionURI)
	s.Require().NoErrorf(err, fmt.Sprintf("failed to initialise pgxpool.Pool: %s", err))

	conn, err := pool.Acquire(s.T().Context())
	s.Require().NoErrorf(err, fmt.Sprintf("failed to initialise pgxpool.Conn: %s", err))

	s.Conn = conn
}

func (s *DatabaseSuite) RequireClearDatabase() {
	s.T().Log("Clearing database")
	tablesToClear := []string{
		"bars_1sec",
		"hydration_state_1sec",
	}

	stmt := fmt.Sprintf("TRUNCATE TABLE %s;", strings.Join(tablesToClear, ", "))
	_, err := s.Conn.Exec(s.T().Context(), stmt)
	s.Require().NoError(err)
}
