package db

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/suite"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

type dbTestSuite struct {
	suite.Suite
	conn      *pgx.Conn
	container testcontainers.Container
}

func (suite *dbTestSuite) SetupSuite() {
	suite.setupTestDB()
}

func (suite *dbTestSuite) TearDownSuite() {
	err := suite.conn.Close(context.Background())
	if err != nil {
		suite.Assert().NoError(err, "failed to close suite conn: %#v", err)
	}
	err = suite.container.Terminate(context.Background())
	if err != nil {
		suite.Assert().NoError(err, "failed to terminate suite container: %#v", err)
	}
}

func (suite *dbTestSuite) ctxWithTimeout() context.Context {
	ctx, _ := context.WithTimeout(context.Background(), 3*time.Second)
	return ctx
}

func (suite *dbTestSuite) TestPingDB() {
	err := suite.conn.Ping(suite.ctxWithTimeout())

	suite.Assert().NoError(err, "failed to ping db: %#v", err)
}

func (suite *dbTestSuite) setupTestDB() {
	ctx := context.Background()

	// Start timescale db container
	os.Setenv("TESTCONTAINERS_RYUK_DISABLE", "true")
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
	suite.Assert().NoError(err)
	suite.container = timescaleC

	// Get endpoint
	endpoint, err := timescaleC.Endpoint(ctx, "")
	suite.Assert().NoError(err)
	url := fmt.Sprintf("postgres://postgres:password@%s/postgres?sslmode=disable", endpoint)

	// Connect
	conn, err := pgx.Connect(ctx, url)
	suite.Assert().NoError(err)
	suite.conn = conn
}

func (s *dbTestSuite) TestGetMigratons() {
	migrations, err := getMigrations()
	s.T().Logf("migrations len(%d): %#v\n", len(migrations), migrations)
	s.Assert().NoError(err)
	s.Assert().Len(migrations, 1)
}

func (s *dbTestSuite) TestSetupDB() {
	err := setupDB(s.conn)
	s.Assert().NoError(err)

}

func TestDBTestSuite(t *testing.T) {
	suite.Run(t, new(dbTestSuite))
}
