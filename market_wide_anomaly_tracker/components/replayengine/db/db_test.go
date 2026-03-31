package db

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"cloud.google.com/go/civil"
	"github.com/matthewCudby22896/market_wide_anomaly_tracker/components/replayengine/common"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/suite"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

type dbTestSuite struct {
	suite.Suite
	ctx       context.Context
	conn      *pgxpool.Conn
	container testcontainers.Container
	dbUrl     string
	db        *database
}

func (s *dbTestSuite) SetupSuite() {
	s.ctx = s.T().Context()

	// 1. Launch the containerised db, and set s.dbUrl
	url := s.setupTestDB()
	s.dbUrl = url

	// 2. Create the `database` using the url and getConn for convenience
	s.db = NewDatabase(s.dbUrl)
	conn, err := s.db.getConn(s.ctx)
	s.Require().NoError(err)
	s.conn = conn

	// 3. Apply migrations
	err = applyMigrations(s.conn)
	s.Require().NoError(err)

	// 4. Run tests...
}

func (suite *dbTestSuite) TearDownSuite() {
	err := suite.container.Terminate(context.Background())
	if err != nil {
		suite.Assert().NoError(err, "failed to terminate suite container: %#v", err)
	}
}

func (suite *dbTestSuite) ctxWithTimeout() context.Context {
	ctx, _ := context.WithTimeout(context.Background(), 3*time.Second)
	return ctx
}

func (suite *dbTestSuite) setupTestDB() string {
	ctx := context.Background()

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
	suite.Require().NoError(err)
	suite.container = timescaleC

	// Get endpoint
	endpoint, err := timescaleC.Endpoint(ctx, "")
	suite.Assert().NoError(err)
	url := fmt.Sprintf("postgres://postgres:password@%s/postgres?sslmode=disable", endpoint)
	return url
}

func (suite *dbTestSuite) TestPingDB() {
	err := suite.conn.Ping(suite.ctxWithTimeout())

	suite.Assert().NoError(err, "failed to ping db: %#v", err)
}

func (s *dbTestSuite) TestGetMigratons() {
	migrations, err := getMigrations()
	s.T().Logf("migrations len(%d): %#v\n", len(migrations), migrations)
	s.Assert().NoError(err)
	s.Assert().Len(migrations, 1)
}

func (s *dbTestSuite) TestBatchStoreBars() {
	symbol := "RR"
	day := civil.Date{
		Year:  2026,
		Month: 3,
		Day:   31,
	}
	nBars := int64(4680)
	open := common.GetMarketOpenUnixMilli(day)
	close := common.GetMarketCloseUnixMilli(day)
	diff := close - open
	delta := diff / nBars

	s.T().Logf("open: %d\n", open)
	s.T().Logf("close: %d\n", close)
	s.T().Logf("diff: %d\n", diff)
	s.T().Logf("delta: %d\n", delta)

	bars := make(common.AggBars, nBars)
	for i := range nBars {
		bars[i] = common.OHLC{
			Symbol: symbol,
			T:      open + delta*i,
		}
	}

	err := s.db.BatchStoreBars(context.Background(), bars)
	s.Require().NoError(err)

	retBars, err := s.db.GetCompleteTradingDay(s.ctx, day, symbol)
	s.Require().NoError(err)
	s.Require().NotEmpty(retBars)

	for _, bar := range retBars {
		s.T().Logf("%v\n", bar)
	}
}

func TestDBTestSuite(t *testing.T) {
	suite.Run(t, new(dbTestSuite))
}
