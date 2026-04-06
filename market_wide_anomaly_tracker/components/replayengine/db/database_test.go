package db

import (
	"context"
	"fmt"
	"os"
	"slices"
	"testing"
	"time"

	"cloud.google.com/go/civil"
	"github.com/matthewCudby22896/market_wide_anomaly_tracker/components/replayengine/common"

	"github.com/stretchr/testify/suite"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

type dbTestSuite struct {
	suite.Suite
	ctx       context.Context
	container testcontainers.Container
	db        *database
}

func (s *dbTestSuite) SetupSuite() {
	s.ctx = s.T().Context()

	url := s.setupTestDB()

	s.db = RequireNewDatabase(url)

	s.db.RequireApplyMigrations()
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

func (s *dbTestSuite) TestBatchStoreBars() {
	symbol := common.Symbol("RR")
	date := civil.Date{
		Year:  2026,
		Month: 3,
		Day:   31,
	}
	nBars := int64(4680)
	open := common.NYSEOpenUnixMilli(date)
	close := common.NYSECloseUnixMilli(date)
	diff := close - open
	delta := diff / nBars

	bars := make(common.Series, nBars)
	for i := range nBars {
		bars[i] = common.Bar{
			Symbol: symbol,
			T:      open + delta*i,
		}
	}
	slices.Reverse(bars)

	err := s.db.BatchStoreBars(context.Background(), bars, symbol, date)
	s.Require().NoError(err)

	retBars, err := s.db.GetCompleteTradingDay(s.ctx, symbol, date)

	s.Require().NoError(err)
	s.Require().Equal(bars, retBars, "the fetched bars were not equal to the input bars")
}

func (s *dbTestSuite) TestLoadHydrationState() {
	ctx := s.ctxWithTimeout()
	conn, err := s.db.getConn(ctx)
	s.Require().NoError(err)

	stmt := "INSERT INTO hydration_state_1sec (symbol, date) VALUES ($1, $2)"
	_, err = conn.Exec(ctx, stmt, "AAPL", "2026-03-20")
	s.Require().NoError(err)

	res, err := s.db.LoadHydrationState(ctx)
	s.Require().NoError(err)
	s.Assert().Len(res, 1)
	expected := common.HydrationStatusRow{
		Symbol: "AAPL",
		Date:   "2026-03-20",
	}
	s.Assert().Equal(expected, res[0])
}

func TestDBTestSuite(t *testing.T) {
	suite.Run(t, new(dbTestSuite))
}
