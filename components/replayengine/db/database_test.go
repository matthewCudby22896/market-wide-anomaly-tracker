package db_test

import (
	"context"
	"slices"
	"testing"

	"cloud.google.com/go/civil"

	"github.com/mcudby/mwat/components/replayengine/common"
	"github.com/mcudby/mwat/components/replayengine/db"
	"github.com/mcudby/mwat/test/replayengine/utils"
	"github.com/stretchr/testify/suite"
)

const (
	PostgresDB       = "postgres"
	PostgresPassword = "password"
	symbol           = "RR"
	date             = "2026-03-31"
)

var (
	civilDate   = civil.Date{Year: 2026, Month: 3, Day: 1}
	openUnixTS  = common.NYSEOpenUnixMilli(civilDate)
	closeUnixTS = common.NYSECloseUnixMilli(civilDate)
)

type databaseTestSuite struct {
	suite.Suite
	database utils.TestTimescaleDB

	replayEngineDB *db.ReplayEngineDB
}

func (s *databaseTestSuite) SetupSuite() {
	t := s.T()
	s.database = *utils.RequireStartTimescaleDB(
		t,
		PostgresPassword,
		PostgresDB,
	)

	cleanup := func() {
		err := s.database.Cancel()
		if err != nil {
			t.Log(err)
		}
	}

	t.Cleanup(cleanup)

	s.replayEngineDB = db.RequireNewDatabase(s.database.GetConnectionURI())
	s.replayEngineDB.RequireApplyMigrations()
}

func (s *databaseTestSuite) SetupTest() {
	s.database.RequireClearDatabase()
}
func (s *databaseTestSuite) TestStoreCompleteSeries() {
	ctx := s.T().Context()

	// Setup
	civilDate, _ := civil.ParseDate(date)
	nBars := int64(4680)
	open := common.NYSEOpenUnixMilli(civilDate)
	close := common.NYSECloseUnixMilli(civilDate)
	series := s.insertDummySeries(ctx, symbol, date, open, close, nBars)

	// FUT
	err := s.replayEngineDB.StoreSeries(ctx, series, symbol, date)
	s.Require().NoError(err)

	retBars, err := s.replayEngineDB.GetSeries(ctx, symbol, civilDate)

	slices.Reverse(series) // Currently
	s.Require().NoError(err)
	s.Require().Equal(series, retBars, "the fetched bars were not equal to the input bars")
}

func (s *databaseTestSuite) TestLoadHydrationState() {
	ctx := s.T().Context()
	conn, err := s.replayEngineDB.GetConn(ctx)
	s.Require().NoError(err)

	stmt := "INSERT INTO hydration_state_1sec (symbol, date) VALUES ($1, $2)"
	_, err = conn.Exec(ctx, stmt, "AAPL", "2026-03-20")
	s.Require().NoError(err)

	res, err := s.replayEngineDB.LoadHydrationState(ctx)
	s.Require().NoError(err)
	s.Assert().Len(res, 1)
	expected := common.HydrationStatusRow{
		Symbol: "AAPL",
		Date:   "2026-03-20",
	}
	s.Assert().Equal(expected, res[0])
}

func (s *databaseTestSuite) TestGetSeriesSegment() {
	ctx := s.T().Context()
	symbol := common.Symbol("QQQ")

	s.insertDummySeries(ctx)

}

// Inserts n dummy ohlc Bar evenly distributed across the timestamp range [t1, t2)
func (s *databaseTestSuite) insertDummySeries(ctx context.Context, symbol, date string, t1, t2, n int64) []common.Bar {
	var series []common.Bar

	delta := int64((t2 - t1) / n)
	for i := range n {
		series[i] = common.Bar{T: int64(i) * delta}
	}

	s.replayEngineDB.StoreSeries(ctx, series, symbol, date)

	return series
}

func TestDBTestSuite(t *testing.T) {
	suite.Run(t, new(databaseTestSuite))
}
