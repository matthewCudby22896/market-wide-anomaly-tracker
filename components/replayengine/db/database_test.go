package db_test

import (
	"context"
	"slices"
	"testing"

	"cloud.google.com/go/civil"

	"github.com/mcudby/mwat/components/replayengine/common"
	"github.com/mcudby/mwat/test/replayengine/utils"
	"github.com/mcudby/mwat/components/replayengine/db"
	"github.com/stretchr/testify/suite"
)

const (
	PostgresDB       = "postgres"
	PostgresPassword = "password"
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
func (s *databaseTestSuite) TestBatchStoreBars() {
	ctx := s.T().Context()
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

	err := s.replayEngineDB.StoreSeries(context.Background(), bars, symbol, date)
	s.Require().NoError(err)

	retBars, err := s.replayEngineDB.GetSeries(ctx, symbol, date)

	s.Require().NoError(err)
	s.Require().Equal(bars, retBars, "the fetched bars were not equal to the input bars")
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

func TestDBTestSuite(t *testing.T) {
	suite.Run(t, new(databaseTestSuite))
}
