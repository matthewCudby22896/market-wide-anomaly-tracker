package db_test

import (
	"context"
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
	timescaleDB utils.TestTimescaleDB

	replayEngineDB *db.ReplayEngineDB
}

func (s *databaseTestSuite) SetupSuite() {
	t := s.T()
	s.timescaleDB = *utils.RequireStartTimescaleDB(
		t,
		PostgresPassword,
		PostgresDB,
	)

	cleanup := func() {
		err := s.timescaleDB.Cancel()
		if err != nil {
			t.Log(err)
		}
	}

	t.Cleanup(cleanup)
	s.replayEngineDB = db.EstablishDBConnection(s.T().Context(), s.timescaleDB.GetConnectionURI())
	s.replayEngineDB.RequireApplyMigrations()
}

func (s *databaseTestSuite) SetupTest() {
	s.timescaleDB.RequireClearDatabase()
}
func (s *databaseTestSuite) TestInsertFullSession() {
	ctx := s.T().Context()

	s.timescaleDB.RequireClearDatabase()

	// Setup
	civilDate, _ := civil.ParseDate(date)
	nBars := int64(100)
	open := common.NYSEOpenUnixMilli(civilDate)
	close := common.NYSECloseUnixMilli(civilDate)
	series := s.getDummySeries(symbol, open, close, nBars)

	// FUT
	err := s.replayEngineDB.InsertFullSession(ctx, series, symbol, date)
	s.Require().NoError(err)

	retBars, err := s.replayEngineDB.GetFullSession(ctx, symbol, civilDate)

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

	res, err := s.replayEngineDB.GetHydrationStatus(ctx)
	s.Require().NoError(err)
	s.Assert().Len(res, 1)
	expected := common.HydrationStatusRow{
		Symbol: "AAPL",
		Date:   "2026-03-20",
	}
	s.Assert().Equal(expected, res[0])
}

func (s *databaseTestSuite) TestGetSeries() {
	ctx := s.T().Context()

	t1 := int64(0)
	t2 := int64(9999)
	n := int64(500)
	s.insertDummySeries(ctx, symbol, date, t1, t2, n)
	s.T().Run(
		"TestHappyGet",
		func(t *testing.T) {
			buffer := make([]common.Bar, 500)
			x, err := s.replayEngineDB.GetSeries(ctx, symbol, date, t1, t2, buffer)
			s.Require().NoError(err)
			s.Require().Equal(x, 500)
		},
	)
	s.T().Run(
		"TestErrorsWhenBufferTooSmall",
		func(t *testing.T) {

			buffer := make([]common.Bar, 499)
			x, err := s.replayEngineDB.GetSeries(ctx, symbol, date, t1, t2, buffer)
			s.Require().Error(err)
			s.Require().ErrorIs(err, db.BufferTooSmallErr)
			s.Require().Equal(x, 0)
		},
	)
}

// Inserts n dummy ohlc Bar evenly distributed across the timestamp range [t1, t2)
func (s *databaseTestSuite) insertDummySeries(ctx context.Context, symbol, date string, t1, t2, n int64) []common.Bar {
	series := s.getDummySeries(symbol, t1, t2, n)

	err := s.replayEngineDB.InsertFullSession(ctx, series, symbol, date)
	s.Require().NoError(err)

	return series
}

func (s *databaseTestSuite) getDummySeries(symbol string, t1, t2, n int64) []common.Bar {
	series := make([]common.Bar, n)

	delta := int64((t2 - t1) / n)
	for i := range n {
		series[i] = common.Bar{Symbol: symbol, T: t1 + int64(i)*delta}
	}

	return series
}

func TestDBTestSuite(t *testing.T) {
	suite.Run(t, new(databaseTestSuite))
}
