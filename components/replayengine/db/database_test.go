package db

import (
	"context"
	"slices"

	"testing"

	"cloud.google.com/go/civil"
	testutils "github.com/mcudby/mwat/common/testing"
	"github.com/mcudby/mwat/components/replayengine/common"

	"github.com/stretchr/testify/suite"
	"github.com/testcontainers/testcontainers-go"
)

type databaseTestSuite struct {
	suite.Suite
	testutils.DatabaseSuite

	container testcontainers.Container
	db        *database
}

func (s *databaseTestSuite) SetupSuite() {
	s.DatabaseSuite.SetT(s.T())
	s.DatabaseSuite.SetupSuite()

	s.db = RequireNewDatabase(s.DatabaseConnectionURI)
	s.db.RequireApplyMigrations()
}

func (s *databaseTestSuite) TearDownSuite() {
	s.DatabaseSuite.TearDownSuite()
}

func (s *databaseTestSuite) TearDownTest() {
	s.RequireClearDatabase()
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

	err := s.db.BatchStoreBars(context.Background(), bars, symbol, date)
	s.Require().NoError(err)

	retBars, err := s.db.GetCompleteTradingDay(ctx, symbol, date)

	s.Require().NoError(err)
	s.Require().Equal(bars, retBars, "the fetched bars were not equal to the input bars")
}

func (s *databaseTestSuite) TestLoadHydrationState() {
	ctx := s.T().Context()
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
	suite.Run(t, new(databaseTestSuite))
}
