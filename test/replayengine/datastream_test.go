package test

import (
	"embed"
	"encoding/gob"
	"testing"
	"time"

	"cloud.google.com/go/civil"
	"github.com/stretchr/testify/suite"

	"github.com/mcudby/mwat/components/replayengine/api"
	"github.com/mcudby/mwat/components/replayengine/common"
	"github.com/mcudby/mwat/components/replayengine/db"
	"github.com/mcudby/mwat/test/replayengine/utils"
)

type datastreamTestSuite struct {
	suite.Suite

	database       *utils.TestTimescaleDB
	replayengine   *utils.ReplayEngine
	replayenginedb *db.ReplayEngineDB
}

func (s *datastreamTestSuite) SetupSuite() {
	t := s.T()
	s.database = utils.RequireStartTimescaleDB(
		t,
		postgresPassword,
		postgresDB,
	)

	cleanup := func() {
		err := s.database.Cancel()
		if err != nil {
			t.Log(err)
		}
	}

	t.Cleanup(cleanup)

	s.replayenginedb = db.EstablishDBConnection(s.T().Context(), s.database.GetConnectionURI())
	s.replayenginedb.ApplyMigrations()

	s.replayengine = utils.RequireInitReplayEngine(
		t,
		s.database.GetContainerEndpoint(),
		postgresPassword,
		postgresDB,
	)
}

//go:embed data/*.gob
var testData embed.FS

func (s *datastreamTestSuite) TestEntireSeriesIsStreamedOut() {
	t := s.T()
	ctx := t.Context()

	// Load series
	f, err := testData.Open("data/2025-03-20_QQQ.gob")
	s.Require().NoError(err)
	defer f.Close()

	var expectedSeries []common.Bar
	gob.NewDecoder(f).Decode(&expectedSeries)

	// GIVEN the series for QQQ on 2025-03-20 is stored in
	// the replayengine's db
	symbol := "QQQ"
	dateStr := "2025-03-20"
	civilDate, _ := civil.ParseDate(dateStr)

	err = s.replayenginedb.InsertFullSession(ctx, expectedSeries, symbol, dateStr)
	s.Require().NoError(err)

	// verify series has been correctly stored
	series, err := s.replayenginedb.GetFullSession(ctx, symbol, civilDate, db.T1s)
	s.Require().Equal(expectedSeries, series)

	// AND the replayengine is running
	cancel := s.replayengine.RequireStartReplayEngine(t)
	t.Cleanup(cancel)

	// AND the simulation settings are set to the correct day with a high timescale
	utils.RequireUpdateSettings(
		t,
		api.ConfigMessage{
			Timescale:      3600.0,
			SimulationDate: "2025-03-20",
		},
	)

	// AND we have a connected client
	n := len(expectedSeries)
	client, err := utils.NewTestClient(ctx, replayEngineWSURL)
	s.Require().NoError(err)

	// AND the client subscribes to A.QQQ
	err = client.SubToSymbols([]string{"A.QQQ"})
	s.Require().NoError(err)

	time.Sleep(2 * time.Second)

	// WHEN the simulation is started
	utils.RequireResumeSimulation(t)

	// THEN the replayengine streams out every datapoint
	// in chronological order
	actualSeries := make([]common.Bar, 0, n)
	var bar common.Bar
	for i := range n {
		err := client.BlockingReceive(&bar)
		s.Require().NoError(err)

		actualSeries = append(actualSeries, bar)

		// AND each streamed out bar is identical to the expected
		s.Require().Equal(expectedSeries[i], actualSeries[i], "i=%d", i)
	}

	// AND all bars are streamed out
	s.Require().Equal(len(expectedSeries), len(actualSeries))
}

// Test suite entry point
func TestDataStreamTestSuite(t *testing.T) {
	suite.Run(t, new(datastreamTestSuite))
}
