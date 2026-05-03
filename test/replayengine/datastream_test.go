package test

import (
	"embed"
	"encoding/gob"
	"net/http"
	"slices"
	"testing"
	"time"

	"cloud.google.com/go/civil"
	"github.com/stretchr/testify/suite"

	enginecommon "github.com/mcudby/mwat/components/replayengine/common"
	"github.com/mcudby/mwat/components/replayengine/db"
	"github.com/mcudby/mwat/test/replayengine/utils"
)

type datastreamTestSuite struct {
	suite.Suite

	database       *utils.TestTimescaleDB
	replayengine   *utils.ReplayEngine
	replayenginedb db.ReplayEngineDB
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

	s.replayenginedb = db.RequireNewDatabase(s.database.GetConnectionURI())
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

	var expectedSeries enginecommon.Series
	gob.NewDecoder(f).Decode(&expectedSeries)

	// Store series
	symbol := enginecommon.Symbol("QQQ")
	date, err := civil.ParseDate("2025-03-20")
	s.Require().NoError(err)

	err = s.replayenginedb.StoreSeries(ctx, expectedSeries, symbol, date)
	s.Require().NoError(err)

	// Verify series has been correctly stored
	series, err := s.replayenginedb.GetSeries(ctx, symbol, date)
	slices.Reverse(series)
	s.Require().Equal(expectedSeries, series)

	// Start replayengine
	cancel := s.replayengine.RequireStartReplayEngine(t)
	t.Cleanup(cancel)

	// Client connects
	n := len(expectedSeries)
	client, err := utils.NewTestClient(ctx, replayEngineWSURL)
	s.Require().NoError(err)

	// Subscribe to QQQ
	err = client.SubToSymbols([]string{"QQQ"})
	s.Require().NoError(err)

	time.Sleep(2 * time.Second)

	// Start simulation
	s.requireResumeSimulation()

	// actualSeries := make(enginecommon.Series, n)
	for i := 0; i < n; i++ {
		var bar any
		err := client.BlockingReceive(&bar)
		s.Require().NoError(err)
		// actualSeries[0] = bar
		s.T().Log(bar)
		s.T().Logf("%d / %d", i, n)
	}
}

// Test suite entry point
func TestDataStreamTestSuite(t *testing.T) {
	suite.Run(t, new(datastreamTestSuite))
}

// todo: common loc
func (s *datastreamTestSuite) requirePauseSimulation() {
	resp, err := http.Post(replayEnginePauseURL, "", nil)
	s.Require().NoError(err)
	resp.Body.Close()
	s.T().Log("replayengine paused.")
}

// todo: common loc
func (s *datastreamTestSuite) requireResumeSimulation() {
	resp, err := http.Post(replayEngineResumeURL, "", nil)
	s.Require().NoError(err)
	resp.Body.Close()

	s.T().Log("replayengine resumed.")
}
