package test

import (
	"bytes"
	"embed"
	"encoding/gob"
	"encoding/json"
	"io"
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

	// GIVEN the series for QQQ on 2025-03-20 is stored in
	// the replayengine's db
	symbol := enginecommon.Symbol("QQQ")
	date, err := civil.ParseDate("2025-03-20")
	s.Require().NoError(err)

	err = s.replayenginedb.StoreSeries(ctx, expectedSeries, symbol, date)
	s.Require().NoError(err)

	// Verify series has been correctly stored
	series, err := s.replayenginedb.GetSeries(ctx, symbol, date)
	slices.Reverse(series)
	s.Require().Equal(expectedSeries, series)

	// AND the replayengine is running
	cancel := s.replayengine.RequireStartReplayEngine(t)
	t.Cleanup(cancel)

	// AND the simulation settings are set to the correct day with a high timescale
	settings := Settings{
		Timescale:      3600.0,
		SimulationDate: "2025-03-20",
	}
	body, err := json.Marshal(settings)
	s.Require().NoError(err)

	resp, err := http.Post(replayEngineSettingsURL, "application/json", bytes.NewBuffer(body))
	s.Require().NoError(err)
	defer resp.Body.Close()
	s.T().Log(resp)

	resp, err = http.Get(replayEngineSettingsURL)
	s.Require().NoError(err)
	defer resp.Body.Close()
	s.T().Log(resp)
	body, err = io.ReadAll(resp.Body)
	s.Require().NoError(err)

	s.T().Logf("Response Body: %s", string(body))

	// AND we have a connected client
	n := len(expectedSeries)
	client, err := utils.NewTestClient(ctx, replayEngineWSURL)
	s.Require().NoError(err)

	// AND the client subscribes to QQQ
	err = client.SubToSymbols([]string{"QQQ"})
	s.Require().NoError(err)

	time.Sleep(2 * time.Second)

	// WHEN the simulation is started
	s.requireResumeSimulation()

	// THEN the replayengine streams out every datapoint
	// in chronological order
	actualSeries := make(enginecommon.Series, n)
	for i := 0; i < n; i++ {
		var bar enginecommon.Bar
		err := client.BlockingReceive(&bar)
		s.Require().NoError(err)
		actualSeries[i] = bar
		s.T().Log(bar)
		s.T().Logf("%d / %d", i+1, n)
	}

	// AND the streamed out data is identical to the data
	// stored within the db
	s.Require().Equal(expectedSeries, actualSeries)
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
