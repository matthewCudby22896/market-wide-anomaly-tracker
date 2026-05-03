package test

import (
	"testing"

	"github.com/stretchr/testify/suite"

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
		POSTGRES_PASSWORD,
		POSTGRES_DB,
	)

	cleanup := func() {
		err := s.database.Cancel()
		if err != nil {
			t.Log(err)
		}
	}

	t.Cleanup(cleanup)

	s.replayenginedb = db.RequireNewDatabase(s.database.GetConnectionURI())

	s.replayengine = utils.RequireInitReplayEngine(
		t,
		s.database.GetContainerEndpoint(),
		POSTGRES_PASSWORD,
		POSTGRES_DB,
	)
}



func (s *datastreamTestSuite) TestEntireSeriesIsStreamedOut() {
	// Load series
	
	// Store series

	// Start replayengine

	// Client connects

}

// Test suite entry point
func TestDataStreamTestSuite(t *testing.T) {
	suite.Run(t, new(controlPlaneTestSuite))
}
