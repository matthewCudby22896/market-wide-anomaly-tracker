package test

import (
	"github.com/stretchr/testify/suite"

	"github.com/mcudby/mwat/test/replayengine/utils"
)

const (
	POSTGRES_DB       = "postgres"
	POSTGRES_PASSWORD = "password"
)

type controlPlaneTestSuite struct {
	suite.Suite

	database utils.TestTimescaleDB
}

func (s *controlPlaneTestSuite) SetupSuite() {
	t := s.T()
	s.database := utils.RequireStartTimescaleDB(
		t,
		POSTGRES_PASSWORD,
		POSTGRES_DB,
	)
}
