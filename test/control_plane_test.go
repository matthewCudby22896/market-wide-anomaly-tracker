package test

import (
	"testing"

	"github.com/stretchr/testify/suite"
)

type controlPlaneTestSuite struct {
	suite.Suite
}



func compileReplayEnginer() {

}

func (s *controlPlaneTestSuite) SetupSuite() {

}

func (s *controlPlaneTestSuite) SetupTest() {

}

func TestDBTestSuite(t *testing.T) {
	suite.Run(t, new(controlPlaneTestSuite))
}
