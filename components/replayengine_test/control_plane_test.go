package test

import (
	"os"
	"testing"

	"github.com/stretchr/testify/suite"
)

type controlPlaneTestSuite struct {
	suite.Suite
}

func (s *controlPlaneTestSuite) compileBinary() string {
	// Create test directory
	os.MkdirTemp("", "replayengine_test")

	// Compile replay engine

	// Return path to go binary

}

func (s *controlPlaneTestSuite) SetupSuite() {
	s.T()

}

func (s *controlPlaneTestSuite) SetupTest() {

}

func TestDBTestSuite(t *testing.T) {
	suite.Run(t, new(controlPlaneTestSuite))
}
