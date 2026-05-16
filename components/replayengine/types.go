package replayengine

import (
	"context"
	"cloud.google.com/go/civil"

	"github.com/mcudby/mwat/components/replayengine/common"
	"github.com/mcudby/mwat/components/replayengine/wsclient"
)

type Hub interface {
	Start()
	Shutdown()
	GetID() string
	RegisterClient(c *wsclient.WSClient)
	PauseSimulation()
	ResumeSimulation()
	RestartSimulation()
	HydrateSymbol(ctx context.Context, symbol string, date civil.Date) error
	GetSimulationSettings() common.SimulationConfig
	SetSimulationSettings(newSettings common.SimulationConfig)
	GetInbox() chan<- any
}
