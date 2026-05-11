package replayengine

import (
	"context"

	"cloud.google.com/go/civil"

	"github.com/mcudby/mwat/components/replayengine/common"
	"github.com/mcudby/mwat/components/replayengine/wsclient"
)

// WS Interface

type SubscriptionRequest struct {
	Action  string   `json:"action"`  // "sub" or "unsub"
	Symbols []string `json:"symbols"` // e.g., ["QQQ", "SPY"]
}

type ConfigMessage struct {
	Timescale      float32 `json:"timescale"`
	SimulationDate string  `json:"simulation-date"`
}

type HydrationRequest struct {
	Symbol string `json:"symbol"`
	Date   string `json:"date"`
}

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
