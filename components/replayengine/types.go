package replayengine

import (
	"context"

	"cloud.google.com/go/civil"
)

type broadcastMessage struct {
	Symbol string
	Data   any
}

// WS Interface

type SubscriptionRequest struct {
	Action  string   `json:"action"`  // "sub" or "unsub"
	Symbols []string `json:"symbols"` // e.g., ["QQQ", "SPY"]
}

type Tick struct {
	Tick string `json:"tick"`
}

// Control Plane

type ConfigMessage struct {
	Timescale      float32 `json:"timescale"`
	SimulationDate string  `json:"simulation-date"`
}

type HydrationRequest struct {
	Symbol string `json:"symbol"`
	Date   string `json:"date"`
}

// Hub / Client  Registration

// Component Interfaces

type LifeCycle interface {
	Start()
	Shutdown()
}

type Hub interface {
	LifeCycle

	GetID() string
	RegisterClient(c *client)
	PauseSimulation()
	ResumeSimulation()
	RestartSimulation()
	HydrateSymbol(ctx context.Context, symbol string, date civil.Date) error
	GetSimulationSettings() SimulationConfig
	SetSimulationSettings(newSettings SimulationConfig)
	GetInbox() chan<- any
}

type HydrationMgr interface {
	LifeCycle
	GetID() string
	IsReady(t string, d civil.Date) bool
	HydrateSymbol(ctx context.Context, symbol string, date civil.Date) (err error)
}

type Clock interface {
	Start()
	Shutdown()
	RegisterPipe(pipe chan<- int64)
	UnregisterPipe(pipe chan<- int64)
	Pause()
	Resume()
	Restart()
	IsPaused() bool
	GetSimulationTime() int64
}
