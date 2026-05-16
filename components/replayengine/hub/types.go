package hub

import (
	"context"

	"cloud.google.com/go/civil"
)

type HydrationMgr interface {
	Start()
	Shutdown()
	GetID() string
	IsReady(t string, d civil.Date) bool
	HydrateSymbol(ctx context.Context, symbol string, date civil.Date) (err error)
}

type SymbolThread interface {
	Start()
	Shutdown()
	AsyncShutdown()
	SetOutbox(outbox chan<- any)
	Restart()
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
