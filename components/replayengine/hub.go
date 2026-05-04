package replayengine

import (
	"context"
	"fmt"
	"maps"
	"slices"
	"sync"

	"cloud.google.com/go/civil"
	"github.com/mcudby/mwat/components/replayengine/common"
	"github.com/mcudby/mwat/components/replayengine/db"
)

type HubReqType int

const (
	REGISTER HubReqType = iota
	UNREGISTER
	SUB
	UNSUB
)

const hubID = "hub"

type ClientRequest interface {
	GetSender() *client
}

type BaseRequest struct {
	Sender *client
}

func (r BaseRequest) GetSender() *client {
	return r.Sender
}

type registerRequest struct {
	BaseRequest
}

type unregisterRequest struct {
	BaseRequest
}

type subRequest struct {
	BaseRequest
	symbols []common.Symbol
}

type unsubRequest struct {
	BaseRequest
	symbols []common.Symbol
}

type Hub interface {
	LifeCycle

	GetID() string
	RegisterClient(c *client)
	PauseSimulation()
	ResumeSimulation()
	RestartSimulation()
	HydrateSymbol(ctx context.Context, symbol common.Symbol, date civil.Date) error
	GetSimulationSettings() simulationSettings
	SetSimulationSettings(newSettings *simulationSettings)
}

/*
Settings:

- Do I need a mutex?
- Getting is simple

Setting Settings:
- Should implicitly restart the simulation
*/

// hub implements the Hub interface
type hub struct {
	ID           string
	Ctx          context.Context
	CancelCtx    context.CancelFunc
	wg           sync.WaitGroup
	logger       *Logger
	settingsLock sync.Mutex
	settings     simulationSettings

	clientRequestInbox chan ClientRequest
	broadcastInbox     chan BroadcastMessage
	dataInfoInbox      chan any

	clients               map[*client]struct{}
	symbolToClient        map[common.Symbol]map[*client]struct{}
	symbolThreads         map[common.Symbol]*symbolThread
	clientToSubbedSymbols map[*client]map[common.Symbol]struct{}

	subbedToTimestream map[*client]struct{}
	timeStreamInbox    <-chan Tick

	DataCoordinator
	Clock
	Database *db.ReplayEngineDB
}

type simulationSettings struct {
	Timescale float32
	Date      civil.Date
}

func defaultSimulationSettings() simulationSettings {
	return simulationSettings{
		Timescale: DefaultTimescale,
		Date:      DefaultDay,
	}
}

func NewHub(database *db.ReplayEngineDB) *hub {
	ctx, cancel := context.WithCancel(context.Background())

	settings := defaultSimulationSettings()

	timeStreamChan := make(chan Tick, 1)

	// Init clock
	clock := NewClock(settings.Date, settings.Timescale)
	clock.timestreamOutbox = timeStreamChan

	h := &hub{
		ID:                 hubID,
		Ctx:                ctx,
		CancelCtx:          cancel,
		wg:                 sync.WaitGroup{},
		logger:             NewComponentLogger(hubID),
		settings:           settings,
		clientRequestInbox: make(chan ClientRequest, 1024),
		broadcastInbox:     make(chan BroadcastMessage, 1024),
		dataInfoInbox:      make(chan any, 1024),

		clients:               make(map[*client]struct{}),
		symbolToClient:        make(map[common.Symbol]map[*client]struct{}),
		clientToSubbedSymbols: make(map[*client]map[common.Symbol]struct{}),
		symbolThreads:         make(map[common.Symbol]*symbolThread),

		subbedToTimestream: make(map[*client]struct{}),
		timeStreamInbox:    timeStreamChan,

		DataCoordinator: NewDataCoordinator(database),
		Clock:           clock,
		Database:        database,
	}

	h.DataCoordinator.SetOutbox(h.dataInfoInbox)

	return h
}

func (h *hub) GetID() string { return h.ID }

func (h *hub) Shutdown() {
	// Shutdown all child components:
	// - clients
	// - symbol threads
	// - data controller

	for client := range h.clients {
		h.logger.LogStopChild(client.ID)
		client.Shutdown()
	}

	h.logger.LogStopChild(h.DataCoordinator.GetID())
	h.DataCoordinator.Shutdown()

	for _, symbolThread := range h.symbolThreads {
		h.logger.LogStopChild(symbolThread.ID)
		symbolThread.Shutdown()
	}

	// Shutdown self
	h.CancelCtx()
	h.wg.Wait()
	h.logger.LogShutdown()
}

func (h *hub) Start() {
	h.logger.LogStartChild(h.DataCoordinator.GetID())
	h.DataCoordinator.Start()
	h.Clock.Start()

	h.wg.Add(1)
	go func() {
		defer h.wg.Done()
		for {
			select {
			case <-h.Ctx.Done():
				return

			case msg := <-h.broadcastInbox:
				h.handleBroadcast(msg)

			case req := <-h.clientRequestInbox:
				switch v := req.(type) {
				case registerRequest:
					h.handleRegister(v)
				case unregisterRequest:
					h.handleUnregister(v)
				case subRequest:
					h.handleSub(v)
				case unsubRequest:
					h.handleUnsub(v)
				}

			case notification := <-h.dataInfoInbox:
				switch v := notification.(type) {
				case hydrationSuccess:
					h.logger.Info("hydration notification received", "notification", v)
					h.StartTickerThread(v.Symbol, v.Date)

				case hydrationFailure:
					h.logger.Info("hydration failure notification received", "notification", v)

					clients := h.symbolToClient[v.Symbol]
					for c := range clients {
						c.Outbox() <- struct{ msg string }{
							msg: fmt.Sprintf("hydration failed for symbol '%s'", v.Symbol),
						}
						delete(clients, c)
					}
					delete(h.symbolToClient, v.Symbol)

				default:
					h.logger.Fatal("unrecognised notification received", "notification", v)
				}
			case tick := <-h.timeStreamInbox:
				for c := range h.subbedToTimestream {
					c.outbox <- tick
				}
			}
		}
	}()
	h.logger.LogStart()
}

func (h *hub) handleBroadcast(msg BroadcastMessage) {
	for client := range h.symbolToClient[msg.Symbol] {
		client.Outbox() <- msg.Data
	}
}

func (h *hub) RegisterClient(c *client) {
	h.clientRequestInbox <- registerRequest{BaseRequest{c}}
}

func (h *hub) PauseSimulation() {
	h.Clock.Pause()
}

func (h *hub) ResumeSimulation() {
	h.Clock.Resume()
}

func (h *hub) RestartSimulation() {
	wasPaused := h.Clock.IsPaused()
	h.Clock.Pause()
	h.Clock.ResetState()

	h.restartSymbolThreads()

	if wasPaused && !h.Clock.IsPaused() {
		h.Clock.Pause()
	} else if !wasPaused && h.Clock.IsPaused() {
		h.Clock.Resume()
	}
}

func (h *hub) HydrateSymbol(ctx context.Context, symbol common.Symbol, date civil.Date) error {
	return h.DataCoordinator.HydrateSymbol(ctx, symbol, date)
}

func (h *hub) GetSimulationSettings() simulationSettings {
	h.settingsLock.Lock()
	defer h.settingsLock.Unlock()
	return h.settings
}

func (h *hub) SetSimulationSettings(settings *simulationSettings) {
	h.settingsLock.Lock()
	h.settings = *settings
	h.settingsLock.Unlock()

	wasPaused := h.Clock.IsPaused()
	h.Clock.Pause()
	h.Clock.UpdateClockSettings(settings.Timescale, settings.Date)
	h.Clock.ResetState()

	h.restartSymbolThreads()

	if wasPaused && !h.Clock.IsPaused() {
		h.Clock.Pause()
	} else if !wasPaused && h.Clock.IsPaused() {
		h.Clock.Resume()
	}
}

func (h *hub) restartSymbolThreads() {
	wg := sync.WaitGroup{}
	for _, symbolThread := range h.symbolThreads {
		wg.Add(1)
		go func() {
			defer wg.Done()
			h.logger.Info("restarting symbol thread", "symbol-thread-id", symbolThread.ID)
			symbolThread.Restart()
		}()
	}
	wg.Wait()
}

const TIMESTREAM = "TIMESTREAM"

func (h *hub) subToTimestream(c *client) {
	if _, ok := h.subbedToTimestream[c]; ok {
		h.logger.Info(
			"client already subscribed",
			"client-id", c.ID,
			"symbol", TIMESTREAM,
			"num-subscribed", len(h.subbedToTimestream),
		)
	} else {
		h.subbedToTimestream[c] = struct{}{}
		h.logger.Info(
			"client subscribed",
			"client-id", c.ID,
			"symbol", TIMESTREAM,
			"num-subscribed", len(h.subbedToTimestream),
		)
	}
}

func (h *hub) unsubToTimestream(c *client) {
	if _, ok := h.subbedToTimestream[c]; ok {
		delete(h.subbedToTimestream, c)
		h.logger.Info(
			"client unsubscribed",
			"client-id", c.ID,
			"symbol", TIMESTREAM,
			"num-subscribed", len(h.subbedToTimestream),
		)
	}
}

func (h *hub) handleSub(req subRequest) {
	c := req.Sender

	for _, symbol := range req.symbols {
		if symbol == TIMESTREAM {
			h.subToTimestream(c)
			continue
		}

		if _, ok := h.symbolThreads[symbol]; !ok {
			h.logger.Info("first subscriber, starting symbol thread", "client-id", c.ID, "symbol", symbol)

			if h.DataCoordinator.IsReady(symbol, h.settings.Date) {

				// If ready, notify main loop
				h.dataInfoInbox <- hydrationSuccess{
					Symbol: symbol,
					Date:   h.settings.Date,
				}
			}

			// If not, DataAvailabilityProvider will eventually signal that it is
		}

		if h.symbolToClient[symbol] == nil {
			h.symbolToClient[symbol] = make(map[*client]struct{})
		}

		if h.clientToSubbedSymbols[c] == nil {
			h.clientToSubbedSymbols[c] = make(map[common.Symbol]struct{})
		}

		if _, ok := h.symbolToClient[symbol][c]; ok {
			h.logger.Info(
				"client already subscribed",
				"client-id", c.ID,
				"symbol", symbol,
				"num-subscribed", len(h.symbolToClient[symbol]),
			)
			continue
		}

		h.symbolToClient[symbol][c] = struct{}{}
		h.clientToSubbedSymbols[c][symbol] = struct{}{}

		h.logger.Info(
			"client subscribed",
			"client-id", c.ID,
			"symbol", symbol,
			"num-subscribed", len(h.symbolToClient[symbol]),
		)
	}
}

func (h *hub) handleUnsub(req unsubRequest) {
	symbols := req.symbols
	c := req.Sender
	for _, symbol := range symbols {
		if symbol == TIMESTREAM {
			h.unsubToTimestream(c)
			continue
		}
		if clients, ok := h.symbolToClient[symbol]; ok {
			delete(clients, c)

			h.logger.Info(
				"client unsubscribed from symbol",
				"client-id", c.ID,
				"symbol", symbol,
				"num-subscribed", len(h.symbolToClient[symbol]),
			)

			if len(clients) == 0 {
				h.logger.Info("last subscriber unsubbed, killing symbol thread", "symbol", symbol)
				h.killSymbolThread(symbol)
				delete(h.symbolToClient, symbol)
			}
		}
	}
}

func (h *hub) handleRegister(req registerRequest) {
	c := req.Sender
	h.clients[c] = struct{}{}
	c.hubRequestOutbox = h.clientRequestInbox
	h.logger.Info("client registered to hub", "client-id", c.ID, "client-count", len(h.clients))
	h.logger.LogStartChild(c.ID)
	c.Start()
}

func (h *hub) handleUnregister(req unregisterRequest) {
	c := req.Sender
	if symbols, ok := h.clientToSubbedSymbols[c]; ok && len(symbols) > 0 {
		h.handleUnsub(
			unsubRequest{
				BaseRequest: BaseRequest{c},
				symbols:     slices.Collect(maps.Keys(symbols)),
			},
		)
	}

	delete(h.clients, c)
	h.logger.Info("client unregistered from hub", "client-id", c.ID, "client-count", len(h.clients))

}

func (h *hub) killSymbolThread(symbol common.Symbol) {
	thread := h.symbolThreads[symbol]
	h.logger.LogStopChild(thread.ID)
	thread.AsyncShutdown()
	delete(h.symbolThreads, symbol)
}

func (h *hub) StartTickerThread(symbol common.Symbol, date civil.Date) *symbolThread {
	h.logger.LogStartChild(fmt.Sprintf("SymbolThread-%s", symbol))

	// 1. Init symbol thread
	thread := NewSymbolThread(h, symbol, date, h.Database)
	thread.outbox = h.broadcastInbox

	// 2. Register it with the clock s.t. it recieves ticks
	h.Clock.RegisterPipe(thread.GetTickPipe())

	// 3. Keep ref in map
	h.symbolThreads[symbol] = thread

	// 4. Start the thread
	thread.Start()

	return thread
}
