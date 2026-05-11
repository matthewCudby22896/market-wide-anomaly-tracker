package hub

import (
	"context"
	"fmt"
	"maps"
	"slices"
	"sync"

	"cloud.google.com/go/civil"
	sim "github.com/mcudby/mwat/components/replayengine/clock"
	"github.com/mcudby/mwat/components/replayengine/db"
	hdrmgr "github.com/mcudby/mwat/components/replayengine/hydrationmanager"
	"github.com/mcudby/mwat/components/replayengine/logging"
	ws "github.com/mcudby/mwat/components/replayengine/wsclient"
)

type HubReqType int

const (
	REGISTER HubReqType = iota
	UNREGISTER
	SUB
	UNSUB
)

const hubID = "hub"

// hub implements the Hub interface
type hub struct {
	id        string
	ctx       context.Context
	cancelCtx context.CancelFunc
	wg        sync.WaitGroup
	logger    *logging.Logger
	config    *configWrapper

	clientInbox   		 chan any 
	broadcastInbox       chan any
	hydrationStateInbox  <-chan any
	hydrationStateOutbox chan<- any

	clients               map[*ws.WSClient]struct{}
	symbolToClient        map[string]map[*ws.WSClient]struct{}
	symbolThreads         map[string]*symbolThread
	clientToSubbedSymbols map[*ws.WSClient]map[string]struct{}

	subbedToTimestream map[*ws.WSClient]struct{}
	timestreamInbox    <-chan sim.Tick

	HydrationMgr
	clock    Clock
	Database *db.ReplayEngineDB
}

type SimulationConfig struct {
	Timescale float32
	Date      civil.Date
}

type configWrapper struct {
	lock   sync.Mutex
	config SimulationConfig
}

func defaultSimulationConfig() *configWrapper {
	return &configWrapper{
		lock: sync.Mutex{},
		config: SimulationConfig{
			Timescale: DefaultTimescale,
			Date:      DefaultDay,
		},
	}
}

func (s *configWrapper) GetConfig() SimulationConfig {
	s.lock.Lock()
	defer s.lock.Unlock()
	return s.config
}

func (s *configWrapper) SetConfig(newConfig SimulationConfig) {
	s.lock.Lock()
	defer s.lock.Unlock()
	s.config = newConfig
}

func NewHub(database *db.ReplayEngineDB) *hub {
	ctx, cancel := context.WithCancel(context.Background())

	config := defaultSimulationConfig()

	timestreamChan := make(chan sim.Tick, 1)
	hydrationStateChan := make(chan any, 1024)

	// Init clock
	clock := sim.NewClock(
		timestreamChan,
		config.GetConfig,
	)

	// Init data coordinator
	hyrdationMgr := hdrmgr.NewHydrationMgr(database, hydrationStateChan)

	// Hub
	h := &hub{
		id:                   hubID,
		ctx:                  ctx,
		cancelCtx:            cancel,
		wg:                   sync.WaitGroup{},
		logger:               logging.NewComponentLogger(hubID),
		config:               config,
		clientInbox:   		  make(chan any, 1024),
		broadcastInbox:       make(chan broadcastMessage, 1024),
		hydrationStateInbox:  hydrationStateChan,
		hydrationStateOutbox: hydrationStateChan,

		clients:               make(map[*ws.WSClient]struct{}),
		symbolToClient:        make(map[string]map[*ws.WSClient]struct{}),
		clientToSubbedSymbols: make(map[*ws.WSClient]map[string]struct{}),
		symbolThreads:         make(map[string]*symbolThread),

		subbedToTimestream: make(map[*ws.WSClient]struct{}),
		timestreamInbox:    timestreamChan,

		HydrationMgr: hyrdationMgr,
		clock:        clock,
		Database:     database,
	}

	return h
}

func (h *hub) GetInbox() chan<- any{
	return h.clientInbox
}

func (h *hub) GetID() string { return h.id }

func (h *hub) Shutdown() {
	// Shutdown all child components:
	// - clients
	// - symbol threads
	// - data controller

	for client := range h.clients {
		h.logger.LogStopChild(client.ID)
		client.Shutdown()
	}

	h.logger.LogStopChild(h.HydrationMgr.GetID())
	h.HydrationMgr.Shutdown()

	for _, symbolThread := range h.symbolThreads {
		h.logger.LogStopChild(symbolThread.id)
		symbolThread.Shutdown()
	}

	// Shutdown self
	h.cancelCtx()
	h.wg.Wait()
	h.logger.LogShutdown()
}

func (h *hub) Start() {
	h.logger.LogStartChild(h.HydrationMgr.GetID())
	h.HydrationMgr.Start()
	h.clock.Start()

	h.wg.Add(1)
	go func() {
		defer h.wg.Done()
		for {
			select {
			case <-h.ctx.Done():
				return

			case msg := <-h.broadcastInbox:
				h.handleBroadcast(msg)

			case req := <-h.clientInbox:
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

			case notification := <-h.hydrationStateInbox:
				switch v := notification.(type) {
				case hdrmgr.HydrationSuccess:
					h.logger.Info("hydration notification received", "notification", v)
					h.StartTickerThread(v.Symbol, v.Date)

				case hdrmgr.HydrationFailure:
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
			case tick := <-h.timestreamInbox:
				for c := range h.subbedToTimestream {
					c.Outbox() <- tick
				}
			}
		}
	}()
	h.logger.LogStart()
}

func (h *hub) handleBroadcast(msg broadcastMessage) {
	for client := range h.symbolToClient[msg.Symbol] {
		client.Outbox() <- msg.Data
	}
}

func (h *hub) RegisterClient(c *client) {
	h.clientInbox <- registerRequest{BaseRequest{c}}
}

func (h *hub) PauseSimulation() {
	h.clock.Pause()
}

func (h *hub) ResumeSimulation() {
	h.clock.Resume()
}

func (h *hub) RestartSimulation() {
	h.clock.Restart()
	h.restartSymbolThreads()
}

func (h *hub) HydrateSymbol(ctx context.Context, symbol string, date civil.Date) error {
	return h.HydrationMgr.HydrateSymbol(ctx, symbol, date)
}

func (h *hub) GetSimulationSettings() SimulationConfig {
	return h.config.GetConfig()
}

func (h *hub) SetSimulationSettings(newConfig SimulationConfig) {
	h.config.SetConfig(newConfig)
	h.RestartSimulation()
}

func (h *hub) restartSymbolThreads() {
	wg := sync.WaitGroup{}
	for _, symbolThread := range h.symbolThreads {
		wg.Add(1)
		go func() {
			defer wg.Done()
			h.logger.Info("restarting symbol thread", "symbol-thread-id", symbolThread.id)
			symbolThread.Restart()
		}()
	}
	wg.Wait()
}

const TIMESTREAM = "TIMESTREAM"

func (h *hub) subToTimestream(c *ws.WSClient) {
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

func (h *hub) unsubToTimestream(c *ws.WSClient) {
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
	date := h.config.GetConfig().Date

	for _, symbol := range req.symbols {
		if symbol == TIMESTREAM {
			h.subToTimestream(c)
			continue
		}

		if _, ok := h.symbolThreads[symbol]; !ok {
			h.logger.Info("first subscriber, starting symbol thread", "client-id", c.ID, "symbol", symbol)

			if h.HydrationMgr.IsReady(symbol, date) {

				// If ready, notify main loop
				h.hydrationStateOutbox <- hdrmgr.HydrationSuccess{
					Symbol: symbol,
					Date:   date,
				}
			}

			// If not, DataAvailabilityProvider will eventually signal that it is
		}

		if h.symbolToClient[symbol] == nil {
			h.symbolToClient[symbol] = make(map[*ws.WSClient]struct{})
		}

		if h.clientToSubbedSymbols[c] == nil {
			h.clientToSubbedSymbols[c] = make(map[string]struct{})
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
	c.requestOutbox = h.clientInbox
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

func (h *hub) killSymbolThread(symbol string) {
	thread := h.symbolThreads[symbol]

	h.logger.LogStopChild(thread.id)

	// Trigger shutdown
	thread.AsyncShutdown()

	delete(h.symbolThreads, symbol)
	h.clock.UnregisterPipe(thread.TickChan)
}

func (h *hub) StartTickerThread(symbol string, date civil.Date) *symbolThread {
	h.logger.Info("StartTickerThread()!")
	h.logger.LogStartChild(fmt.Sprintf("SymbolThread-%s", symbol))

	tickChan := make(chan int64)

	// 1. Init symbol thread
	thread := NewSymbolThread(
		symbol,
		date,
		h.Database,
		h.broadcastInbox,
		tickChan,
		h.clock.GetSimulationTime,
	)

	// 2. Register it with the clock s.t. it recieves ticks
	h.clock.RegisterPipe(tickChan)

	// 3. Keep ref in map
	h.symbolThreads[symbol] = thread

	// 4. Start the thread
	go thread.Start()

	return thread
}