package hub

import (
	"context"
	"fmt"
	"sync"

	"cloud.google.com/go/civil"
	sim "github.com/mcudby/mwat/components/replayengine/clock"
	"github.com/mcudby/mwat/components/replayengine/common"
	"github.com/mcudby/mwat/components/replayengine/db"
	"github.com/mcudby/mwat/components/replayengine/defaults"
	hdrmgr "github.com/mcudby/mwat/components/replayengine/hydrationmanager"
	"github.com/mcudby/mwat/components/replayengine/logging"
	"github.com/mcudby/mwat/components/replayengine/symbolthread"
	ws "github.com/mcudby/mwat/components/replayengine/wsclient"
)

const hubID = "hub"

const TIMESTREAM = "TIMESTREAM"

var Timeframes = []common.Timeframe{
	common.T1s,
	common.T1m,
}

// hub implements the Hub interface
type hub struct {
	id        string
	ctx       context.Context
	cancelCtx context.CancelFunc
	wg        sync.WaitGroup
	logger    *logging.Logger
	config    *configWrapper

	clientInbox          chan any
	broadcastInbox       chan common.BroadcastMessage
	hydrationStateInbox  <-chan any
	hydrationStateOutbox chan<- any
	signalStartThread    chan common.Stream

	clientsSet               map[*ws.WSClient]struct{}
	streamToSubbedClientsSet map[string]map[*ws.WSClient]struct{}
	streamToThreads          map[string]*symbolthread.SymbolThread
	clientToSubbedStreamsSet map[*ws.WSClient]map[string]struct{}

	subbedToTimestream map[*ws.WSClient]struct{}
	timestreamInbox    <-chan sim.Tick

	HydrationMgr
	clock    Clock
	Database *db.ReplayEngineDB
}

type configWrapper struct {
	lock   sync.Mutex
	config common.SimulationConfig
}

func defaultSimulationConfig() *configWrapper {
	return &configWrapper{
		lock: sync.Mutex{},
		config: common.SimulationConfig{
			Timescale: defaults.DefaultTimescale,
			Date:      defaults.DefaultDay,
		},
	}
}

func (s *configWrapper) GetConfig() common.SimulationConfig {
	s.lock.Lock()
	defer s.lock.Unlock()
	return s.config
}

func (s *configWrapper) SetConfig(newConfig common.SimulationConfig) {
	s.lock.Lock()
	defer s.lock.Unlock()
	s.config = newConfig
}

func NewHub(database *db.ReplayEngineDB) *hub {
	ctx, cancel := context.WithCancel(context.Background())

	config := defaultSimulationConfig()

	timestreamChan := make(chan sim.Tick, 1)
	hydrationStateChan := make(chan any, 1024)

	clock := sim.NewClock(
		timestreamChan,
		config.GetConfig,
	)

	hyrdationMgr := hdrmgr.NewHydrationMgr(database, hydrationStateChan)

	return &hub{
		id:                   hubID,
		ctx:                  ctx,
		cancelCtx:            cancel,
		wg:                   sync.WaitGroup{},
		logger:               logging.NewComponentLogger(hubID),
		config:               config,
		clientInbox:          make(chan any, 1024),
		broadcastInbox:       make(chan common.BroadcastMessage, 1024),
		hydrationStateInbox:  hydrationStateChan,
		hydrationStateOutbox: hydrationStateChan,

		clientsSet: make(map[*ws.WSClient]struct{}),
		// Maps stream id -> set of subscribed clients
		streamToSubbedClientsSet: make(map[string]map[*ws.WSClient]struct{}),
		// Maps client -> set of subscribed-to stream ids
		clientToSubbedStreamsSet: make(map[*ws.WSClient]map[string]struct{}),
		// Maps stream id -> symbol thread
		streamToThreads: make(map[string]*symbolthread.SymbolThread),

		subbedToTimestream: make(map[*ws.WSClient]struct{}),
		timestreamInbox:    timestreamChan,

		HydrationMgr: hyrdationMgr,
		clock:        clock,
		Database:     database,
	}
}

func (h *hub) GetInbox() chan<- any {
	return h.clientInbox
}

func (h *hub) GetID() string { return h.id }

func (h *hub) Shutdown() {
	// Shutdown all child components:
	// - clients
	// - symbol threads
	// - data controller

	for client := range h.clientsSet {
		h.logger.LogStopChild(client.ID)
		client.Shutdown()
	}

	h.logger.LogStopChild(h.HydrationMgr.GetID())
	h.HydrationMgr.Shutdown()

	for _, symbolThread := range h.streamToThreads {
		h.logger.LogStopChild(symbolThread.GetID())
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
				case ws.RegisterRequest:
					h.logger.Info("register requested", "client-id", v.Sender.ID)
					h.handleRegister(v)
				case ws.UnregisterRequest:
					h.logger.Info("unregister requested", "client-id", v.Sender.ID)
					h.handleUnregister(v)
				case ws.SubRequest:
					h.logger.Info("sub requested", "client-id", v.Sender.ID, "streams", v.Streams)
					h.handleSub(v)
				case ws.UnsubRequest:
					h.logger.Info("sub requested", "client-id", v.Sender.ID, "streams", v.Streams)
					h.handleUnsub(v)
				}

			case notification := <-h.hydrationStateInbox:
				switch v := notification.(type) {

				case hdrmgr.HydrationSuccess:
					h.logger.Info("hydration notification received", "symbol", v.Symbol, "date", v.Date.String())

					// todo: start ticker threads for A.<symbol> and/or AM.<symbol>
					// todo: helper func
					id := fmt.Sprintf("AM.%s", v.Symbol)
					hasSubscribers := len(h.streamToSubbedClientsSet[id]) > 0
					threadMissing := h.streamToThreads[id] == nil
					h.logger.Info("", "hasSubscribers", hasSubscribers, "threadMissing", threadMissing)
					if hasSubscribers && threadMissing {
						h.StartTickerThread(common.Stream{Type: "AM", Symbol: v.Symbol}, v.Date)
					}

					id = fmt.Sprintf("A.%s", v.Symbol)
					hasSubscribers = len(h.streamToSubbedClientsSet[id]) > 0
					threadMissing = h.streamToThreads[id] == nil
					h.logger.Info("", "hasSubscribers", hasSubscribers, "threadMissing", threadMissing)
					if hasSubscribers && threadMissing {
						h.StartTickerThread(common.Stream{Type: "AM", Symbol: v.Symbol}, v.Date)
					}

				case hdrmgr.HydrationFailure:
					h.logger.Info("hydration failure notification received", "notification", v)

					// clients := h.streamToSubbedClientsSet[v.Symbol]
					// for c := range clients {
					// 	c.Outbox() <- struct{ msg string }{
					// 		msg: fmt.Sprintf("hydration failed for symbol '%s'", v.Symbol),
					// 	}
					// 	delete(clients, c)
					// }
					// delete(h.streamToSubbedClientsSet, v.Symbol)

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

func (h *hub) handleBroadcast(msg common.BroadcastMessage) {
	for client := range h.streamToSubbedClientsSet[msg.StreamID] {
		client.Outbox() <- msg.Payload
	}
}

func (h *hub) RegisterClient(c *ws.WSClient) {
	h.clientInbox <- ws.RegisterRequest{Sender: c}
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

func (h *hub) GetSimulationSettings() common.SimulationConfig {
	return h.config.GetConfig()
}

func (h *hub) SetSimulationSettings(newConfig common.SimulationConfig) {
	h.config.SetConfig(newConfig)
	h.RestartSimulation()
}

func (h *hub) restartSymbolThreads() {
	wg := sync.WaitGroup{}
	for _, symbolThread := range h.streamToThreads {
		wg.Add(1)
		go func() {
			defer wg.Done()
			h.logger.Info("restarting symbol thread", "symbol-thread-id", symbolThread.GetID())
			symbolThread.Restart()
		}()
	}
	wg.Wait()
}

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

func (h *hub) handleSub(req ws.SubRequest) {
	c := req.Sender
	date := h.config.GetConfig().Date

	for _, stream := range req.Streams {
		if stream.Symbol == TIMESTREAM {
			h.subToTimestream(c)
			continue
		}

		// Ensure Sets are initialised
		if h.streamToSubbedClientsSet[stream.ID()] == nil {
			h.streamToSubbedClientsSet[stream.ID()] = map[*ws.WSClient]struct{}{}
		}
		if h.clientToSubbedStreamsSet[c] == nil {
			h.clientToSubbedStreamsSet[c] = map[string]struct{}{}
		}

		// Update state
		h.streamToSubbedClientsSet[stream.ID()][c] = struct{}{}
		h.clientToSubbedStreamsSet[c][stream.ID()] = struct{}{}

		threadAlreadyStarted := h.streamToThreads[stream.ID()] != nil

		h.logger.Info(
			"client subscribed to stream",
			"client-id", c.ID,
			"stream", stream.ID(),
			"num-subscribed", len(h.streamToSubbedClientsSet[stream.ID()]),
			"thread-already-started", threadAlreadyStarted,
		)

		if threadAlreadyStarted {
			continue
		}

		if h.HydrationMgr.IsReady(stream.Symbol, date) { // Data is already ready
			// Start symbol thread
			h.StartTickerThread(
				stream,
				date,
			)
			return
		}

		// If data is not ready, hydration mgr will send msg to h.hydrationStateInbox
		h.logger.Info(
			"symbol not yet hydrated",
			"symbol", stream.Symbol,
		)
	}
}

func (h *hub) handleUnsub(req ws.UnsubRequest) {
	c := req.Sender
	for _, stream := range req.Streams {
		if stream.Symbol == TIMESTREAM {
			h.unsubToTimestream(c)
			continue
		}

		if clients, ok := h.streamToSubbedClientsSet[stream.ID()]; ok {
			delete(clients, c)

			h.logger.Info(
				"client unsubscribed from stream",
				"client-id", c.ID,
				"stream", stream,
				"num-subscribed", len(h.streamToSubbedClientsSet[stream.ID()]),
			)

			if len(clients) == 0 {
				h.logger.Info("last subscriber unsubbed, killing symbol thread", "stream", stream.ID())
				h.killSymbolThread(stream.ID())
				delete(h.streamToSubbedClientsSet, stream.ID())
			}
		}
	}
}

func (h *hub) handleRegister(req ws.RegisterRequest) {
	c := req.Sender
	h.clientsSet[c] = struct{}{}
	h.logger.Info("client registered to hub", "client-id", c.ID, "client-count", len(h.clientsSet))
	h.logger.LogStartChild(c.ID)
	c.Start()
}

func (h *hub) handleUnregister(req ws.UnregisterRequest) {
	c := req.Sender
	if streamIDs, ok := h.clientToSubbedStreamsSet[c]; ok && len(streamIDs) > 0 {
		// convert []ids to []Streams
		streams := make([]common.Stream, 0, len(streamIDs))
		for id := range streamIDs {
			streams = append(streams, common.StreamFromID(id))
		}

		h.handleUnsub(
			ws.UnsubRequest{
				Sender:  c,
				Streams: streams,
			},
		)
	}

	delete(h.clientsSet, c)
	h.logger.Info("client unregistered from hub", "client-id", c.ID, "client-count", len(h.clientsSet))
}

func (h *hub) killSymbolThread(symbol string) {
	thread := h.streamToThreads[symbol]

	h.logger.LogStopChild(thread.GetID())

	thread.AsyncShutdown()

	delete(h.streamToThreads, symbol)
	h.clock.UnregisterPipe(thread.TickChan)
}

// todo: update
func (h *hub) StartTickerThread(stream common.Stream, date civil.Date) *symbolthread.SymbolThread {
	h.logger.Info("starting symbol thread", "stream-id", stream.ID())

	tickChan := make(chan int64)

	// 1. Init symbol thread
	thread := symbolthread.NewSymbolThread(
		stream,
		date,
		h.Database,
		h.broadcastInbox,
		tickChan,
		h.clock.GetSimulationTime,
	)

	// 2. Register it with the clock s.t. it recieves ticks
	h.clock.RegisterPipe(tickChan)

	// 3. Keep ref in map
	h.streamToThreads[stream.ID()] = thread

	// 4. Start the thread
	h.logger.LogStartChild(thread.GetID())

	go thread.Start()

	return thread
}
