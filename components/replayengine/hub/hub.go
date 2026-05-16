package hub

import (
	"context"
	"fmt"
	"sync"

	"cloud.google.com/go/civil"
	"github.com/mcudby/mwat/components/replayengine/api"
	"github.com/mcudby/mwat/components/replayengine/clock"
	"github.com/mcudby/mwat/components/replayengine/common"
	"github.com/mcudby/mwat/components/replayengine/db"
	"github.com/mcudby/mwat/components/replayengine/logging"
	"github.com/mcudby/mwat/components/replayengine/symbolthread"
	ws "github.com/mcudby/mwat/components/replayengine/wsclient"
	hdrmgr "github.com/mcudby/mwat/components/replayengine/hydrationmanager"
)

const (
	hubID      = "hub"
	TIMESTREAM = "TIMESTREAM"
)

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

	clientsSet                 map[*ws.WSClient]struct{}
	streamIDToSubbedClientsSet map[string]map[*ws.WSClient]struct{}
	streamToThreads            map[string]*symbolthread.SymbolThread
	clientToSubbedStreamsSet   map[*ws.WSClient]map[string]struct{}

	subbedToTimestream map[*ws.WSClient]struct{}
	timestreamInbox    <-chan api.Tick

	HydrationMgr
	clock    Clock
	Database *db.ReplayEngineDB
}

func NewHub(database *db.ReplayEngineDB) *hub {
	ctx, cancel := context.WithCancel(context.Background())

	config := defaultSimulationConfig()

	timestreamChan := make(chan api.Tick, 1)
	hydrationStateChan := make(chan any, 1024)

	clock := clock.NewClock(
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

		clientsSet:                 make(map[*ws.WSClient]struct{}),
		streamIDToSubbedClientsSet: make(map[string]map[*ws.WSClient]struct{}),
		clientToSubbedStreamsSet:   make(map[*ws.WSClient]map[string]struct{}),
		streamToThreads:            make(map[string]*symbolthread.SymbolThread),

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
					h.handleRegister(v)
				case ws.UnregisterRequest:
					h.handleUnregister(v)
				case ws.SubRequest:
					h.handleSub(v)
				case ws.UnsubRequest:
					h.handleUnsub(v)
				}

			case notification := <-h.hydrationStateInbox:
				switch v := notification.(type) {

				case hdrmgr.HydrationSuccess:
					h.logger.Info("hydration success msg received", "symbol", v.Symbol, "date", v.Date.String())

					h.startStreamThreadsIfRequired(v.Symbol, v.Date)

				case hdrmgr.HydrationFailure:
					h.logger.Info("hydration failure msg received", "symbol", v.Symbol, "date", v.Date.String())

					h.notifyOfHydrationFailure(v.Symbol, v.Date)

				default:
					h.logger.Fatal("unrecognised notification received", "v", v)
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

func (h *hub) startStreamThreadsIfRequired(symbol string, date civil.Date) {
	for _, _type := range api.StreamTypes {
		streamID := fmt.Sprintf("%s.%s", _type, symbol)
		hasSubscribers := len(h.streamIDToSubbedClientsSet[streamID]) > 0
		threadMissing := h.streamToThreads[streamID] == nil
		if hasSubscribers && threadMissing {
			h.StartTickerThread(common.Stream{Type: "AM", Symbol: symbol}, date)
		}
	}
}

func (h *hub) notifyOfHydrationFailure(symbol string, date civil.Date) {
	for _, _type := range api.StreamTypes {
		streamID := fmt.Sprintf("%s.%s", _type, symbol)

		for c := range h.streamIDToSubbedClientsSet[streamID] {
			c.Outbox() <- api.FailedHydrationMsg{
				Msg: "hydration failed",
				Symbol: symbol,
				Date: date.String(),
			}
		}
	}
}

func (h *hub) handleBroadcast(msg common.BroadcastMessage) {
	for client := range h.streamIDToSubbedClientsSet[msg.StreamID] {
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
	h.logger.Info("sub requested", "client-id", req.Sender.ID, "streams", req.Streams)
	client := req.Sender
	date := h.config.GetConfig().Date

	for _, stream := range req.Streams {
		streamID := stream.ID()

		if stream.Symbol == TIMESTREAM {
			h.subToTimestream(client)
			continue
		}

		// Ensure Sets are initialised
		if h.streamIDToSubbedClientsSet[streamID] == nil {
			h.streamIDToSubbedClientsSet[streamID] = map[*ws.WSClient]struct{}{}
		}
		if h.clientToSubbedStreamsSet[client] == nil {
			h.clientToSubbedStreamsSet[client] = map[string]struct{}{}
		}

		// Update state
		h.streamIDToSubbedClientsSet[streamID][client] = struct{}{}
		h.clientToSubbedStreamsSet[client][streamID] = struct{}{}

		threadAlreadyStarted := h.streamToThreads[stream.ID()] != nil

		h.logger.Info(
			"client subscribed to stream",
			"client-id", client.ID,
			"stream", stream.ID(),
			"num-subscribed-to-stream", len(h.streamIDToSubbedClientsSet[streamID]),
			"client-subscription-count", len(h.clientToSubbedStreamsSet[client]),
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

		// If data is not ready, hydration mgr will notify when it is
		h.logger.Info(
			"symbol not yet hydrated",
			"symbol", stream.Symbol,
			"date", date.String(),
		)
	}
}

func (h *hub) handleUnsub(req ws.UnsubRequest) {
	h.logger.Info("sub requested", "client-id", req.Sender.ID, "streams", req.Streams)
	c := req.Sender
	for _, stream := range req.Streams {
		if stream.Symbol == TIMESTREAM {
			h.unsubToTimestream(c)
			continue
		}

		if clients, ok := h.streamIDToSubbedClientsSet[stream.ID()]; ok {
			delete(clients, c)

			h.logger.Info(
				"client unsubscribed from stream",
				"client-id", c.ID,
				"stream", stream,
				"num-subscribed", len(h.streamIDToSubbedClientsSet[stream.ID()]),
			)

			if len(clients) == 0 {
				h.logger.Info("last subscriber unsubbed, killing symbol thread", "stream", stream.ID())
				h.killSymbolThread(stream.ID())
				delete(h.streamIDToSubbedClientsSet, stream.ID())
			}
		}
	}
}

func (h *hub) handleRegister(req ws.RegisterRequest) {
	h.logger.Info("register requested", "client-id", req.Sender.ID)
	c := req.Sender
	h.clientsSet[c] = struct{}{}
	h.logger.Info("client registered to hub", "client-id", c.ID, "client-count", len(h.clientsSet))
	h.logger.LogStartChild(c.ID)
	c.Start()
}

func (h *hub) handleUnregister(req ws.UnregisterRequest) {
	h.logger.Info("unregister requested", "client-id", req.Sender.ID)
	c := req.Sender
	if streamIDs, ok := h.clientToSubbedStreamsSet[c]; ok && len(streamIDs) > 0 {
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

func (h *hub) StartTickerThread(stream common.Stream, date civil.Date) *symbolthread.SymbolThread {
	h.logger.Info("starting symbol thread", "stream-id", stream.ID())

	tickChan := make(chan int64)

	thread := symbolthread.NewSymbolThread(
		stream,
		date,
		h.Database,
		h.broadcastInbox,
		tickChan,
		h.clock.GetSimulationTime,
	)

	h.clock.RegisterPipe(tickChan)

	h.streamToThreads[stream.ID()] = thread

	h.logger.LogStartChild(thread.GetID())

	go thread.Start()

	return thread
}
