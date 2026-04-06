package replayengine

import (
	"context"
	"fmt"
	"maps"
	"os"
	"slices"
	"sync"

	"cloud.google.com/go/civil"
	"github.com/matthewCudby22896/market_wide_anomaly_tracker/components/replayengine/common"
	"github.com/matthewCudby22896/market_wide_anomaly_tracker/components/replayengine/db"
)

type HubReqType int

const (
	REGISTER HubReqType = iota
	UNREGISTER
	SUB
	UNSUB
)

type hubRequest struct {
	Type    HubReqType
	Client  *client
	Symbols []common.Symbol
}

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

	RegisterClient(c *client)
}

// hub implements the Hub interface
type hub struct {
	Ctx       context.Context
	CancelCtx context.CancelFunc
	wg        sync.WaitGroup

	clientRequestInbox chan ClientRequest

	broadcast chan BroadcastMessage

	clients               map[*client]struct{} // A 'Set' of the registered clients
	symbolToClient        map[common.Symbol]map[*client]struct{}
	symbolThreads         map[common.Symbol]*symbolThread
	clientToSubbedSymbols map[*client]map[common.Symbol]struct{}

	notificationChan chan any
	logger           ComponentLogger

	DataCoordinator
	Clock
	Database db.Database
}

func NewHub(database db.Database) *hub {
	ctx, cancel := context.WithCancel(context.Background())

	h := &hub{
		Ctx:                   ctx,
		CancelCtx:             cancel,
		wg:                    sync.WaitGroup{},
		clientRequestInbox:    make(chan ClientRequest, 1024),
		broadcast:             make(chan BroadcastMessage, 1024),
		notificationChan:      make(chan any, 1024),
		clients:               make(map[*client]struct{}),
		symbolToClient:        make(map[common.Symbol]map[*client]struct{}),
		clientToSubbedSymbols: make(map[*client]map[common.Symbol]struct{}),
		symbolThreads:         make(map[common.Symbol]*symbolThread),
		logger:                NewLogger("Hub"),
		DataCoordinator:       NewDataCoordinator(database),
		Clock:                 NewClock(DEFAULT_DAY, DEFAULT_SPEEDUP),
		Database:              database,
	}

	h.DataCoordinator.SetOutbox(h.notificationChan)

	return h
}

func (h *hub) Shutdown() {
	// First shutdown all child components (client, ticker threads, data controller)
	h.logger.LogShutdownChild("DataCoordinator")
	h.DataCoordinator.Shutdown()

	for ticker, tickerThread := range h.symbolThreads {
		h.logger.LogShutdownChild(fmt.Sprintf("TickerThread-%s", ticker))
		tickerThread.Shutdown()
	}

	// Then shutdown itself
	h.CancelCtx()
	h.wg.Wait()
	h.logger.LogShutdown()
}

func (h *hub) Start() {
	h.logger.LogStartChild("DataCoordinator")
	h.DataCoordinator.Start()
	h.Clock.Start()

	h.wg.Add(1)
	go func() {
		defer h.wg.Done()
		for {
			select {
			case <-h.Ctx.Done():
				return

			case msg := <-h.broadcast:
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

			case notification := <-h.notificationChan:
				switch v := notification.(type) {
				case hydrationSuccess:
					h.logger.Info("hydrationSuccess notification recieved: %#v\n", v)
					h.StartTickerThread(v.Symbol, v.Date)

				case hydrationFailure:
					h.logger.Info("hydrationFailure notification recieved: %#v\n", v)

					clients := h.symbolToClient[v.Symbol]
					for c, _ := range clients {
						c.Outbox() <- struct{ msg string }{
							msg: fmt.Sprintf("hydration failed for symbol '%s'", v.Symbol),
						}
						delete(clients, c)
					}
					delete(h.symbolToClient, v.Symbol)

				default:
					h.logger.Errorf("unrecognised notification recieved: %#v\n", v)
					os.Exit(1)
				}
			}
		}
	}()
	h.logger.Info("started.")
}

func (h *hub) handleBroadcast(msg BroadcastMessage) {
	// Fan-out msg to subscribed clients
	for client := range h.symbolToClient[msg.Symbol] {
		client.Outbox() <- msg.Data
	}
}

func (h *hub) RegisterClient(c *client) {
	h.clientRequestInbox <- registerRequest{BaseRequest{c}}
}

func (h *hub) handleSub(req subRequest) {
	c := req.Sender

	for _, symbol := range req.symbols {

		if _, ok := h.symbolThreads[symbol]; !ok {
			h.logger.Info("first subscriber for %s. Starting ticker thread.", symbol)

			if h.DataCoordinator.IsReady(symbol, DEFAULT_DAY) {

				// If ready, notify main loop
				h.notificationChan <- hydrationSuccess{
					Symbol: symbol,
					Date:   DEFAULT_DAY,
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
			h.logger.Info("client already subscribed to %s. Ignoring subscription request.", symbol)
			continue
		}

		h.symbolToClient[symbol][c] = struct{}{}
		h.clientToSubbedSymbols[c][symbol] = struct{}{}

		h.logger.Info("client subscribed to %s, total %d", symbol, len(h.symbolToClient[symbol]))
	}
}

func (h *hub) handleUnsub(req unsubRequest) {
	symbols := req.symbols
	c := req.Sender
	for _, symbol := range symbols {
		if clients, ok := h.symbolToClient[symbol]; ok {
			delete(clients, c)

			if len(clients) == 0 {
				h.logger.Info("last subscriber left for %s. Killing symbol thread.", symbol)
				h.killSymbolThread(symbol)
				delete(h.symbolToClient, symbol)
			}

			h.logger.Info("a client unsubscribed from %s, total %d", symbol, len(h.symbolToClient[symbol]))
		}
	}
}

func (h *hub) handleRegister(req registerRequest) {
	c := req.Sender
	h.clients[c] = struct{}{}
	c.hubRequestOutbox = h.clientRequestInbox
	h.logger.Info("a client has been registered, total: %d", len(h.clients))
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

	h.logger.Info("a client has been unregistered, total: %d", len(h.clients))
}

func (h *hub) killSymbolThread(symbol common.Symbol) {
	thread := h.symbolThreads[symbol]
	h.logger.LogShutdownChild(fmt.Sprintf("SymbolThread-%s", symbol))
	thread.AsynShutdown()
	delete(h.symbolThreads, symbol)
}

func (h *hub) StartTickerThread(symbol common.Symbol, date civil.Date) *symbolThread {
	h.logger.LogStartChild(fmt.Sprintf("SymbolThread-%s", symbol))

	// 1. Init symbol thread
	thread := NewSymbolThread(h, symbol, date, h.Database)
	thread.outbox = h.broadcast

	// 2. Register it with the clock s.t. it recieves ticks
	h.Clock.RegisterPipe(thread.GetTickPipe())

	// 3. Keep ref in map
	h.symbolThreads[symbol] = thread

	// 4. Start the thread
	thread.Start()

	return thread
}
