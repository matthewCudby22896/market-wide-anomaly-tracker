package replayengine

import (
	"context"
	"fmt"
	"maps"
	"os"
	"slices"
	"sync"

	"cloud.google.com/go/civil"
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
	Tickers []Symbol
}

// Hub functionality exposed to dataCoordinator
type DataAvailabilityConsumer interface {
	SignalDataReady(ticker Symbol, date civil.Date)
}

type HubClientInterface interface {
	RequestSub(c *client, tickers []Symbol)
	RequestUnsub(c *client, tickers []Symbol)
	UnregisterClient(c *client)
}

type Hub interface {
	LifeCycle

	HubClientInterface
	DataAvailabilityConsumer

	RegisterClient(c *client)
}

// hub implements the Hub interface
type hub struct {
	// Rework
	Ctx       context.Context
	CancelCtx context.CancelFunc
	wg        sync.WaitGroup

	requestsChan chan hubRequest

	broadcast chan BroadcastMessage

	clients               map[*client]struct{} // A 'Set' of the registered clients
	symbolToClient        map[Symbol]map[*client]struct{}
	symbolThreads         map[Symbol]*symbolThread
	clientToSubbedSymbols map[*client]map[Symbol]struct{}

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
		requestsChan:          make(chan hubRequest, 1024),
		broadcast:             make(chan BroadcastMessage, 1024),
		notificationChan:      make(chan any, 1024),
		clients:               make(map[*client]struct{}),
		symbolToClient:        make(map[Symbol]map[*client]struct{}),
		clientToSubbedSymbols: make(map[*client]map[Symbol]struct{}),
		symbolThreads:         make(map[Symbol]*symbolThread),
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

			case req := <-h.requestsChan:
				switch req.Type {
				case REGISTER:
					h.handleRegister(req.Client)
				case UNREGISTER:
					h.handleUnregister(req.Client)
				case SUB:
					h.handleSub(req.Client, req.Tickers)
				case UNSUB:
					h.handleUnsub(req.Client, req.Tickers)
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

func (h *hub) RegisterClient(c *client) {
	h.requestsChan <- hubRequest{
		Type:   REGISTER,
		Client: c,
	}
}

func (h *hub) UnregisterClient(c *client) {
	h.requestsChan <- hubRequest{
		Type:   UNREGISTER,
		Client: c,
	}
}

func (h *hub) RequestSub(c *client, tickers []Symbol) {
	h.requestsChan <- hubRequest{SUB, c, tickers}
}

func (h *hub) RequestUnsub(c *client, tickers []Symbol) {
	h.requestsChan <- hubRequest{UNSUB, c, tickers}
}

func (h *hub) SignalDataReady(ticker Symbol, date civil.Date) {
	h.notificationChan <- symbolHydrated{ticker, date}
}

// BroadcastIngester interface implementation
func (h *hub) BroadcastMessagePipe() chan<- BroadcastMessage { return h.broadcast }

func (h *hub) handleBroadcast(msg BroadcastMessage) {
	// Fan-out msg to subscribed clients
	for client := range h.symbolToClient[msg.Ticker] {
		client.Outbox() <- msg.Data
	}
}

func (h *hub) handleSub(c *client, symbols []Symbol) {
	for _, symbol := range symbols {

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
			h.clientToSubbedSymbols[c] = make(map[Symbol]struct{})
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

func (h *hub) handleUnsub(c *client, tickers []Symbol) {
	for _, ticker := range tickers {
		if clients, ok := h.symbolToClient[ticker]; ok {
			delete(clients, c)

			if len(clients) == 0 {
				h.logger.Info("last subscriber left for %s. Killing ticker thread.", ticker)
				h.killTickerThread(ticker)
				delete(h.symbolToClient, ticker)
			}

			h.logger.Info("a client unsubscribed from %s, total %d", ticker, len(h.symbolToClient[ticker]))
		}
	}
}

func (h *hub) handleRegister(c *client) {
	h.clients[c] = struct{}{}
	h.logger.Info("a client has been registered, total: %d", len(h.clients))
}

func (h *hub) handleUnregister(c *client) {
	if tickers, ok := h.clientToSubbedSymbols[c]; ok && len(tickers) > 0 {
		h.handleUnsub(
			c,
			slices.Collect(maps.Keys(tickers)),
		)
	}

	delete(h.clients, c)

	h.logger.Info("a client has been unregistered, total: %d", len(h.clients))
}

func (h *hub) killTickerThread(ticker Symbol) {
	thread := h.symbolThreads[ticker]
	h.logger.LogShutdownChild(fmt.Sprintf("TickerThread-%s", ticker))
	thread.AsynShutdown()
	delete(h.symbolThreads, ticker)
}

func (h *hub) StartTickerThread(ticker Symbol, date civil.Date) *symbolThread {
	h.logger.LogStartChild(fmt.Sprintf("TickerThread-%s", ticker))

	// 1. Init ticker thread
	thread := NewSymbolThread(h, ticker, date, h.Database)
	thread.outbox = h.broadcast

	// 2. Register it with the clock s.t. it recieves ticks
	h.Clock.RegisterPipe(thread.GetTickPipe())

	// 3. Keep ref in map
	h.symbolThreads[ticker] = thread

	// 4. Start the thread
	thread.Start()

	return thread
}
