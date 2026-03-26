package replayengine

import (
	"context"
	"fmt"
	"maps"
	"slices"
	"sync"
	"time"

	"cloud.google.com/go/civil"
	"github.com/matthewCudby22896/market_wide_anomaly_tracker/components/replayengine/db"
)

var DEFAULT_DAY = civil.Date{Year: 2025, Month: 3, Day: 20}
var DEFAULT_SPEEDUP int = 2

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
	Tickers []Ticker
}

// Hub functionality exposed to dataCoordinator
type DataAvailabilityConsumer interface {
	SignalDataReady(ticker Ticker, date civil.Date)
}

type HubClientInterface interface {
	RequestSub(c *client, tickers []Ticker)
	RequestUnsub(c *client, tickers []Ticker)
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
	tickerToClient        map[Ticker]map[*client]struct{}
	ownedTickerThreads    map[Ticker]*tickerThread
	clientToSubbedTickers map[*client]map[Ticker]struct{}

	notificationChan chan any
	logger           ComponentLogger

	DataCoordinator
	Clock
	Database db.Database
}

// INIT METHOD
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
		tickerToClient:        make(map[Ticker]map[*client]struct{}),
		clientToSubbedTickers: make(map[*client]map[Ticker]struct{}),
		ownedTickerThreads:    make(map[Ticker]*tickerThread),
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

	for ticker, tickerThread := range h.ownedTickerThreads {
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
					h.StartTickerThread(v.Ticker, v.Date)

				case hydrationFailure:
					h.logger.Info("hydrationFailure notification recieved: %#v\n", v)
					// TODO: Decide what to do in this case
				default:
					h.logger.Info("unrecognised notification recieved: %#v\n", v)
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

func (h *hub) RequestSub(c *client, tickers []Ticker) {
	h.requestsChan <- hubRequest{SUB, c, tickers}
}

func (h *hub) RequestUnsub(c *client, tickers []Ticker) {
	h.requestsChan <- hubRequest{UNSUB, c, tickers}
}

func (h *hub) SignalDataReady(ticker Ticker, date civil.Date) {
	h.notificationChan <- dataReadyMsg{ticker, date}
}

// BroadcastIngester interface implementation
func (h *hub) BroadcastMessagePipe() chan<- BroadcastMessage { return h.broadcast }

func (h *hub) handleBroadcast(msg BroadcastMessage) {
	// Fan-out msg to subscribed clients
	for client := range h.tickerToClient[msg.Ticker] {
		client.Outbox() <- msg.Data
	}
}

func (h *hub) handleSub(c *client, tickers []Ticker) {
	for _, ticker := range tickers {

		if _, ok := h.ownedTickerThreads[ticker]; !ok {
			h.logger.Info("first subscriber for %s. Starting ticker thread.", ticker)

			if h.DataCoordinator.IsReady(ticker, DEFAULT_DAY) {
				fmt.Printf("dataCoordinator returned ready immediately")
				// If it is, send to data ready chan
				h.notificationChan <- dataReadyMsg{
					ticker: ticker,
					date:   civil.DateOf(time.Now()),
				}
			}

			// If not, DataAvailabilityProvider will eventually signal that it is
		}

		if h.tickerToClient[ticker] == nil {
			h.tickerToClient[ticker] = make(map[*client]struct{})
		}

		if h.clientToSubbedTickers[c] == nil {
			h.clientToSubbedTickers[c] = make(map[Ticker]struct{})
		}

		if _, ok := h.tickerToClient[ticker][c]; ok {
			h.logger.Info("client already subscribed to %s. Ignoring subscription request.", ticker)
			continue
		}

		h.tickerToClient[ticker][c] = struct{}{}
		h.clientToSubbedTickers[c][ticker] = struct{}{}

		h.logger.Info("client subscribed to %s, total %d", ticker, len(h.tickerToClient[ticker]))
	}
}

func (h *hub) handleUnsub(c *client, tickers []Ticker) {
	for _, ticker := range tickers {
		if clients, ok := h.tickerToClient[ticker]; ok {
			delete(clients, c)

			if len(clients) == 0 {
				h.logger.Info("last subscriber left for %s. Killing ticker thread.", ticker)
				h.killTickerThread(ticker)
				delete(h.tickerToClient, ticker)
			}

			h.logger.Info("a client unsubscribed from %s, total %d", ticker, len(h.tickerToClient[ticker]))
		}
	}
}

func (h *hub) handleRegister(c *client) {
	h.clients[c] = struct{}{}
	h.logger.Info("a client has been registered, total: %d", len(h.clients))
}

func (h *hub) handleUnregister(c *client) {
	if tickers, ok := h.clientToSubbedTickers[c]; ok && len(tickers) > 0 {
		h.handleUnsub(
			c,
			slices.Collect(maps.Keys(tickers)),
		)
	}

	delete(h.clients, c)

	h.logger.Info("a client has been unregistered, total: %d", len(h.clients))
}

func (h *hub) killTickerThread(ticker Ticker) {
	thread := h.ownedTickerThreads[ticker]
	h.logger.LogShutdownChild(fmt.Sprintf("TickerThread-%s", ticker))
	thread.AsynShutdown()
	delete(h.ownedTickerThreads, ticker)
}

func (h *hub) StartTickerThread(ticker Ticker, date civil.Date) *tickerThread {
	h.logger.LogStartChild(fmt.Sprintf("TickerThread-%s", ticker))

	// 1. Init ticker thread
	thread := NewTickerThread(h, ticker, date)
	thread.outbox = h.broadcast

	// 2. Register it with the clock s.t. it recieves ticks
	h.Clock.RegisterPipe(thread.GetTickPipe())

	// 3. Keep ref in map
	h.ownedTickerThreads[ticker] = thread

	// 4. Start the thread
	thread.Start()

	return thread
}
