package replayengine

import (
	"context"
	"maps"
	"slices"
	"sync"
	"time"

	"cloud.google.com/go/civil"
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

// Hub functionality exposed to TickerThread
type BroadcastIngester interface {
	BroadcastMessagePipe() chan<- BroadcastMessage
}

type Hub interface {
	LifeCycle

	HubClientInterface
	DataAvailabilityConsumer
	BroadcastIngester

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

	dataReadyChan chan dataReadyMsg

	DataAvailabilityProvider
	logger ComponentLogger
}

// INIT METHOD
func NewHub() *hub {
	ctx, cancel := context.WithCancel(context.Background())

	return &hub{
		Ctx:                      ctx,
		CancelCtx:                cancel,
		wg:                       sync.WaitGroup{},
		requestsChan:             make(chan hubRequest, 1024),
		broadcast:                make(chan BroadcastMessage, 1024),
		dataReadyChan:            make(chan dataReadyMsg, 1024),
		clients:                  make(map[*client]struct{}),
		tickerToClient:           make(map[Ticker]map[*client]struct{}),
		clientToSubbedTickers:    make(map[*client]map[Ticker]struct{}),
		ownedTickerThreads:       make(map[Ticker]*tickerThread),
		DataAvailabilityProvider: NewDataCoordinater(),
		logger:                   NewLogger("Hub"),
	}
}

func (h *hub) Shutdown() {
	// First shutdown all child components (client, ticker threads, data controller)

	// Then shutdown itself
	h.CancelCtx()
	h.wg.Wait()
	h.logger.Info("shutdown")
}

func (h *hub) Start() {
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
			case sig := <-h.dataReadyChan:
				if _, ok := h.ownedTickerThreads[sig.ticker]; !ok {
					h.StartTickerThread(sig.ticker, sig.date)
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
	h.dataReadyChan <- dataReadyMsg{ticker, date}
}

// DataAvailabilityConsumer interface implementation
func (h *hub) DataReadyPipe() chan<- dataReadyMsg { return h.dataReadyChan }

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

			if h.DataAvailabilityProvider.IsReady(ticker, civil.DateOf(time.Now())) == READY {
				// If it is, send to data ready chan
				h.dataReadyChan <- dataReadyMsg{
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
	thread.AsynShutdown()
	delete(h.ownedTickerThreads, ticker)
}

func (h *hub) StartTickerThread(ticker Ticker, date civil.Date) {
	h.logger.Info("starting ticker thread for %s", ticker)
	// Init ticker thread
	thread := NewTickerThread(h, ticker, date)

	// Add to map
	h.ownedTickerThreads[ticker] = thread

	// Start
	thread.Start()
}
