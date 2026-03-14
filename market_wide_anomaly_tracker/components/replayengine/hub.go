package replayengine

import (
	"fmt"
	"maps"
	"slices"
)

// Hub functionality exposed to dataCoordinator
type DataAvailabilityConsumer interface {
	DataReadyPipe() chan<- DataReadyMsg
}

// Hub functionality exposed to Clients
type ClientSignallingAPI interface {
	RegisterPipe() chan<- *Client
	UnregisterPipe() chan<- *Client
	SubscribePipe() chan<- SubscriptionRequest
	UnsubPipe() chan<- SubscriptionRequest
}

// Hub functionality exposed to TickerThread
type TickerThreadAPI interface {
	BroadcastMessagePipe() chan<- BroadcastMessage
}

type Hub interface {
	ClientSignallingAPI
	DataAvailabilityConsumer
	TickerThreadAPI
	SignalShutdownPipe() chan<- struct{}
}

// hub implements the Hub interface
type hub struct {
	shutdownChan chan struct{}

	register    chan *Client
	unregister  chan *Client
	subscribe   chan SubscriptionRequest
	unsubscribe chan SubscriptionRequest

	broadcast chan BroadcastMessage

	// Essentially a Set of the currently `registered` clients
	clients map[*Client]struct{}
	// e.g. "APPL" to slice of Client's subscribed to APPL
	tickerToClient map[Ticker]map[*Client]struct{}
	// For the hub to maintain a reference to each of it's owned TickerThreads
	ownedTickerThreads map[Ticker]*TickerThreadOwner
	// Used when a client has disconnected and the Hub needs to unsub the client from all it's subbed tickers
	clientToSubbedTickers map[*Client]map[Ticker]struct{}

	dataReadyChan chan DataReadyMsg
}

// INIT METHOD
func NewHub() *hub {
	return &hub{
		register:      make(chan *Client),
		unregister:    make(chan *Client),
		subscribe:     make(chan SubscriptionRequest),
		unsubscribe:   make(chan SubscriptionRequest),
		broadcast:     make(chan BroadcastMessage, 1024), // Buffer high-volume data
		shutdownChan:  make(chan struct{}),
		dataReadyChan: make(chan DataReadyMsg),

		// Maps: Must be initialized via make() or they will panic on first use.
		clients:               make(map[*Client]struct{}),
		tickerToClient:        make(map[Ticker]map[*Client]struct{}),
		ownedTickerThreads:    make(map[Ticker]*TickerThreadOwner),
		clientToSubbedTickers: make(map[*Client]map[Ticker]struct{}),
	}
}

// ClientSignallingAPI interface implementation
func (h *hub) RegisterPipe() chan<- *Client   { return h.register }
func (h *hub) UnregisterPipe() chan<- *Client { return h.unregister }
func (h *hub) SubscribePipe() chan<- SubscriptionRequest {
	return h.subscribe
}
func (h *hub) UnsubPipe() chan<- SubscriptionRequest {
	return h.unsubscribe
}

// DataAvailabilityConsumer interface implementation
func (h *hub) DataReadyPipe() chan<- DataReadyMsg { return h.dataReadyChan }

// TickerThreadAPI interface implementation
func (h *hub) BroadcastMessagePipe() chan<- BroadcastMessage { return h.broadcast }

// Hub interface implementation
func (h *hub) SignalShutdownPipe() chan<- struct{} { return h.shutdownChan }

// CORE LOOP
func (h *hub) Run() {
	for {
		select {
		case <-h.shutdownChan:
			h.shutdown()
			return

		// Handle incoming broadcast message from ticker threads
		case msg := <-h.broadcast:
			h.handleBroadcast(msg)

		// Handle Client's request to subscribe
		case subReq := <-h.subscribe:
			h.handleSub(subReq)

		// Handle Client's request to unsubscribe
		case subReq := <-h.unsubscribe:
			h.handleUnsub(subReq)

		// Client registration
		case client := <-h.register:
			h.handleRegister(client)

		// Client unregistration
		case client := <-h.unregister:
			h.handleUnregister(client)
		}
	}
}

func (h *hub) shutdown() {
	// Shutdown child ticker threads
	for _, tickerThread := range h.ownedTickerThreads {
		tickerThread.Shutdown <- struct{}{}
	}
	// Trigger shutdown of clients
	for client := range maps.Keys(h.clients) {
		close(client.Shutdown)
		delete(h.clients, client)
	}
}

func (h *hub) handleBroadcast(msg BroadcastMessage) {
	// Fan-out msg to subscribed clients
	for client := range h.tickerToClient[msg.Ticker] {
		client.Outbox <- msg.Data
	}
}

func (h *hub) handleSub(subReq SubscriptionRequest) {
	for _, ticker := range subReq.Tickers {

		if _, ok := h.ownedTickerThreads[ticker]; !ok {
			fmt.Printf("First subscriber for %s. Starting ticker thread.\n", ticker)
			h.startTickerThread(ticker)
		}

		if h.tickerToClient[ticker] == nil {
			h.tickerToClient[ticker] = make(map[*Client]struct{})
		}

		if h.clientToSubbedTickers[subReq.Client] == nil {
			h.clientToSubbedTickers[subReq.Client] = make(map[Ticker]struct{})
		}

		if _, ok := h.tickerToClient[ticker][subReq.Client]; ok {
			fmt.Printf("Client already subscribed to %s. Ignoring subscription request.", ticker)
			continue
		}

		h.tickerToClient[ticker][subReq.Client] = struct{}{}
		h.clientToSubbedTickers[subReq.Client][ticker] = struct{}{}

		fmt.Printf("Client subscribed to %s, total %d\n", ticker, len(h.tickerToClient[ticker]))
	}
}

func (h *hub) handleUnsub(subReq SubscriptionRequest) {
	for _, ticker := range subReq.Tickers {
		if clients, ok := h.tickerToClient[ticker]; ok {
			delete(clients, subReq.Client)

			if len(clients) == 0 {
				fmt.Printf("Last subscriber left for %s. Killing ticker thread.\n", ticker)
				h.killTickerThread(ticker)
				delete(h.tickerToClient, ticker)
			}

			fmt.Printf("Client unsubscribed from %s, total %d\n", ticker, len(h.tickerToClient[ticker]))

		}
	}
}

func (h *hub) handleRegister(c *Client) {
	h.clients[c] = struct{}{}
	fmt.Printf("A client has been registered, total: %d\n", len(h.clients))
}

func (h *hub) handleUnregister(c *Client) {
	if tickers, ok := h.clientToSubbedTickers[c]; ok && len(tickers) > 0 {
		h.handleUnsub(
			SubscriptionRequest{
				Client:  c,
				Tickers: slices.Collect(maps.Keys(tickers)),
			},
		)
	}

	delete(h.clients, c)

	fmt.Printf("A client has been unregistered, total: %d\n", len(h.clients))
}

func (h *hub) startTickerThread(ticker Ticker) {
	tickerOwner := TickerThreadOwner{
		broadcast: h.broadcast,
		Shutdown:  make(chan struct{}),
		Ticker:    ticker,
	}

	go tickerOwner.RunThread()

	h.ownedTickerThreads[ticker] = &tickerOwner
}

func (h *hub) killTickerThread(ticker Ticker) {
	thread := h.ownedTickerThreads[ticker]
	thread.Shutdown <- struct{}{}
	delete(h.ownedTickerThreads, ticker)
}
