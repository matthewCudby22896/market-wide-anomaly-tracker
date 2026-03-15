package replayengine

import (
	"context"
	"fmt"
	"maps"
	"slices"
	"sync"
)


type HubReqType int

const (
	REGISTER HubReqType = iota
	UNREGISTER 
	SUB	
	UNSUB
)

type hubRequest struct {
	Type HubReqType
	Client  *client
	Tickers []Ticker
}

// Hub functionality exposed to dataCoordinator
type DataAvailabilityConsumer interface {
	DataReadyPipe() chan<- DataReadyMsg
}

type HubClientInterface interface {
	SubscribePipe() chan<- SubscriptionRequest
	UnsubPipe() chan<- SubscriptionRequest
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
	UnregisterClient(c *client)
	SubscribePipe() chan<- SubscriptionRequest
	UnsubPipe() chan<- SubscriptionRequest
}

// hub implements the Hub interface
type hub struct {
	// Rework 
	Ctx context.Context
	CancelCtx context.CancelFunc
	wg sync.WaitGroup

	requestsChan chan hubRequest
	// End Rework
	
	register    chan *Client
	unregister  chan *Client
	subscribe   chan SubscriptionRequest
	unsubscribe chan SubscriptionRequest

	broadcast chan BroadcastMessage

	// Essentially a Set of the currently `registered` clients
	clients map[*client]struct{}
	// e.g. "APPL" to slice of Client's subscribed to APPL
	tickerToClient map[Ticker]map[*client]struct{}
	// For the hub to maintain a reference to each of it's owned TickerThreads
	ownedTickerThreads map[Ticker]*TickerThread
	// Used when a client has disconnected and the Hub needs to unsub the client from all it's subbed tickers
	clientToSubbedTickers map[*client]map[Ticker]struct{}

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
		clients:               make(map[*client]struct{}),
		tickerToClient:        make(map[Ticker]map[*Client]struct{}),
		ownedTickerThreads:    make(map[Ticker]*TickerThread),
		clientToSubbedTickers: make(map[*Client]map[Ticker]struct{}),
	}
}

func (h *hub) Shutdown() {	
	// First shutdown all child components (client, ticker threads, data controller)

	// Then shutdown itself
	h.CancelCtx()
}

func (h *hub) Start() {
	h.wg.Add(1)
	go func() {
		defer h.wg.Done()
		for {
			select {
			case <- h.Ctx.Done():
				return

			case msg := <- h.broadcast:
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
			}
		}
	}()
}


func (h *hub) Start() {
	for {
		select {
		case <-h.Ctx.Done():
			return
		
		// Handle incoming broadcast message from ticker threads
		case msg := <-h.broadcast:
			h.handleBroadcast(msg)

		// TODO: Shift all 4 chans into one (for determinism)
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

// BroadcastIngester interface implementation
func (h *hub) BroadcastMessagePipe() chan<- BroadcastMessage { return h.broadcast }

// Hub interface implementation
// func (h *hub) SignalShutdownPipe() chan<- struct{} { return h.shutdownChan }


// func (h *hub) Shutdown() {
// 	// Shutdown child ticker threads
// 	for _, tickerThread := range h.ownedTickerThreads {
// 		tickerThread.Shutdown <- struct{}{}
// 	}
// }

func (h *hub) handleBroadcast(msg BroadcastMessage) {
	// Fan-out msg to subscribed clients
	for client := range h.tickerToClient[msg.Ticker] {
		client.Outbox() <- msg.Data
	}
}

func (h *hub) handleSub(c *client, tickers []Ticker) {
	for _, ticker := range tickers {

		if _, ok := h.ownedTickerThreads[ticker]; !ok {
			fmt.Printf("First subscriber for %s. Starting ticker thread.\n", ticker)
			h.startTickerThread(ticker)
		}

		if h.tickerToClient[ticker] == nil {
			h.tickerToClient[ticker] = make(map[*client]struct{})
		}

		if h.clientToSubbedTickers[c] == nil {
			h.clientToSubbedTickers[c] = make(map[Ticker]struct{})
		}

		if _, ok := h.tickerToClient[ticker][c]; ok {
			fmt.Printf("Client already subscribed to %s. Ignoring subscription request.", ticker)
			continue
		}

		h.tickerToClient[ticker][c] = struct{}{}
		h.clientToSubbedTickers[c][ticker] = struct{}{}

		fmt.Printf("Client subscribed to %s, total %d\n", ticker, len(h.tickerToClient[ticker]))
	}
}

func (h *hub) handleUnsub(c *client, tickers []Ticker) {
	for _, ticker := range tickers {
		if clients, ok := h.tickerToClient[ticker]; ok {
			delete(clients, c)

			if len(clients) == 0 {
				fmt.Printf("Last subscriber left for %s. Killing ticker thread.\n", ticker)
				h.killTickerThread(ticker)
				delete(h.tickerToClient, ticker)
			}

			fmt.Printf("Client unsubscribed from %s, total %d\n", ticker, len(h.tickerToClient[ticker]))
		}
	}
}

func (h *hub) handleRegister(c *client) {
	h.clients[c] = struct{}{}
	fmt.Printf("A client has been registered, total: %d\n", len(h.clients))
}

func (h *hub) handleUnregister(c *client) {
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
	tickerOwner := TickerThread{
		ingester: h,
		Shutdown: make(chan struct{}),
		Ticker:   ticker,
	}

	go tickerOwner.RunThread()

	h.ownedTickerThreads[ticker] = &tickerOwner
}

func (h *hub) killTickerThread(ticker Ticker) {
	thread := h.ownedTickerThreads[ticker]
	thread.Shutdown <- struct{}{}
	delete(h.ownedTickerThreads, ticker)
}
