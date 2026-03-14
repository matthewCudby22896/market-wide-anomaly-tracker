package replayengine

import (
	"context"
	"fmt"
	"log"
	"maps"
	"net/http"
	"slices"
	"time"

	"github.com/coder/websocket"
)

type ReplayEngineServer struct {
	Hub      *Hub
	shutdown chan struct{}
	server   *http.Server
}

func initServer() *ReplayEngineServer {
	mux := http.NewServeMux()

	// Setup
	s := &ReplayEngineServer{
		Hub:      NewHub(),
		shutdown: make(chan struct{}),
	}
	mux.HandleFunc("/ws", s.handleConnection)

	s.server = &http.Server{
		Addr:    ":8080",
		Handler: mux,
	}

	// Start the Hub
	go s.Hub.Run()

	// Start the HTTP Listener thread
	go s.HTTPListen()

	return s
}

func (s *ReplayEngineServer) Shutdown() {
	fmt.Printf("\nReplayEngineServer shutting down...\n")
	close(s.Hub.Shutdown)

    ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
    defer cancel()

    if err := s.server.Shutdown(ctx); err != nil {
        fmt.Printf("HTTP shutdown error: %v\n", err)
    }
}

func (s *ReplayEngineServer) HTTPListen() {
	fmt.Printf("ReplayEnginerServer listening on %s\n", s.server.Addr)
	err := s.server.ListenAndServe()
	if err != nil {
		log.Fatalf("ListenAndServe: %s", err)
		s.Shutdown()
		return
	}
}

type Hub struct {
	// Can be triggered by parent Server
	Shutdown chan struct{}
	// For registering a Client with the Hub
	register chan *Client
	// For unregistering a Client from the Hub
	unregister chan *Client
	// For subscribing to ticker data streams
	subscribe chan SubscriptionRequest
	// For unsuscribing to ticker data streams
	unsubscribe chan SubscriptionRequest

	// For TickerThreadOwners to broadcast price updates to, the Hub handles fanning the msgs out to the correct clients
	broadcast chan BroadcastMessage

	// Essentially a Set of the currently `registered` clients
	clients map[*Client]bool
	// e.g. "APPL" to slice of Client's subscribed to APPL
	tickerToClient map[Ticker]map[*Client]struct{}
	// For the hub to maintain a reference to each of it's owned TickerThreads
	ownedTickerThreads map[Ticker]*TickerThreadOwner
	// Used when a client has disconnected and the Hub needs to unsub the client from all it's subbed tickers
	clientToSubbedTickers map[*Client]map[Ticker]struct{}
}

func NewHub() *Hub {
	return &Hub{
		// Channels: Use unbuffered for control flow,
		// but buffered for data (broadcast) to prevent blocking.
		register:    make(chan *Client),
		unregister:  make(chan *Client),
		subscribe:   make(chan SubscriptionRequest),
		unsubscribe: make(chan SubscriptionRequest),
		broadcast:   make(chan BroadcastMessage, 1024), // Buffer high-volume data
		Shutdown:    make(chan struct{}),

		// Maps: Must be initialized via make() or they will panic on first use.
		clients:               make(map[*Client]bool),
		tickerToClient:        make(map[Ticker]map[*Client]struct{}),
		ownedTickerThreads:    make(map[Ticker]*TickerThreadOwner),
		clientToSubbedTickers: make(map[*Client]map[Ticker]struct{}),
	}
}

func (h *Hub) Run() {
	for {
		select {
		case <-h.Shutdown:
			// Shutdown child ticker threads
			for _, tickerThread := range h.ownedTickerThreads {
				tickerThread.Shutdown <- struct{}{}
			}
			// Trigger shutdown of clients
			for client := range maps.Keys(h.clients) {
				close(client.Shutdown)
				delete(h.clients, client)
			}

			return

		// Handle incoming broadcast message from ticker threads
		case broadcastMessage := <-h.broadcast:
			for client := range h.tickerToClient[broadcastMessage.Ticker] {
				client.Outbox <- broadcastMessage.Data
			}

		// Handle Client's request to subscribe
		case subReq := <-h.subscribe:
			h.handleSub(subReq)

		// Handle Client's request to unsubscribe
		case subReq := <-h.unsubscribe:
			h.handleUnsub(subReq)

		// Client registration
		case client := <-h.register:
			h.clients[client] = true

			fmt.Printf("A client has been registered, total: %d\n", len(h.clients))

		// Client unregistration
		case client := <-h.unregister:

			if tickers, ok := h.clientToSubbedTickers[client]; ok && len(tickers) > 0 {
				h.handleUnsub(
					SubscriptionRequest{
						Client:  client,
						Tickers: slices.Collect(maps.Keys(tickers)),
					},
				)
			}

			delete(h.clients, client)

			fmt.Printf("A client has been unregistered, total: %d\n", len(h.clients))
		}
	}
}

func (h *Hub) handleSub(subReq SubscriptionRequest) {
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

func (h *Hub) handleUnsub(subReq SubscriptionRequest) {
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

func (h *Hub) startTickerThread(ticker Ticker) {
	tickerOwner := TickerThreadOwner{
		broadcast: h.broadcast,
		Shutdown:  make(chan struct{}),
		Ticker:    ticker,
	}

	go tickerOwner.RunThread()

	h.ownedTickerThreads[ticker] = &tickerOwner
}

func (h *Hub) killTickerThread(ticker Ticker) {
	thread := h.ownedTickerThreads[ticker]
	thread.Shutdown <- struct{}{}
	delete(h.ownedTickerThreads, ticker)
}

func (s *ReplayEngineServer) handleConnection(w http.ResponseWriter, r *http.Request) {
	c, err := websocket.Accept(w, r, nil)
	if err != nil {
		return
	}
	defer func() {
		_ = c.CloseNow()
	}()

	ctx, cancel := context.WithCancel(r.Context())
	client := &Client{
		Ctx:         ctx,
		CancelCtx:   cancel,
		Connection:  c,
		Hub:         s.Hub,
		Outbox:      make(chan any, 10),
		Shutdown:    make(chan struct{}),
		subscribe:   s.Hub.subscribe,
		unsubscribe: s.Hub.unsubscribe,
	}

	client.StartClient()
}

func LaunchServer() *ReplayEngineServer {
	return initServer()
}
