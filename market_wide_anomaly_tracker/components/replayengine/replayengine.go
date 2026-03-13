package replayengine

import (
	"context"
	"fmt"
	"log"
	"maps"
	"net/http"
	"slices"

	"github.com/coder/websocket"
)

type Server struct {
	Hub *Hub
}

type BroadcastMessage struct {
	Ticker Ticker
	Data   any
}

type Hub struct {
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

	// Used to trigger graceful shutdown of the Hub
	stop chan struct{}
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
		stop:        make(chan struct{}),

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
		case broadcastMessage := <-h.broadcast:
			for client := range h.tickerToClient[broadcastMessage.Ticker] {
				client.Outbox <- broadcastMessage.Data
			}

		case subReq := <-h.subscribe:
			fmt.Println("Sub")
			h.handleSub(subReq)

		case subReq := <-h.unsubscribe:
			h.handleUnsub(subReq)

		case client := <-h.register:
			h.clients[client] = true

			fmt.Printf("A client has been registered, total: %d\n", len(h.clients))

		case client := <-h.unregister: // For when a client closes the connection

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

		case <-h.stop: // For when the Hub has been called to shutdown

			// Shutdown all server-client connections, before returning itself
			for client := range h.clients {
				close(client.Shutdown)
				delete(h.clients, client)
			}
			return
		}
	}
}

func (h *Hub) handleSub(subReq SubscriptionRequest) {
	for _, ticker := range subReq.Tickers {

		if _, ok := h.ownedTickerThreads[ticker]; !ok {
			fmt.Printf("First subscriber for %s. Starting ticker thread.\n", ticker)
			h.StartTickerThread(ticker)
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
				h.KillTickerThread(ticker)
				delete(h.tickerToClient, ticker)
			}

			fmt.Printf("Client unsubscribed from %s, total %d\n", ticker, len(h.tickerToClient[ticker]))

		}
	}
}

func (h *Hub) StartTickerThread(ticker Ticker) {
	tickerOwner := TickerThreadOwner{
		broadcast: h.broadcast,
		Shutdown:  make(chan struct{}),
		Ticker:    ticker,
	}

	go tickerOwner.RunThread()

	h.ownedTickerThreads[ticker] = &tickerOwner
}

func (h *Hub) KillTickerThread(ticker Ticker) {
	thread := h.ownedTickerThreads[ticker]
	thread.Shutdown <- struct{}{}
	delete(h.ownedTickerThreads, ticker)
}

func (s *Server) handleConnection(w http.ResponseWriter, r *http.Request) {
	c, err := websocket.Accept(w, r, nil)
	if err != nil {
		return
	}
	defer func() {
		_ = c.CloseNow()
	}()

	ctx, cancel := context.WithCancel(r.Context())
	client := &Client{
		Ctx:        ctx,
		CancelCtx:  cancel,
		Connection: c,
		Hub:        s.Hub,
		Outbox:     make(chan any, 10),
		Shutdown:   make(chan struct{}),
		subscribe: s.Hub.subscribe,
		unsubscribe: s.Hub.unsubscribe,
	}

	client.StartClient()
}
func runWebSocketServer() {
	// Init & Start Hub
	hub := NewHub()
	go hub.Run()

	// Init Server
	server := &Server{Hub: hub}

	http.HandleFunc("/ws", server.handleConnection)
	fmt.Printf("WebSocket Server listening on %s\n", replayEnginerServerSocket)

	// blocking
	err := http.ListenAndServe(replayEnginerServerSocket, nil)
	if err != nil {
		log.Fatal("ListenAndServe: ", err)
	}
}

func LaunchServer() {
	runWebSocketServer()
}
