package replayengine

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"time"

	"github.com/coder/websocket"
	"github.com/coder/websocket/wsjson"
)

type Server struct {
	Hub *Hub
}

type Ticker string

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
}

type Client struct {
	Ctx        context.Context
	CancelCtx  context.CancelFunc
	Connection *websocket.Conn
	Hub        *Hub
	Outbox     chan any      // For sending
	Shutdown   chan struct{} // Triggered by Hub when shutting down the server
}

type SubscriptionRequest struct {
	Client  *Client
	Tickers []Ticker
}

type TickerThreadOwner struct {
	broadcast chan<- BroadcastMessage
	Shutdown  chan struct{}
	Ticker    Ticker
}

type AggregateBar struct {
	Event   string  `json:"ev"` // Event Type (e.g., "AM")
	Symbol  string  `json:"sym"`
	Volume  int     `json:"v"`
	Open    float64 `json:"o"`
	Close   float64 `json:"c"`
	High    float64 `json:"h"`
	Low     float64 `json:"l"`
	VWAP    float64 `json:"a"` // Volume Weighted Average Price
	StartMS int64   `json:"s"` // Starting Unix Epoch (milliseconds)
	EndMS   int64   `json:"e"` // Ending Unix Epoch (milliseconds)
}

var DummyBar = AggregateBar{
	Event:   "AM",
	Symbol:  "____",
	Volume:  12345,
	Open:    150.85,
	High:    153.17,
	Low:     150.50,
	Close:   152.90,
	VWAP:    151.87,
	StartMS: 1611082800000,
	EndMS:   1611082860000,
}

func (t *TickerThreadOwner) RunThread() {
	// 1. Initialize the ticker for 1-second intervals
	ticker := time.NewTicker(1 * time.Second)

	// 2. Always clean up the ticker when the function exits
	defer ticker.Stop()

	for {
		select {
		case <-t.Shutdown:
			// Exit immediately when signaled
			return

		case <-ticker.C:
			// 3. This block only triggers once every second
			dummyMsg := BroadcastMessage{
				Ticker: t.Ticker,
				Data:   DummyBar,
			}

			// Send to the broadcast channel
			// (Consider using a non-blocking send here if you have many listeners)
			t.broadcast <- dummyMsg
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
	// Get's the ticker thread via the map, triggers shutdown
	thread := h.ownedTickerThreads[ticker]

	// Do I need to wait for confirmation of shutdown?? not sure
	thread.Shutdown <- struct{}{}

	// Dels from map
	delete(h.ownedTickerThreads, ticker)
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
		clients:            make(map[*Client]bool),
		tickerToClient:     make(map[Ticker]map[*Client]struct{}),
		ownedTickerThreads: make(map[Ticker]*TickerThreadOwner),
	}
}

func (h *Hub) Run() {
	for {
		select {
		case broadcastMessage := <-h.broadcast:
			// TODO: May need to check 
			for client := range h.tickerToClient[broadcastMessage.Ticker] {
				client.Outbox <- broadcastMessage.Data
			}

		case subReq := <-h.subscribe:
			// TODO: Move to method
			for _, ticker := range subReq.Tickers {

				if _, ok := h.ownedTickerThreads[ticker]; !ok {
					h.StartTickerThread(ticker)
				}

				if h.tickerToClient[ticker] == nil {
					h.tickerToClient[ticker] = make(map[*Client]struct{})
				}

				h.tickerToClient[ticker][subReq.Client] = struct{}{}
			}

		case subReq := <-h.unsubscribe:
			// TODO: Handle method
			for _, ticker := range subReq.Tickers {
				if clients, ok := h.tickerToClient[ticker]; ok {
					delete(clients, subReq.Client)

					if len(clients) == 0 {
						fmt.Printf("Last subscriber left for %s. Cleaning up...\n", ticker)
						h.KillTickerThread(ticker)
						delete(h.tickerToClient, ticker)
					}
				}
			}

		case client := <-h.register: // For when a client has opened a connection
			h.clients[client] = true

			fmt.Printf("A client has been registered, total: %d\n", len(h.clients))

		case client := <-h.unregister: // For when a client closes the connection
			// TODO: Unsub the client from all of the tickers
			// NOTE: Do this HERE, do not sent messages to the unsub pipe

			// THEN: Delete
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

func (c *Client) StartClient() {
	// Send the Hub a reference of itself to register
	c.Hub.register <- c

	// Unregister the client when the connection closes
	defer c.UnregisterFromHub()

	// Start go routine to listen for shutdown (shutdown on context closure)
	go c.ListenForShutdown()

	// Start SENDER go routine
	go c.SenderThread()

	// Start LISTENER go routine
	go c.ListenerThread()

	<-c.Ctx.Done()
}

func (c *Client) UnregisterFromHub() {
	select {
	// If Hub is listening send reference of self to unregister
	case c.Hub.unregister <- c:
	case <-c.Shutdown:
	}
}

// Exits on either:
//   - Cancellation of ctx from client closing connection
//   - Shutdown signal sent from parent Hub -> triggering ctx cancellation
func (c *Client) ListenForShutdown() {
	select {
	case <-c.Shutdown:
		// The hub has signalled for the client connection to be shutdown
		c.CancelCtx()

	case <-c.Ctx.Done():
		// The user disconnected normally
		return
	}
}

// Exits on either:
//   - Cancellation of ctx
func (c *Client) SenderThread() {
	for {
		select {
		case <-c.Ctx.Done():
			return
		case msg := <-c.Outbox:
			err := wsjson.Write(c.Ctx, c.Connection, msg)
			if err != nil {
				fmt.Println(err)
			}
		}
	}
}

type Message struct {
	Action  string   `json:"action"`
	Tickers []string `json:"tickers"`
}

// Exits on either:
//   - Cancellation of ctx
func (c *Client) ListenerThread() {
	for {
		var v Message
		err := wsjson.Read(c.Ctx, c.Connection, &v)
		if err != nil {
			fmt.Println("Reader error/disconnect:", err)
			c.CancelCtx()
			return
		}

		fmt.Printf("Received:\n %#v\n", v)

		switch v.Action {
		case "subscribe":
			c.HandleSub(v.Tickers)

		case "unsubscribe":
			c.HandleUnsub(v.Tickers)

		default:
			fmt.Println("Unrecognised `action` field : ", v.Action)
		}
	}
}

// TODO: Commonise code, and provide refs to Hub channels rather than the Hub itself
func (c *Client) HandleSub(tickers []string) {
	typedTickers := make([]Ticker, 0, len(tickers))
	for _, t := range tickers {
		typedTickers = append(typedTickers, Ticker(t))
	}
	req := SubscriptionRequest{
		Client:  c,
		Tickers: typedTickers,
	}
	fmt.Println("Sending sub req")
	c.Hub.subscribe <- req
}

func (c *Client) HandleUnsub(tickers []string) {
	typedTickers := make([]Ticker, 0, len(tickers))
	for _, t := range tickers {
		typedTickers = append(typedTickers, Ticker(t))
	}
	req := SubscriptionRequest{
		Client:  c,
		Tickers: typedTickers,
	}
	c.Hub.unsubscribe <- req
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
