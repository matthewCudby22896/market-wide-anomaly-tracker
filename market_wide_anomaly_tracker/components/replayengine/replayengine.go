package replayengine

import (
	"context"
	"fmt"
	"log"
	"net/http"

	"github.com/coder/websocket"
	"github.com/coder/websocket/wsjson"
)

type Server struct {
	Hub *Hub
}

type Hub struct {
	// For registering a Client with the Hub
	register chan *Client
	// For unregistering a Client from the Hub
	unregister chan *Client
	// Used to trigger graceful shutdown of the Hub
	stop chan struct{}
	// Essentially a Set of the currently `registered` clients
	clients map[*Client]bool
}

type Client struct {
	Ctx        context.Context
	CancelCtx  context.CancelFunc
	Connection *websocket.Conn
	Hub        *Hub
	Outbox     chan any      // For sending
	Shutdown   chan struct{} // Triggered by Hub when shutting down the server
}

func NewHub() *Hub {
	return &Hub{
		register:   make(chan *Client),
		unregister: make(chan *Client),
		stop:       make(chan struct{}),
		clients:    make(map[*Client]bool),
	}
}

func (h *Hub) Run() {
	for {
		select {
		case client := <-h.register: // For when a client has opened a connection
			h.clients[client] = true

			fmt.Printf("A client has been registered, total: %d\n", len(h.clients))

		case client := <-h.unregister: // For when a client closes the connection
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

	// Start SENDER go routine (shutdown on context closure)
	go c.SenderThread()

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
			wsjson.Write(c.Ctx, c.Connection, msg)
		}
	}
}

// Exits on either:
//   - Cancellation of ctx
func (c *Client) ListenerThread() {
	for {
		var v any
		fmt.Println("Listening...")
		err := wsjson.Read(c.Ctx, c.Connection, &v)
		if err != nil {
			fmt.Println("Reader error/disconnect:", err)
			break
		}
		fmt.Printf("Received: %v\n", v)

		// TODO: Remove
		// Test: Send something back
		c.Outbox <- map[string]string{"echo": "got it"}
	}
}

func (s *Server) handleConnection(w http.ResponseWriter, r *http.Request) {
	c, err := websocket.Accept(w, r, nil)
	if err != nil {
		return
	}
	defer c.CloseNow()

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
