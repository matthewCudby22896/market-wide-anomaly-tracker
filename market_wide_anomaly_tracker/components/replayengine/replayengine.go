package replayengine

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"time"

	"github.com/coder/websocket"
)

type ReplayEngineServer struct {
	Hub      *hub
	shutdown chan struct{}
	server   *http.Server
}

// INIT METHOD
func NewServer() *ReplayEngineServer {
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

func (s *ReplayEngineServer) Shutdown() {
	fmt.Printf("\nReplayEngineServer shutting down...\n")
	close(s.Hub.shutdown)

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

func LaunchServer() *ReplayEngineServer {
	return NewServer()
}
