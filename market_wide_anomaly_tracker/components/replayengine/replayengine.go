package replayengine

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"sync"
	"time"

	"github.com/coder/websocket"
)

type ReplayEnginerServer interface {
	LifeCycle
}

type replayEngineServer struct {
	Server *http.Server

	// Children
	Hub Hub
}

func NewReplayEnginerServer() *replayEngineServer {
	// 1. Init the multiplexer
	mux := http.NewServeMux()
	mux.HandleFunc("/ws", s.handleConnection)

	// 2. Init the http server
	server := &http.Server{
		Addr:    replayEnginerServerSocket,
		Handler: mux,
	}

	// 3. Init the replayEngineServer
	return &replayEngineServer{
		Server: server,
		Hub:    NewHub(),
	}
}

func (s *replayEngineServer) Start() {
	// Start child components
	fmt.Printf("[ReplayEnginerServer] starting hub...\n")
	s.Hub.Start()

	// Start listening
	fmt.Printf("[ReplayEnginerServer] listening on %s\n", s.Server.Addr)
	err := s.server.ListenAndServe()
	if err != nil {
		log.Fatalf("ListenAndServe: %s", err)
		s.Shutdown()
		return
	}
}

func (s *replayEngineServer) Shutdown() {
	var wg sync.WaitGroup
	// 1. Shutdown
	wg.Add(1)
	go func() {
		defer wg.Done()
		fmt.Printf("[ReplayEnginerServer] Shutting down http server...\n")
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second) //TODO: magic num
		defer cancel()

		if err := s.server.Shutdown(ctx); err != nil {
			fmt.Printf("HTTP shutdown error: %v\n", err)
		}
	}()

	fmt.Printf("[ReplayEnginerServer] Shutting down Hub...\n")
	s.Hub.Shutdown()

	wg.Wait()
	fmt.Printf("[ReplayEnginerServer] All systems halted\n")
}

// Will launch in new go thread
func (s *replayEngineServer) handleConnection(w http.ResponseWriter, r *http.Request) {
	// Accept and upgrade the connection
	c, err := websocket.Accept(w, r, nil)
	if err != nil {
		// TODO: Log
		return
	}
	
	// Create a new client instance
	client := &client{
		Connection:   c,
		ClientSignallingAPI: s.Hub,
		Outbox:       make(chan any, 10),
		Shutdown:     make(chan struct{}),
		subscribe:    s.Hub.subscribe,
		unsubscribe:  s.Hub.unsubscribe,
	}

	// Hand it off to the Hub, which controls it's lifecycle.



	client.StartClient()
}

func (s *ReplayEngineServer) Shutdown() {
	fmt.Printf("\nReplayEngineServer shutting down...\n")
	close(s.Hub.shutdownChan)

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
	return Start()
}
