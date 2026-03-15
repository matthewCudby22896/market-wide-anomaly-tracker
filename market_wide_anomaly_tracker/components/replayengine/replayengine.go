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

	// 2. Init the http server
	server := &http.Server{
		Addr:    replayEnginerServerSocket,
		Handler: mux,
	}

	// 3. Init the replayEngineServer
	ret := &replayEngineServer{
		Server: server,
		Hub:    NewHub(),
	}

	// 4. Assing handler for /ws endpoint
	mux.HandleFunc("/ws", ret.handleConnection)

	return ret
}

func (s *replayEngineServer) Start() {
	// Start child components
	fmt.Printf("[ReplayEnginerServer] starting hub...\n")
	s.Hub.Start()

	// Start listening
	fmt.Printf("[ReplayEnginerServer] listening on %s\n", s.Server.Addr)
	err := s.Server.ListenAndServe()
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

		if err := s.Server.Shutdown(ctx); err != nil {
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
	// 1. Accept and upgrade the connection
	c, err := websocket.Accept(w, r, nil)
	if err != nil {
		// TODO: Log
		return
	}
	
	// 2. Create a new client instance
	client := NewClient(c, s.Hub)	

	// 3. Register it with the Hub, the Hub will handle its lifecycle
	s.Hub.RegisterClient(client)

	// 4. Start the client up (i.e. listener / sender)
	client.Start()
}


