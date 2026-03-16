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
	wg     sync.WaitGroup

	// Children
	Hub Hub

	logger ComponentLogger
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
		wg:     sync.WaitGroup{},
		Hub:    NewHub(),
		logger: NewLogger("ReplayEnginerServer"),
	}

	// 4. Assing handler for /ws endpoint
	mux.HandleFunc("/ws", ret.handleConnection)

	return ret
}

func (s *replayEngineServer) Start() {
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		// Start child components
		s.Hub.Start()

		// Start listening
		s.logger.Info("listening on %s", s.Server.Addr)
		err := s.Server.ListenAndServe()
		if err != nil {
			log.Fatalf("ListenAndServe: %s", err)
			s.Shutdown()
			return
		}
	}()
}

func (s *replayEngineServer) Shutdown() {
	// 1. Stop serving new connections
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()

		s.logger.Info("Shutting down http server...")
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second) //TODO: magic num
		defer cancel()

		if err := s.Server.Shutdown(ctx); err != nil {
			fmt.Printf("HTTP shutdown error: %v\n", err)
		}
	}()

	s.Hub.Shutdown()

	s.wg.Wait()
	s.logger.Info("Shutdown")

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
