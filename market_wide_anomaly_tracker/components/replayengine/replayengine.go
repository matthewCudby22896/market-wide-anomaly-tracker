package replayengine

import (
	"context"
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/coder/websocket"
	"github.com/matthewCudby22896/market_wide_anomaly_tracker/components/replayengine/db"
)

type ReplayEnginerServer interface {
	LifeCycle
}

type replayEngineServer struct {
	Server *http.Server
	wg     sync.WaitGroup
	logger ComponentLogger

	// Children
	Hub Hub
}

func NewReplayEnginerServer() *replayEngineServer {
	// Created once at this top level, and then passed down
	// the component tree
	database := db.RequireNewDatabase(DB_URL)

	// Apply migrations
	database.RequireApplyMigrations()

	mux := http.NewServeMux()

	server := &http.Server{
		Addr:    replayEnginerServerSocket,
		Handler: mux,
	}

	ret := &replayEngineServer{
		Server: server,
		wg:     sync.WaitGroup{},
		logger: NewLogger("ReplayEnginerServer"),
		Hub:    NewHub(database),
	}

	mux.HandleFunc("/ws", ret.handleConnection)

	return ret
}

func (s *replayEngineServer) Start() {
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		// Start child components
		s.logger.LogStartChild("Hub")
		s.Hub.Start()

		// Start listening
		s.logger.Info("listening on %s", s.Server.Addr)
		err := s.Server.ListenAndServe()
		if err != nil {
			s.logger.Info("ListenAndServe err: %v", err)
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

	s.logger.LogShutdownChild("Hub")
	s.Hub.Shutdown()

	s.wg.Wait()
	s.logger.LogShutdown()

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
