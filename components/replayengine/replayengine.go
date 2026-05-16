package replayengine

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/coder/websocket"
	"github.com/mcudby/mwat/components/replayengine/db"
	"github.com/mcudby/mwat/components/replayengine/hub"
	"github.com/mcudby/mwat/components/replayengine/logging"
	"github.com/mcudby/mwat/components/replayengine/wsclient"
)

const replayEngineServerID = "replay-engine-server"

type replayEngineServer struct {
	id     string
	wg     sync.WaitGroup
	logger *logging.Logger
	Server *http.Server
	Hub    Hub
}

type Opts struct {
	DatabaseURL string
	Port        string
}

func NewReplayEngineServer(opts Opts) *replayEngineServer {
	// Created once at this top level, and then passed down
	// the component tree
	database := db.EstablishDBConnection(
		context.Background(),
		fmtDBUrl(opts.DatabaseURL),
	)

	database.RequireApplyMigrations()

	mux := http.NewServeMux()

	server := &http.Server{
		Addr:    ":" + opts.Port,
		Handler: mux,
	}

	srv := &replayEngineServer{
		id:     replayEngineServerID,
		Server: server,
		wg:     sync.WaitGroup{},
		logger: logging.NewComponentLogger("replay-engine-server"),
		Hub:    hub.NewHub(database),
	}

	// WebSocket
	mux.HandleFunc("/ws", srv.handleConnection)

	// Control Plane
	mux.HandleFunc("/simulation/pause", srv.handlePause)
	mux.HandleFunc("/simulation/resume", srv.handleResume)
	mux.HandleFunc("/simulation/restart", srv.handleRestart)
	mux.HandleFunc("/control/settings", srv.handleSettings)
	mux.HandleFunc("/control/hydrate", srv.handleHydrate)
	mux.HandleFunc("/ping", srv.handlePing)

	return srv
}

func (s *replayEngineServer) Start() {
	s.logger.LogStartChild(s.Hub.GetID())
	s.Hub.Start()

	s.wg.Add(1)
	go func() {
		defer s.wg.Done()

		// Start listening
		msg := fmt.Sprintf("listening on %s", s.Server.Addr)
		s.logger.Info(msg)
		err := s.Server.ListenAndServe()
		if err != nil {
			if errors.Is(err, http.ErrServerClosed) {
				s.logger.Info("http server closed.")
				return
			}
			s.logger.Error("ListenAndServe() errored", "error", err)
			return
		}
	}()
}

func (s *replayEngineServer) Shutdown() {
	// Stop serving new connections
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()

		s.logger.Info("shutting down http server")
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		if err := s.Server.Shutdown(ctx); err != nil {
			fmt.Printf("HTTP shutdown error: %v\n", err)
		}
	}()

	// Wait for the Hub to Stop
	s.logger.LogStopChild(s.Hub.GetID())
	s.Hub.Shutdown()

	s.wg.Wait()
	s.logger.LogShutdown()

}

func (s *replayEngineServer) handleConnection(w http.ResponseWriter, r *http.Request) {
	// 1. Accept and upgrade the connection
	c, err := websocket.Accept(w, r, nil)
	if err != nil {
		s.logger.Fatal("failed to upgrade connection", "error", err)
		return
	}

	// 2. Create a new client instance
	client := wsclient.NewClient(c, s.Hub.GetInbox())

	// 3. Register it with the Hub, the Hub will handle its lifecycle
	s.Hub.RegisterClient(client)

	// Note: The hub handles the Start() of the client once it has succesfully registered
}
