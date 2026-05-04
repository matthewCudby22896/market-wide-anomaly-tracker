package main

import (
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	_ "time/tzdata"

	replayengine "github.com/mcudby/mwat/components/replayengine"
)

func main() {
	databaseURL := flag.String("db-url", "localhost:6543", "url of the timescaledb instance")
	port := flag.String("port", "8080", "port replay engine listents on")

	flag.Parse()

	opts := replayengine.Opts{
		DatabaseURL: *databaseURL,
		Port: *port,
	}

	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)

	s := replayengine.NewReplayEngineServer(opts)
	s.Start()
	<-c
	fmt.Printf("\n")
	s.Shutdown()
	os.Exit(0)
}
