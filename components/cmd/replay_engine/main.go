package main

import (
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	replayengine "github.com/mcudby/mwat/components/replayengine"
)

func main() {
	databaseURL := flag.String("db-url", "localhost:6543", "url of the timescaledb instance")

	flag.Parse()

	opts := replayengine.Opts{
		DatabaseURL: *databaseURL,
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
