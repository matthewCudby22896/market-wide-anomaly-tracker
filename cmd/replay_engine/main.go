package main

import (
	"fmt"
	"os"
	"os/signal"
	"syscall"

	replayengine "github.com/mcudby/mwat/components/replayengine"
)

func main() {
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)

	s := replayengine.NewReplayEngineServer()
	s.Start()
	<-c
	fmt.Printf("\n")
	s.Shutdown()
}
