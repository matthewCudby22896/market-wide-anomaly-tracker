package main

import (
	"os"
	"os/signal"
	"syscall"

	replayengine "github.com/matthewCudby22896/market_wide_anomaly_tracker/components/replayengine"
)

func main() {
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)

	s := replayengine.LaunchServer()
	<-c
	s.Shutdown()
}
