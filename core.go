/*
	1. Create channel
	2. Pass this same channel to both routines (producer and consumer)
*/

/*
	type Agg struct {
		Ticker       string  `json:"T,omitempty"`
		Close        float64 `json:"c,omitempty"`
		High         float64 `json:"h,omitempty"`
		Low          float64 `json:"l,omitempty"`
		Transactions int64   `json:"n,omitempty"`
		Open         float64 `json:"o,omitempty"`
		Timestamp    Millis  `json:"t,omitempty"`
		Volume       float64 `json:"v,omitempty"`
		VWAP         float64 `json:"vw,omitempty"`
		OTC          bool    `json:"otc,omitempty"`
	}
*/
package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"sync"
	"time"

	massive "github.com/massive-com/client-go/v2/rest"
	"github.com/massive-com/client-go/v2/rest/models"
)

const timeMultiplier = 10.0

var tickers = []string{
	"SPY", "QQQ", "APPL", "MSFT", "NVDA", "TSLA", "AMZN", "GOOGL", "JPM", "V",
}

type ClockHub struct {
	mu          sync.Mutex
	subscribers []chan time.Time
}

/* This function belongs to the ClockHub struct. It's like a class method*/
func (h *ClockHub) Subscribe() chan time.Time {
	h.mu.Lock()
	defer h.mu.Unlock()
	ch := make(chan time.Time, 1)
	h.subscribers = append(h.subscribers, ch)
	return ch
}

func (h *ClockHub) Broadcast(t time.Time) {
	h.mu.Lock()
	defer h.mu.Unlock()

	// Non-blocking send
	for _, ch := range h.subscribers {
		select {
		case ch <- t:
		default:
		}
	}
}


func runTickerWorker(ticker string, clockChan chan time.Time) {
	// Initiate consumer: sends the OHLC bars down the web sockets at a rate dictated by the simulation clock

	// Initiate producer: fetches the OHLC bars from massive.com

	for {
		simTime, ok := <-clockChan
		if !ok {
			break
		}
		fmt.Printf("Sim Time: %s | Ticker: %s\n", simTime.Format("15:04:05"), ticker)
	}
}

func simulationClock(hub *ClockHub, startTime time.Time) {
	simStart := time.Now()
	interval := time.Duration(float64(time.Second) / timeMultiplier)

	// Use a counter to anchor every tick to the absolute start
	ticks := 0

	for {
		ticks++
		// 1. Target is ALWAYS (Start + N * Interval)
		// This is mathematically impossible to drift.
		nextTick := simStart.Add(time.Duration(ticks) * interval)

		// 2. Broadcast
		elapsed := time.Duration(float64(time.Since(simStart)) * timeMultiplier)
		hub.Broadcast(startTime.Add(elapsed))

		// 3. Sleep until the absolute target
		remaining := time.Until(nextTick)
		if remaining > 0 {
			time.Sleep(remaining)
		}
	}
}

func initMassive(){
	key := os.Getenv("MASSIVE_API_KEY")
	c := massive.New(key)
}

func startSimulation(rootCtx context.Context, dateStr string) {
	/* A Location maps time instants to the zone in use at that time. Typically, the Location represents the collection of time offsets in use in
	a geographical area.*/

	loc, _ := time.LoadLocation("America/New_York")
	t, err := time.Parse("02-01-2006", dateStr)
	if err != nil {
		fmt.Println(err)
		return
	}

	marketOpen := time.Date(t.Year(), t.Month(), t.Day(), 9, 30, 0, 0, loc)
	marketClose := time.Date(t.Year(), t.Month,(), t.Day(), 4, 30, 0, 0, loc)

	// Create ClockHub
	hub := &ClockHub{}

	// Initiate the individual ticker workers
	for _, ticker := range tickers {
		go runTickerWorker(ticker, hub.Subscribe())
	}

	// Start the simulation clock
	go simulationClock(hub, marketOpen)

	select {} // Blocking
}
func main() {
	rootCtx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// 2. Setup Signal Handling (Ctrl+C)
	go func() {
		stop := make(chan os.Signal, 1)
		signal.Notify(stop, os.Interrupt)
		<-stop
		fmt.Println("\nShutdown signal received...")
		cancel() 
	}()

	startSimulation(rootCtx, "05-01-2026")
}
