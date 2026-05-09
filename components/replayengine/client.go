package replayengine

import (
	"context"
	"fmt"
	"sync"

	"github.com/coder/websocket"
	"github.com/coder/websocket/wsjson"
	"github.com/google/uuid"
)

var clientID int = 0

type Client interface {
	LifeCycle
	Outbox() <-chan any
}

type client struct {
	ID         string
	connection *websocket.Conn

	outbox chan any

	context       context.Context
	cancelContext context.CancelFunc

	wg sync.WaitGroup

	logger *Logger

	// For sending unsub, sub, unregister requests to hub
	// Set upon registration of the client to the hub
	hubRequestOutbox chan<- ClientRequest
}

func NewClient(c *websocket.Conn, hub Hub) *client {
	defer func() {
		clientID += 1
	}()

	ctx, cancel := context.WithCancel(context.Background())

	id := fmt.Sprintf("client-%s", uuid.New().String()[:8])

	return &client{
		ID:               id,
		connection:       c,
		outbox:           make(chan any, 1024),
		context:          ctx,
		cancelContext:    cancel,
		wg:               sync.WaitGroup{},
		logger:           NewComponentLogger(id),
		hubRequestOutbox: nil, // Set by hub upon registration
	}
}

func (c *client) Shutdown() {
	c.cancelContext()
	c.wg.Wait()
	c.logger.LogShutdown()
}

func (c *client) Start() {
	c.wg.Add(1)
	go c.ListenerThread()
	c.wg.Add(1)
	go c.SenderThread()

	// This will ensure that the client is always unregistered from the Hub
	// when the client disconnects
	c.wg.Add(1)
	go func(c *client) {
		defer c.wg.Done()
		<-c.context.Done()
		c.logger.Info("requesting deregistration")
		c.hubRequestOutbox <- unregisterRequest{BaseRequest{c}}
	}(c)
	c.logger.LogStart()
}

func (c *client) ListenerThread() {
	defer c.wg.Done()
	for {
		var v SubscriptionRequest
		err := wsjson.Read(c.context, c.connection, &v)

		if err != nil {
			status := websocket.CloseStatus(err)
			if status == -1 {
				c.logger.Info("client gracefully disconnected")
			} else {
				c.logger.Error("client read error", "error", err)
			}
			go c.Shutdown()
			return
		}

		switch v.Action {
		case "sub":
			c.hubRequestOutbox <- subRequest{BaseRequest{c}, toTypedTicker(v.Symbols)}
		case "unsub":
			c.hubRequestOutbox <- unsubRequest{BaseRequest{c}, toTypedTicker(v.Symbols)}
		default:
			c.logger.Info("unrecognised `action` field", "action", v.Action)
		}
	}
}

func toTypedTicker(arr []string) []string {
	typedTickers := make([]string, len(arr))
	for i, ticker := range arr {
		typedTickers[i] = string(ticker)
	}
	return typedTickers
}

func (c *client) SenderThread() {
	defer c.wg.Done()
	for {
		select {
		case <-c.context.Done():
			return
		case msg := <-c.outbox:
			err := wsjson.Write(c.context, c.connection, msg)
			if err != nil {
				fmt.Println(err)
			}
		}
	}
}

func (c *client) Outbox() chan<- any {
	return c.outbox
}
