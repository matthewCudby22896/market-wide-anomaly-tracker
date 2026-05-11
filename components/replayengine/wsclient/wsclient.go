package wsclient

import (
	"context"
	"fmt"
	"sync"

	"github.com/coder/websocket"
	"github.com/coder/websocket/wsjson"
	"github.com/google/uuid"

	"github.com/mcudby/mwat/components/replayengine/logging"
)

type WSClient struct {
	ID        string
	ctx       context.Context
	cancelCtx context.CancelFunc
	wg        sync.WaitGroup
	logger    *logging.Logger

	connection *websocket.Conn
	outbox     chan any

	requestOutbox chan<- any
}

func NewClient(conn *websocket.Conn, outbox chan<- any) *WSClient {
	ctx, cancel := context.WithCancel(context.Background())

	id := fmt.Sprintf("client-%s", uuid.New().String()[:8])

	return &WSClient{
		ID:            id,
		ctx:           ctx,
		cancelCtx:     cancel,
		wg:            sync.WaitGroup{},
		logger:        logging.NewComponentLogger(id),
		connection:    conn,
		outbox:        make(chan any, 1024),
		requestOutbox: outbox,
	}
}

func (c *WSClient) Shutdown() {
	c.cancelCtx()
	c.wg.Wait()
	c.logger.LogShutdown()
}

func (c *WSClient) Start() {
	c.wg.Add(1)
	go c.ListenerThread()
	c.wg.Add(1)
	go c.SenderThread()

	// This will ensure that the client is always unregistered from the Hub
	// when the client disconnects
	c.wg.Add(1)
	go func(c *WSClient) {
		defer c.wg.Done()
		<-c.ctx.Done()
		c.logger.Info("requesting deregistration")
		c.requestOutbox <- UnregisterRequest{c}
	}(c)
	c.logger.LogStart()
}

func (c *WSClient) ListenerThread() {
	defer c.wg.Done()
	for {
		var v SubscriptionRequest
		err := wsjson.Read(c.ctx, c.connection, &v)

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
			c.requestOutbox <- SubRequest{c, v.Symbols}
		case "unsub":
			c.requestOutbox <- UnsubRequest{c, v.Symbols}
		default:
			c.logger.Error("unrecognised `action` field", "action", v.Action)
		}
	}
}

func (c *WSClient) SenderThread() {
	defer c.wg.Done()
	for {
		select {
		case <-c.ctx.Done():
			return
		case msg := <-c.outbox:
			err := wsjson.Write(c.ctx, c.connection, msg)
			if err != nil {
				c.logger.Error("failed to write to json", "err", err)
			}
		}
	}
}

func (c *WSClient) Outbox() chan<- any {
	return c.outbox
}
