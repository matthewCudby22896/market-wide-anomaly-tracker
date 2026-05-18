package wsclient

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strings"
	"sync"
	"io"

	"github.com/coder/websocket"
	"github.com/coder/websocket/wsjson"
	"github.com/google/uuid"

	"github.com/mcudby/mwat/components/replayengine/api"
	"github.com/mcudby/mwat/components/replayengine/common"
	"github.com/mcudby/mwat/components/replayengine/logging"
)

type WSClient struct {
	ID           string
	ctx          context.Context
	cancelCtx    context.CancelFunc
	wg           sync.WaitGroup
	shutdownOnce sync.Once
	logger       *logging.Logger

	conn   *websocket.Conn
	outbox chan any

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
		conn:          conn,
		outbox:        make(chan any, 1024),
		requestOutbox: outbox,
	}
}

func (c *WSClient) Close() {
	c.conn.Close(websocket.StatusNormalClosure, "closure requested by server")
	c.Shutdown()
}

func (c *WSClient) Shutdown() {
	c.shutdownOnce.Do(func() {
		go c.conn.Close(websocket.StatusNormalClosure, "")

		c.cancelCtx()
		c.wg.Wait()

		c.logger.Info("requesting deregistration")
		c.requestOutbox <- UnregisterRequest{c}

		c.logger.LogShutdown()
	})
}

func (c *WSClient) Start() {
	c.wg.Go(c.ListenerThread)

	c.wg.Go(c.SenderThread)

	c.logger.LogStart()
}

func (c *WSClient) ListenerThread() {
	for {
		var v api.SubscriptionRequest
		err := wsjson.Read(c.ctx, c.conn, &v)

		if err != nil {
			go c.Shutdown()

			if errors.Is(err, context.Canceled) {
				return
			} 
			
			if status := websocket.CloseStatus(err); status != -1 {
				c.logger.Info("connection closed via handshake, exiting listener loop", "status-code", status)
				return
			} 
			
			if errors.Is(err, net.ErrClosed) || errors.Is(err, io.EOF) {
				c.logger.Info("connection was closed abruptly (EOF), exiting listener loop")
				return
			} 

			c.logger.Error("failed to read from ws conn", "err", err)
			return
		}

		streams, err := extractStreamsFromParams(v.Params)
		if err != nil {
			c.logger.Error("failed to extract symbols from params", "params", v.Params, "err", err)
			continue
		}

		switch v.Action {
		case "sub":
			c.requestOutbox <- SubRequest{c, streams}
		case "unsub":
			c.requestOutbox <- UnsubRequest{c, streams}
		default:
			c.logger.Error("unrecognised `action` field", "action", v.Action)
			continue
		}
	}
}

func (c *WSClient) SenderThread() {
	for {
		select {
		case <-c.ctx.Done():
			return
		case msg := <-c.outbox:

			err := wsjson.Write(c.ctx, c.conn, msg)

			if err != nil {
				go c.Shutdown()
				if errors.Is(err, context.Canceled) {

				} else if status := websocket.CloseStatus(err); status != -1 {
					c.logger.Info("connection closed, exiting sender loop", "status-code", status)

				} else if errors.Is(err, net.ErrClosed) {
					c.logger.Info("connection was closed, exiting sender loop")

				} else {
					c.logger.Error("failed to read from ws conn", "err", err)
				}
				return
			}
		}
	}
}

func (c *WSClient) Outbox() chan<- any {
	return c.outbox
}

func extractStreamsFromParams(params string) ([]common.Stream, error) {
	values := strings.Split(params, ",")
	for i := range values {
		values[i] = strings.TrimSpace(values[i])
	}

	streams := make([]common.Stream, len(values))

	for i, v := range values {
		// Special case
		if v == "TIMESTREAM" {
			streams[i].Symbol, streams[i].Symbol = v, v
			continue
		}

		// Regular case
		parts := strings.Split(v, ".")
		if len(parts) != 2 {
			return nil, fmt.Errorf("csv %d `%s` did not meet expected format", i, v)
		}
		streamID, symbol := parts[0], parts[1]
		if !(streamID == "A" || streamID == "AM") {
			return nil, fmt.Errorf("csv %d `%s` did not meet expected format", i, v)
		}

		streams[i].Type = streamID
		streams[i].Symbol = symbol
	}

	return streams, nil
}
