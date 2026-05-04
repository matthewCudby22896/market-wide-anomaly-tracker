package utils

import (
	"context"

	"github.com/coder/websocket"
	"github.com/coder/websocket/wsjson"

	"github.com/mcudby/mwat/components/replayengine"
)

type TestClient struct {
	ctx  context.Context
	conn *websocket.Conn
}

func NewTestClient(ctx context.Context, url string) (*TestClient, error) {
	conn, _, err := websocket.Dial(ctx, url, nil)
	if err != nil {
		return nil, err
	}

	return &TestClient{
		ctx:  ctx,
		conn: conn,
	}, nil
}

func (c *TestClient) Send(msg any) error {
	return wsjson.Write(c.ctx, c.conn, msg)
}

func (c *TestClient) BlockingReceive(dst any) error {
	return wsjson.Read(c.ctx, c.conn, &dst)
}

func (c *TestClient) SubToTimestream() error {
	req := replayengine.SubscriptionRequest{
		Action: "sub",
		Symbols: []string{"TIMESTREAM"},
	}
	return c.Send(req)
}

func (c *TestClient) SubToSymbols(symbols []string) error {
	req := replayengine.SubscriptionRequest{
		Action: "sub",
		Symbols: symbols,
	}
	return c.Send(req)
}

func (c *TestClient) UnsubToSymbols(symbols []string) error {
	req := replayengine.SubscriptionRequest{
		Action: "unsub",
		Symbols: symbols,
	}
	return c.Send(req)
}
