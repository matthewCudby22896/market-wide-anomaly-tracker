package test

import (
	"context"

	"github.com/coder/websocket"
	"github.com/coder/websocket/wsjson"
)

type testClient struct {
	ctx  context.Context
	conn *websocket.Conn
}

func newTestClient(ctx context.Context, url string) (*testClient, error) {
	conn, _, err := websocket.Dial(ctx, url, nil)
	if err != nil {
		return nil, err
	}

	return &testClient{
		ctx:  ctx,
		conn: conn,
	}, nil
}

func (c *testClient) Send(msg any) error {
	return wsjson.Write(c.ctx, c.conn, msg)
}

func (c *testClient) BlockingReceive(dst any) error {
	return wsjson.Read(c.ctx, c.conn, &dst)
}

func (c *testClient) SubToTimestream() error {
	subTimestream := struct {
		Action  string   `json:"action"`
		Symbols []string `json:"symbols"`
	}{
		Action:  "subscribe",
		Symbols: []string{"TIMESTREAM"},
	}
	return c.Send(subTimestream)
}
