package utils

import (
	"context"

	"github.com/coder/websocket"
	"github.com/coder/websocket/wsjson"
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
	subTimestream := struct {
		Action  string   `json:"action"`
		Symbols []string `json:"symbols"`
	}{
		Action:  "subscribe",
		Symbols: []string{"TIMESTREAM"},
	}
	return c.Send(subTimestream)
}
