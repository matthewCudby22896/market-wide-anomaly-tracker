package wsclient

type UnregisterRequest struct {
	Sender *WSClient
}

type RegisterRequest struct {
	Sender *WSClient
}

// todo: possibly move to own file which defines external interface
type SubscriptionRequest struct {
	Action  string   `json:"action"`  // "sub" or "unsub"
	Symbols []string `json:"symbols"` // e.g., ["QQQ", "SPY"]
}

type SubRequest struct {
	Sender  *WSClient
	Symbols []string
}

type UnsubRequest struct {
	Sender  *WSClient
	Symbols []string
}
