package wsclient

type UnreqisterRequest struct {
	Sender *WSClient
}

type ReqisterRequest struct {
	Sender *WSClient
}

// todo: possibly move to own file which defines external interface
type SubscriptionRequest struct {
	Action  string   `json:"action"`  // "sub" or "unsub"
	Symbols []string `json:"symbols"` // e.g., ["QQQ", "SPY"]
}

type SubRequest struct {
	Sender  *WSClient
	symbols []string
}

type UnsubRequest struct {
	Sender  *WSClient
	symbols []string
}
