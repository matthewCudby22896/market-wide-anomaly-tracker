package replayengine

import (
	"fmt"
	"log"
	"net/http"

	"github.com/coder/websocket"
	"github.com/coder/websocket/wsjson"
)

/*

WORK:

- basic web socket server that can accept connections
- upon recieving a connection a listen & send goroutine pair are launched

*/

// Every time a client makes a connection this function is run within it's
// own thread.
type Connection struct {
}

func webSocketServer(w http.ResponseWriter, r *http.Request) {
	c, err := websocket.Accept(w, r, nil)
	if err != nil {
		return
	}
	defer c.CloseNow()

	ctx := r.Context()

	outbox := make(chan any, 10)

	// Start writer go routine
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case msg := <-outbox:
				wsjson.Write(ctx, c, msg)
			}
		}
	}()

	// Start reader go routine in the current thread
	for {
		var v any
		err := wsjson.Read(ctx, c, &v)
		if err != nil {
			fmt.Println("Reader error/disconnect:", err)
			break
		}
		fmt.Printf("Received: %v\n", v)

		// TODO: Remove
		// Test: Send something back
		outbox <- map[string]string{"echo": "got it"}
	}
}
func runWebSocketServer() {
	http.HandleFunc("/ws", webSocketServer)
	fmt.Printf("WebSocket Server listening on %s\n", replayEnginerServerSocket)
	err := http.ListenAndServe(replayEnginerServerSocket, nil)
	if err != nil {
		log.Fatal("ListenAndServe: ", err)
	}
}

func LaunchServer() {
	runWebSocketServer()
}
