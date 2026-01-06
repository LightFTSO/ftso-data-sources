package internal

import (
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"syscall"

	"github.com/gorilla/websocket"
)

// WsMessage remains the same for internal Client communication
type WsMessage struct {
	Type    int
	Message []byte
	Err     error
}

type WebsocketServer struct {
	Address    string
	hub        *Hub // Changed to pointer to share state safely
	wsEndpoint string
}

func NewWebsocketServer(port int, wsEndpoint string) *WebsocketServer {
	return &WebsocketServer{
		Address:    fmt.Sprintf(":%d", port),
		wsEndpoint: wsEndpoint,
		hub:        newHub(),
	}
}

func (ws *WebsocketServer) Connect() error {
	go ws.hub.run()
	http.HandleFunc(ws.wsEndpoint, func(w http.ResponseWriter, r *http.Request) {
		serveWs(ws.hub, w, r) // serveWs expects *Hub
	})
	return nil
}

// BroadcastJSON is the new optimized method.
// Pass the struct directly (e.g., model.Ticker).
func (ws *WebsocketServer) BroadcastJSON(v interface{}) {
	// Optimization: Don't even send to channel if count is 0.
	// This saves channel lock overhead.
	if ws.hub.clientCount.Load() == 0 {
		return
	}

	ws.hub.broadcast <- LazyMessage{
		Type: websocket.TextMessage,
		Data: v,
	}
}

// BroadcastBytes keeps compatibility if you have pre-serialized data
func (ws *WebsocketServer) BroadcastBytes(messageType int, message []byte) {
	if ws.hub.clientCount.Load() == 0 {
		return
	}

	ws.hub.broadcast <- LazyMessage{
		Type: messageType,
		Data: message,
	}
}

// HasClients allows external services to check connection status cheaply
func (ws *WebsocketServer) HasClients() bool {
	return ws.hub.clientCount.Load() > 0
}

func Hook() {
	sigs := make(chan os.Signal, 1)
	done := make(chan bool, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		for sig := range sigs {
			if sig != syscall.SIGINT && sig != syscall.SIGTERM && sig != syscall.SIGKILL {
				return
			}
			done <- true
		}
	}()

	<-done
}
