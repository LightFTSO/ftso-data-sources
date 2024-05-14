package internal

import (
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"syscall"
)

type WsMessage struct {
	Type    int
	Message []byte
	Err     error
}

type WebsocketServer struct {
	Address    string
	hub        Hub
	wsEndpoint string
}

func NewWebsocketServer(addr string, port int, wsEndpoint string) *WebsocketServer {
	server := &WebsocketServer{
		Address:    fmt.Sprintf("%s:%d", addr, port),
		wsEndpoint: wsEndpoint,
		hub:        *newHub(),
	}

	return server
}

func (ws *WebsocketServer) Connect() error {
	go ws.hub.run()
	println(ws.wsEndpoint)
	http.HandleFunc(ws.wsEndpoint, func(w http.ResponseWriter, r *http.Request) {
		serveWs(&ws.hub, w, r)
	})
	err_chan := make(chan error, 1)
	go func() {
		err := http.ListenAndServe(ws.Address, nil)
		err_chan <- err
	}()
	println(ws.wsEndpoint)
	//err := <-err_chan
	return nil
}

func (ws *WebsocketServer) BroadcastMessage(messageType int, message []byte) error {
	ws.hub.broadcast <- &WsMessage{Type: messageType, Message: message}
	return nil
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
