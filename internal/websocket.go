package internal

import (
	"net/http"
	"net/url"
	"time"

	"github.com/gorilla/websocket"
)

type WebsocketClient struct {
	Endpoint   url.URL
	Connection *websocket.Conn
}

type WsMessage struct {
	Type    int
	Message []byte
	Err     error
}

func NewWebsocketClient(addr string, secure bool) *WebsocketClient {
	scheme := "ws"

	if secure {
		scheme = "wss"
	}

	return &WebsocketClient{
		Endpoint: url.URL{Scheme: scheme, Host: addr},
	}
}

func (ws *WebsocketClient) Connect(header http.Header) (*http.Response, error) {
	dialer := &websocket.Dialer{
		Proxy:             http.ProxyFromEnvironment,
		HandshakeTimeout:  10 * time.Second,
		EnableCompression: false,
	}

	var (
		err error
		res *http.Response
	)
	ws.Connection, res, err = dialer.Dial(ws.Endpoint.Host, header)
	if err != nil {
		return res, err
	}
	ws.Connection.SetReadLimit(655350)

	return res, nil
}

func (ws *WebsocketClient) SendMessageJSON(message interface{}) error {
	err := ws.Connection.WriteJSON(message)
	if err != nil {
		return err
	}
	return nil
}

// Listen receives and parses the message into a map structure
func (ws *WebsocketClient) Listen(messageBuffer chan<- WsMessage) {
	for {
		messageType, message, err := ws.Connection.ReadMessage()
		if err != nil {
			messageBuffer <- WsMessage{Type: messageType, Message: message, Err: err}
			return
		}

		messageBuffer <- WsMessage{Type: messageType, Message: message, Err: nil}
	}

}

func (ws *WebsocketClient) Close() {
	ws.Connection.Close()
}
