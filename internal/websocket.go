package internal

import (
	"net/http"
	"net/url"
	"time"

	"github.com/gorilla/websocket"
)

type WsMessage struct {
	Type    int
	Message []byte
	Err     error
}
type WsMessageHandler func(message WsMessage) error

type WebsocketClient struct {
	Endpoint   url.URL
	Connection *websocket.Conn
	onMessage  WsMessageHandler
}

func NewWebsocketClient(addr string, secure bool, onMessage WsMessageHandler) *WebsocketClient {
	scheme := "ws"

	if secure {
		scheme = "wss"
	}

	return &WebsocketClient{
		Endpoint:  url.URL{Scheme: scheme, Host: addr},
		onMessage: onMessage,
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
func (ws *WebsocketClient) Listen() {
	for {
		messageType, message, err := ws.Connection.ReadMessage()
		if err != nil {
			ws.onMessage(WsMessage{Type: messageType, Message: message, Err: err})
			return
		}

		if ws.onMessage != nil {
			ws.onMessage(WsMessage{Type: messageType, Message: message, Err: nil})
		}
	}

}

func (ws *WebsocketClient) Close() {
	ws.Connection.Close()
}

func (ws *WebsocketClient) SetKeepAlive(c *websocket.Conn, timeout time.Duration) {
	ticker := time.NewTicker(timeout)

	lastResponse := time.Now()
	ws.Connection.SetPongHandler(func(msg string) error {
		lastResponse = time.Now()
		return nil
	})

	go func() {
		defer ticker.Stop()
		for {
			deadline := time.Now().Add(10 * time.Second)
			err := c.WriteControl(websocket.PingMessage, []byte{}, deadline)
			if err != nil {
				return
			}
			<-ticker.C
			if time.Since(lastResponse) > timeout {
				c.Close()
				return
			}
		}
	}()
}

func (b *WebsocketClient) SetMessageHandler(handler WsMessageHandler) {
	b.onMessage = handler
}
