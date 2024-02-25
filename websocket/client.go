package internal

import (
	"encoding/json"
	"net/http"
	"net/url"
	"time"

	log "log/slog"

	"github.com/gorilla/websocket"
)

type WebsocketClient struct {
	Endpoint   url.URL
	Connection *websocket.Conn
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
		Proxy:            http.ProxyFromEnvironment,
		HandshakeTimeout: 45 * time.Second,
	}

	var (
		err error
		r   *http.Response
	)
	ws.Connection, r, err = dialer.Dial(ws.Endpoint.Host, header)
	if err != nil {
		return r, err
	}

	return r, nil
}

func (ws *WebsocketClient) SendMessageJSON(message interface{}) error {
	err := ws.Connection.WriteJSON(message)
	if err != nil {
		return err
	}
	return nil
}

// Listen receives and parses te message into a map structure
func (ws *WebsocketClient) Listen(messageBuffer chan<- map[string]interface{}) {

	for {
		_, message, err := ws.Connection.ReadMessage()
		if err != nil {
			log.Error(err.Error())
			break
		}
		var msg map[string]interface{}
		err = json.Unmarshal(message, &msg)
		if err != nil {
			log.Warn("Failure parsing json", "message", message, "error", err)
		} else {
			messageBuffer <- msg
		}
	}
}

func (ws *WebsocketClient) Close() {
	ws.Connection.Close()
}
