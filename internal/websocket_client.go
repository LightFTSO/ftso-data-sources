package internal

import (
	"log/slog"
	"time"

	"github.com/bytedance/sonic"
	"github.com/gorilla/websocket"
)

type WsMessage struct {
	Type    int
	Message []byte
	Err     error
}
type WsMessageHandler func(message WsMessage)

type WebsocketClient struct {
	conn      *websocket.Conn
	send      chan WsMessage
	received  chan WsMessage
	done      chan struct{}
	onMessage func(WsMessage)
	url       string
	log       *slog.Logger
}

func NewWebsocketClient(url string) *WebsocketClient {
	client := &WebsocketClient{
		conn:      nil,
		send:      make(chan WsMessage),
		received:  make(chan WsMessage),
		done:      make(chan struct{}),
		onMessage: nil,
		url:       url,
		log:       slog.Default(),
	}
	return client
}

func (ws *WebsocketClient) SetMessageHandler(handler WsMessageHandler) {
	ws.onMessage = handler
}
func (ws *WebsocketClient) SetLogger(logger *slog.Logger) {
	ws.log = logger
}

func (c *WebsocketClient) readPump() {
	defer func() {
		c.conn.Close()
		close(c.received)
	}()

	for {
		msgType, msg, err := c.conn.ReadMessage()
		if err != nil {
			c.log.Error("Error reading websocket message", "error", err)
		}
		select {
		case c.received <- WsMessage{Type: msgType, Message: msg, Err: err}:
		case <-c.done:
			return
		}
	}
}

func (c *WebsocketClient) writePump() {
	defer func() {
		c.conn.Close()
	}()

	for {
		select {
		case msg := <-c.send:
			err := c.conn.WriteMessage(msg.Type, msg.Message)
			if err != nil {
				c.log.Error("Error writing message", "err", err)
				c.Disconnect()
				return
			}
		case <-c.done:
			c.log.Debug("Closing connection...")
			return
		}
	}
}

func (c *WebsocketClient) SendMessage(msg WsMessage) {
	select {
	case c.send <- msg:
	case <-c.done:
	}
}

func (c *WebsocketClient) SendMessageJSON(msgType int, message interface{}) error {
	data, err := sonic.Marshal(message)
	if err != nil {
		return err
	}

	select {
	case c.send <- WsMessage{Type: msgType, Message: data}:
	case <-c.done:
	}
	return nil
}

func (c *WebsocketClient) StartListening() {
	for {
		select {
		case msg := <-c.received:
			c.onMessage(msg)
		case <-c.done:
			c.log.Debug("Closing message receiving...")
			return
		}
	}
}

func (c *WebsocketClient) Disconnect() {
	select {
	case <-c.done:
		// Already closed
	default:
		close(c.done)
	}

}

func (c *WebsocketClient) Connect() error {
	c.log.Info("Connecting...")
	var newConn *websocket.Conn
	var err error

	for {
		newConn, _, err = websocket.DefaultDialer.Dial(c.url, nil)
		if err == nil {
			break
		}
		c.log.Error("Error reconnecting. Retrying in 1 second...", "err", err)
		time.Sleep(time.Second)
	}
	newConn.SetReadLimit(655350)

	c.conn = newConn
	c.received = make(chan WsMessage)
	c.send = make(chan WsMessage)
	c.done = make(chan struct{})

	go c.readPump()
	go c.writePump()

	go c.StartListening()
	return nil
}

func (c *WebsocketClient) Reconnect() error {
	c.log.Info("Reconnecting...")
	if c.conn != nil {
		c.Disconnect()
	}

	var newConn *websocket.Conn
	var err error

	for {
		newConn, _, err = websocket.DefaultDialer.Dial(c.url, nil)
		if err == nil {
			break
		}
		c.log.Error("Error reconnecting. Retrying in 1 second...", "err", err)
		time.Sleep(time.Second)
	}
	newConn.SetReadLimit(655350)

	c.conn = newConn
	c.received = make(chan WsMessage)
	c.send = make(chan WsMessage)
	c.done = make(chan struct{})

	go c.readPump()
	go c.writePump()

	go c.StartListening()
	return nil
}
