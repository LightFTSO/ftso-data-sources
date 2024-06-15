package internal

import (
	"fmt"
	"log/slog"
	"net/url"
	"sync"
	"time"

	"github.com/bytedance/sonic"
	"github.com/gorilla/websocket"
)

const (
	reconnectDelay = 1 * time.Second
	writeWait      = 10 * time.Second
)

type WsMessage struct {
	Type    int
	Message []byte
	Err     error
}
type WsMessageHandler func(message WsMessage)

type WebSocketClient struct {
	url          string
	conn         *websocket.Conn
	send         chan WsMessage
	receive      chan WsMessage
	close        chan struct{}
	reconnect    chan struct{}
	done         chan struct{}
	mu           sync.Mutex
	once         sync.Once
	reconnecting bool
	closed       bool
	onConnect    func() error
	onDisconnect func() error
	onMessage    func(WsMessage)
	log          *slog.Logger
}

func NewWebSocketClient(url string) *WebSocketClient {
	return &WebSocketClient{
		url:       url,
		send:      make(chan WsMessage, 256), // Buffered channel
		receive:   make(chan WsMessage, 256), // Buffered channel
		close:     make(chan struct{}),
		reconnect: make(chan struct{}),
		done:      make(chan struct{}),
	}
}

func (c *WebSocketClient) SendMessage(message WsMessage) {
	if c.closed {
		return
	}

	c.send <- message
}

func (c *WebSocketClient) SendMessageJSON(msgType int, message interface{}) error {
	data, err := sonic.Marshal(message)
	if err != nil {
		return err
	}

	c.SendMessage(WsMessage{Type: msgType, Message: data})

	return nil
}

func (c *WebSocketClient) Start() {
	go func() {
		for {
			if err := c.connect(); err != nil {
				c.log.Error("connection error:", "error", err)
				continue
			}

			select {
			case <-c.close:
				c.log.Info("Closing WebSocket client")
				return
			case <-c.reconnect:
				<-c.done
				time.Sleep(reconnectDelay)
				c.log.Info("Reconnecting...")
				continue
			}
		}
	}()
}

func (c *WebSocketClient) Close() {
	c.once.Do(func() {
		c.closed = true
		close(c.close)
	})
}

func (c *WebSocketClient) Reconnect() {
	c.handleDisconnect()
	c.handleReconnection()
}

func (c *WebSocketClient) connect() error {
	u, err := url.Parse(c.url)
	if err != nil {
		return err
	}

	for {
		conn, _, err := websocket.DefaultDialer.Dial(u.String(), nil)
		if err == nil {
			conn.SetReadLimit(655350)
			c.mu.Lock()
			c.conn = conn
			c.reconnecting = false
			c.mu.Unlock()
			break
		}
		c.log.Error("Error connecting. Retrying in 1 second...", "err", err)
		time.Sleep(time.Second)
	}

	if c.onMessage != nil {
		go c.startListening()
	} else {
		return fmt.Errorf("onMessage handler not set")
	}

	go c.writePump()
	go c.readPump()

	c.log.Info("Connected")

	if c.onConnect != nil {
		c.onConnect()
	}

	return nil
}

func (c *WebSocketClient) readPump() {
	defer c.handleReconnection()

	for {
		msgType, message, err := c.conn.ReadMessage()
		if err != nil {
			if c.closed {
				return
			}
			c.log.Error(err.Error())
			c.handleDisconnect()
			return
		}
		c.receive <- WsMessage{Type: msgType, Message: message}
	}
}

func (c *WebSocketClient) writePump() {
	defer c.handleReconnection()

	for message := range c.send {
		if err := c.write(message.Type, message.Message); err != nil {
			if c.closed {
				return
			}
			c.log.Error("write error", "error", err)
			c.handleDisconnect()
			return

		}
	}
}

func (c *WebSocketClient) write(messageType int, data []byte) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.conn == nil {
		return fmt.Errorf("connection is nil")
	}
	c.conn.SetWriteDeadline(time.Now().Add(writeWait))
	return c.conn.WriteMessage(messageType, data)
}

func (c *WebSocketClient) handleReconnection() {
	if c.closed {
		return
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.reconnecting && !c.closed {
		c.reconnecting = true
		c.reconnect <- struct{}{}
	}
}

func (c *WebSocketClient) handleDisconnect() {
	if c.closed {
		return
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	if c.conn != nil {
		c.conn.Close()
		c.conn = nil
	}
	if c.onDisconnect != nil {
		c.onDisconnect()
	}

	close(c.done)
	c.done = make(chan struct{})
}

func (c *WebSocketClient) SetOnConnect(handler func() error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.onConnect = handler
}

func (c *WebSocketClient) SetOnDisconnect(handler func() error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.onDisconnect = handler
}

func (ws *WebSocketClient) SetLogger(logger *slog.Logger) {
	ws.log = logger
}

func (ws *WebSocketClient) SetMessageHandler(handler WsMessageHandler) {
	ws.onMessage = handler
}

func (c *WebSocketClient) startListening() {
	for msg := range c.receive {
		c.onMessage(msg)
	}
}
