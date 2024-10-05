package internal

import (
	"context"
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
	writeWait      = 1 * time.Second
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
	reconnect    chan struct{}
	mu           sync.Mutex
	once         sync.Once
	reconnecting bool
	Closed       bool
	onConnect    func() error
	onDisconnect func() error
	onMessage    func(WsMessage)
	log          *slog.Logger
	ctx          context.Context
	cancel       context.CancelFunc
}

func NewWebSocketClient(url string) *WebSocketClient {
	ctx, cancel := context.WithCancel(context.Background())
	return &WebSocketClient{
		url:       url,
		send:      make(chan WsMessage, 256),
		receive:   make(chan WsMessage, 256),
		reconnect: make(chan struct{}, 1),
		ctx:       ctx,
		cancel:    cancel,
		log:       slog.Default(),
	}
}

func (c *WebSocketClient) SendMessage(message WsMessage) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.Closed {
		return
	}
	select {
	case c.send <- message:
	default:
		c.log.Warn("SendMessage: send channel is full, dropping message")
	}
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
			select {
			case <-c.ctx.Done():
				c.log.Info("WebSocket client context canceled")
				return
			default:
				if err := c.connect(); err != nil {
					c.log.Error("connection error:", "error", err)
					time.Sleep(reconnectDelay)
					continue
				}

				select {
				case <-c.ctx.Done():
					c.log.Info("Closing WebSocket client")
					c.handleDisconnect()
					return
				case <-c.reconnect:
					time.Sleep(reconnectDelay)
					c.log.Info("Reconnecting...")
					continue
				}
			}
		}
	}()
}

func (c *WebSocketClient) Close() {
	c.once.Do(func() {
		c.mu.Lock()
		c.Closed = true
		c.mu.Unlock()
		c.cancel()
		c.handleDisconnect()
	})
}

func (c *WebSocketClient) Reconnect() {
	c.handleDisconnect()
	c.handleReconnection()
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

func (c *WebSocketClient) connect() error {
	u, err := url.Parse(c.url)
	if err != nil {
		return err
	}

	var conn *websocket.Conn
	for {
		conn, _, err = websocket.DefaultDialer.Dial(u.String(), nil)
		if err == nil {
			conn.SetReadLimit(655350)
			c.mu.Lock()
			c.conn = conn
			c.reconnecting = false
			c.mu.Unlock()
			break
		}
		c.log.Error("Error connecting. Retrying in 1 second...", "err", err)
		select {
		case <-c.ctx.Done():
			return c.ctx.Err()
		case <-time.After(time.Second):
		}
	}

	if c.onMessage == nil {
		return fmt.Errorf("onMessage handler not set")
	}

	go c.startListening()
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
		select {
		case <-c.ctx.Done():
			return
		default:
			msgType, message, err := c.conn.ReadMessage()
			if err != nil {
				c.log.Error("read error", "error", err.Error())
				c.handleDisconnect()
				return
			}
			select {
			case c.receive <- WsMessage{Type: msgType, Message: message}:
			case <-c.ctx.Done():
				return
			}
		}
	}
}

func (c *WebSocketClient) writePump() {
	defer c.handleReconnection()

	for {
		select {
		case <-c.ctx.Done():
			return
		case message := <-c.send:
			if err := c.write(message.Type, message.Message); err != nil {
				c.log.Error("write error", "error", err)
				c.handleDisconnect()
				return
			}
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
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.Closed {
		return
	}
	if !c.reconnecting {
		c.reconnecting = true
		select {
		case c.reconnect <- struct{}{}:
		default:
		}
	}
}

func (c *WebSocketClient) handleDisconnect() {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.conn != nil {
		c.conn.Close()
		c.conn = nil
	}
	if c.onDisconnect != nil {
		c.onDisconnect()
	}
}

func (c *WebSocketClient) startListening() {
	for {
		select {
		case <-c.ctx.Done():
			return
		case msg := <-c.receive:
			c.onMessage(msg)
		}
	}
}
