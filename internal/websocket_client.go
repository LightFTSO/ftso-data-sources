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
	// Time allowed to write a message to the peer.
	writeWait = 5 * time.Second
	// Time allowed to read the next pong message from the peer.
	pongWait = 60 * time.Second
	// Send pings to peer with this period. Must be less than pongWait.
	pingPeriod = (pongWait * 9) / 10
	// Base reconnection delay
	baseReconnectDelay = 250 * time.Millisecond
	maxReconnectDelay  = 30 * time.Second
)

type WsMessage struct {
	Type    int
	Message []byte
	Err     error
}

type WsMessageHandler func(message WsMessage)

type WebSocketClient struct {
	url string
	// Guards the connection reference
	mu   sync.RWMutex
	conn *websocket.Conn

	send    chan WsMessage
	receive chan WsMessage

	// Lifecycle management
	ctx    context.Context
	cancel context.CancelFunc

	// Handlers
	onConnect    func() error
	onDisconnect func() error
	onMessage    func(WsMessage)

	reconnectDelay time.Duration

	log *slog.Logger
}

func NewWebSocketClient(url string) *WebSocketClient {
	ctx, cancel := context.WithCancel(context.Background())
	return &WebSocketClient{
		url:     url,
		send:    make(chan WsMessage, 512), // Buffered to handle bursts
		receive: make(chan WsMessage, 512),
		ctx:     ctx,
		cancel:  cancel,
		log:     slog.Default(),
	}
}

// Start begins the connection supervisor and the message processor.
// It is non-blocking.
func (c *WebSocketClient) Start() {
	// 1. Start the Message Processor (Runs once for the life of the client)
	go c.messageProcessor()

	// 2. Start the Connection Supervisor (Manages reconnections)
	go c.connectionSupervisor()
}

func (c *WebSocketClient) Close() {
	c.cancel() // Signals everything to stop
}

func (c *WebSocketClient) Reconnect() {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.conn != nil {
		c.log.Info("Forcing reconnection...")
		// Closing the connection causes readPump/writePump to error out.
		// connectAndServe will return, and the supervisor loop will retry.
		c.conn.Close()
	}
}

func (c *WebSocketClient) SendMessageJSON(ctx context.Context, msgType int, message interface{}) error {
	data, err := sonic.Marshal(message)
	if err != nil {
		return err
	}
	// Non-blocking send with drop logic to prevent backpressure blocking the caller
	select {
	case <-c.ctx.Done():
		// The client itself was closed/cancelled
		return fmt.Errorf("websocket client is closed")
	case <-ctx.Done():
		// The caller gave up (timeout)
		return ctx.Err()
	case c.send <- WsMessage{Type: msgType, Message: data}:
		// Success
		return nil
	default:
		c.log.Warn("SendMessageJSON: send channel full, dropping message")
		return fmt.Errorf("send channel full")
	}
}

func (c *WebSocketClient) SendMessage(ctx context.Context, message WsMessage) error {
	select {
	case <-c.ctx.Done():
		// The client itself was closed/cancelled
		return fmt.Errorf("websocket client is closed")
	case <-ctx.Done():
		// The caller gave up (timeout)
		return ctx.Err()
	case c.send <- message:
		// Success
		return nil
	}
}

func (c *WebSocketClient) TrySendMessage(message WsMessage) error {
	select {
	case <-c.ctx.Done():
		return fmt.Errorf("websocket client is closed")
	case c.send <- message:
		return nil
	default:
		c.log.Warn("TrySendMessage: send channel full, dropping message")
		return fmt.Errorf("send channel full")
	}
}

func (c *WebSocketClient) TrySendMessageJSON(msgType int, message interface{}) error {
	data, err := sonic.Marshal(message)
	if err != nil {
		return err
	}
	select {
	case <-c.ctx.Done():
		return fmt.Errorf("websocket client is closed")
	case c.send <- WsMessage{Type: msgType, Message: data}:
		// Success
		return nil
	default:
		c.log.Warn("TrySendMessage: send channel full, dropping message")
		return fmt.Errorf("send channel full")
	}
}

// -------------------------------------------------------------------------
// Core Supervisor Loop
// -------------------------------------------------------------------------

func (c *WebSocketClient) connectionSupervisor() {
	c.reconnectDelay = baseReconnectDelay

	for {
		select {
		case <-c.ctx.Done():
			return
		default:
			// Attempt connection
			if err := c.connectAndServe(); err != nil {
				c.log.Error("Connection lost/failed", "error", err, "retry_in", c.reconnectDelay.String())

				// Notify disconnect handler
				if c.onDisconnect != nil {
					c.onDisconnect()
				}

				// Backoff sleep
				select {
				case <-c.ctx.Done():
					return
				case <-time.After(c.reconnectDelay):
					c.reconnectDelay = min(maxReconnectDelay, c.reconnectDelay*2)
				}
			} else {
				c.reconnectDelay = baseReconnectDelay
			}
		}
	}
}

// connectAndServe establishes the connection and blocks until it fails.
func (c *WebSocketClient) connectAndServe() error {
	u, err := url.Parse(c.url)
	if err != nil {
		return err
	}

	conn, _, err := websocket.DefaultDialer.Dial(u.String(), nil)
	if err != nil {
		return err
	}

	// Setup Connection Properties
	conn.SetReadLimit(2 * 1024 * 1024) // 2MB
	conn.SetReadDeadline(time.Now().Add(pongWait))
	conn.SetPongHandler(func(string) error {
		conn.SetReadDeadline(time.Now().Add(pongWait))
		return nil
	})

	c.mu.Lock()
	c.conn = conn
	c.mu.Unlock()

	c.reconnectDelay = baseReconnectDelay

	c.log.Info("WebSocket Connected", "url", c.url)
	if c.onConnect != nil {
		if err := c.onConnect(); err != nil {
			c.log.Error("OnConnect handler failed", "error", err)
			// Don't kill connection just because handler failed, usually
		}
	}

	// Create a child context for this specific connection session
	// If either Read or Write pump fails, we cancel this context to kill the other.
	sessionCtx, cancelSession := context.WithCancel(c.ctx)
	defer func() {
		cancelSession()
		c.mu.Lock()
		if c.conn != nil {
			c.conn.Close()
			c.conn = nil
		}
		c.mu.Unlock()
	}()

	// Error channel to catch whichever pump fails first
	errChan := make(chan error, 2)

	go c.readPump(sessionCtx, conn, errChan)
	go c.writePump(sessionCtx, conn, errChan)

	// Block until one of the pumps returns an error or context is canceled
	select {
	case <-c.ctx.Done():
		return nil
	case err := <-errChan:
		return err
	}
}

// -------------------------------------------------------------------------
// Pumps
// -------------------------------------------------------------------------

func (c *WebSocketClient) readPump(ctx context.Context, conn *websocket.Conn, errChan chan<- error) {
	for {
		// No select needed here because ReadMessage fails if connection closes
		msgType, message, err := conn.ReadMessage()
		if err != nil {
			errChan <- fmt.Errorf("read error: %w", err)
			return
		}

		select {
		case c.receive <- WsMessage{Type: msgType, Message: message}:
		case <-ctx.Done():
			return
		}
	}
}

func (c *WebSocketClient) writePump(ctx context.Context, conn *websocket.Conn, errChan chan<- error) {
	ticker := time.NewTicker(pingPeriod)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return

		case message := <-c.send:
			conn.SetWriteDeadline(time.Now().Add(writeWait))
			if err := conn.WriteMessage(message.Type, message.Message); err != nil {
				errChan <- fmt.Errorf("write error: %w", err)
				return
			}

		case <-ticker.C:
			conn.SetWriteDeadline(time.Now().Add(writeWait))
			if err := conn.WriteMessage(websocket.PingMessage, nil); err != nil {
				errChan <- fmt.Errorf("ping error: %w", err)
				return
			}
		}
	}
}

// -------------------------------------------------------------------------
// Consumers
// -------------------------------------------------------------------------

func (c *WebSocketClient) messageProcessor() {
	for {
		select {
		case <-c.ctx.Done():
			return
		case msg := <-c.receive:
			if c.onMessage != nil {
				// NOTE: This blocks the read pump if onMessage is slow.
				c.onMessage(msg)
			}
		}
	}
}

// Setters
func (ws *WebSocketClient) SetLogger(logger *slog.Logger) {
	ws.mu.Lock()
	defer ws.mu.Unlock()
	ws.log = logger
}

func (ws *WebSocketClient) SetOnConnect(handler func() error) {
	ws.mu.Lock()
	defer ws.mu.Unlock()
	ws.onConnect = handler
}

func (ws *WebSocketClient) SetOnDisconnect(handler func() error) {
	ws.mu.Lock()
	defer ws.mu.Unlock()
	ws.onDisconnect = handler
}

func (ws *WebSocketClient) SetMessageHandler(handler WsMessageHandler) {
	ws.mu.Lock()
	defer ws.mu.Unlock()
	ws.onMessage = handler
}
