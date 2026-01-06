package internal

import (
	"encoding/json"
	"log/slog"
	"sync/atomic"
)

// LazyMessage holds the raw data. We serialize it ONLY if clients are connected.
type LazyMessage struct {
	Type int
	Data interface{}
}

type Hub struct {
	// Atomic counter for lock-free checks
	clientCount atomic.Int64

	// Registered clients.
	clients map[*Client]bool

	// Inbound messages (changed to accept LazyMessage)
	broadcast chan LazyMessage

	// Register requests from the clients.
	register chan *Client

	// Unregister requests from clients.
	unregister chan *Client
}

func newHub() *Hub {
	return &Hub{
		broadcast:  make(chan LazyMessage),
		register:   make(chan *Client),
		unregister: make(chan *Client),
		clients:    make(map[*Client]bool),
	}
}

func (h *Hub) run() {
	for {
		select {
		case client := <-h.register:
			h.clients[client] = true
			h.clientCount.Add(1) // Increment atomic counter

		case client := <-h.unregister:
			if _, ok := h.clients[client]; ok {
				delete(h.clients, client)
				close(client.send)
				h.clientCount.Add(-1) // Decrement atomic counter
			}

		case msg := <-h.broadcast:
			// 1. CHEAP CHECK: If no clients, skip everything.
			// This avoids the expensive JSON serialization below.
			if len(h.clients) == 0 {
				continue
			}

			// 2. SERIALIZE: Only now do we pay the CPU cost.
			// We handle []byte and arbitrary structs differently.
			var payload []byte
			var err error

			switch v := msg.Data.(type) {
			case []byte:
				payload = v // Already bytes, pass through
			default:
				payload, err = json.Marshal(v) // Costly operation
				if err != nil {
					slog.Error("Hub: Failed to serialize message", "error", err)
					continue
				}
			}

			// 3. BROADCAST: Send the pre-serialized bytes to all clients
			// We wrap it in the WsMessage struct expected by the Client writePump
			messageToSend := WsMessage{
				Type:    msg.Type,
				Message: payload,
			}

			for client := range h.clients {
				select {
				case client.send <- messageToSend:
				default:
					close(client.send)
					delete(h.clients, client)
					h.clientCount.Add(-1)
				}
			}
		}
	}
}
