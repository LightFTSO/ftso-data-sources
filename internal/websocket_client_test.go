package internal

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/stretchr/testify/assert"
)

var upgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool {
		return true
	},
}

func echoHandler(w http.ResponseWriter, r *http.Request) {
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		return
	}
	defer conn.Close()
	for {
		mt, message, err := conn.ReadMessage()
		if err != nil {
			break
		}
		err = conn.WriteMessage(mt, message)
		if err != nil {
			break
		}
	}
}

func newTestServer() *httptest.Server {
	return httptest.NewServer(http.HandlerFunc(echoHandler))
}

func TestWebSocketClient(t *testing.T) {
	server := newTestServer()
	defer server.Close()

	wsURL := "ws" + server.URL[4:]

	client := NewWebSocketClient(wsURL)

	connectCalled := false
	disconnectCalled := false

	client.SetOnConnect(func() error {
		connectCalled = true
		return nil
	})

	client.SetOnDisconnect(func() error {
		disconnectCalled = true
		return nil
	})

	go client.Start()

	// Wait for connection
	time.Sleep(1 * time.Second)
	assert.True(t, connectCalled, "onConnect should be called")

	// Send and receive a message
	testMessage := []byte("test message")
	client.SendMessage(WsMessage{Type: websocket.TextMessage, Message: testMessage})

	select {
	case receivedMessage := <-client.receive:
		assert.Equal(t, testMessage, receivedMessage, "Received message should match sent message")
	case <-time.After(2 * time.Second):
		t.Fatal("Did not receive message in time")
	}

	// Test disconnect and reconnect
	client.Reconnect()

	// Wait for reconnection
	time.Sleep(2 * time.Second)
	assert.True(t, disconnectCalled, "onDisconnect should be called")
	assert.True(t, connectCalled, "onConnect should be called after reconnect")

	client.Close()
}

func TestWebSocketClient_Close(t *testing.T) {
	server := newTestServer()
	defer server.Close()

	wsURL := "ws" + server.URL[4:]

	client := NewWebSocketClient(wsURL)

	connectCalled := false
	disconnectCalled := false

	client.SetOnConnect(func() error {
		connectCalled = true
		return nil
	})

	client.SetOnDisconnect(func() error {
		disconnectCalled = true
		return nil
	})

	go client.Start()

	// Wait for connection
	time.Sleep(1 * time.Second)
	assert.True(t, connectCalled, "onConnect should be called")

	client.Close()

	// Wait for disconnection
	time.Sleep(1 * time.Second)
	assert.True(t, disconnectCalled, "onDisconnect should be called")
}
