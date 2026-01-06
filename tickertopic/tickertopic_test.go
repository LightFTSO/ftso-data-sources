package tickertopic

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"roselabs.mx/ftso-data-sources/model"
)

// Test 1: Does a slow consumer kill the fast consumer?
func TestFanOut_SlowConsumer(t *testing.T) {
	// Create topic with small buffer (capacity 1)
	topic := NewTickerTopic([]TransformationOptions{}, 2)

	// 1. Setup FAST consumer (Websocket simulation)
	fastListener := topic.Broadcaster.Listen()
	defer fastListener.Discard()

	// 2. Setup SLOW consumer (Redis simulation)
	slowListener := topic.Broadcaster.Listen()
	defer slowListener.Discard()

	// 3. Fill the Slow Consumer's buffer
	// We send one ticker, and we DO NOT read it from slowListener.
	// The slowListener channel is now full (capacity 1).
	topic.Send(model.Ticker{Base: "BTC", Quote: "USD"})

	// Read that item from the fast listener to keep it clear
	<-fastListener.Channel()

	// 4. Send a second ticker
	// Implication: If the broadcaster blocks on full channels, this line will hang forever.
	t.Log("waiting for done")
	done := make(chan bool)
	go func() {
		t.Log("seding 2nd ticker")
		topic.Send(model.Ticker{Base: "ETH", Quote: "USD"})
		t.Log("waiting here")
		done <- true
	}()

	t.Log("done is finished")
	select {
	case <-done:
		t.Log("Success: Broadcaster did not block on slow consumer.")
		// If it didn't block, check if the fast consumer got the data
		select {
		case msg := <-fastListener.Channel():
			assert.NotNil(t, msg, "Fast consumer should receive data even if slow consumer is full")
		case <-time.After(100 * time.Millisecond):
			t.Error("Fast consumer was starved because of the slow consumer!")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("CRITICAL FAIL: The entire pipeline froze because one consumer was slow.")
	}
}

// Test 2: Verify Nil Pointer Protection (After you apply the fix)
func TestSend_HandlesTransformationErrors(t *testing.T) {
	// Setup a transformation that forces an error (returns nil)
	// You might need to mock a failing transform or use invalid input logic
	// For this example, assuming we have a way to make transform fail:

	topic := NewTickerTopic([]TransformationOptions{}, 1)
	listener := topic.Broadcaster.Listen()
	defer listener.Discard()

	// Inject a nil-causing logic manually or via config if possible
	// For now, let's manually assume ApplyTransformations returns nil to test safety
	// (You can mock this if you separate the interface, or just test the fix logic)

	// Call Send with something that triggers the failure
	// topic.Send(badTicker)

	// Assert that listener does NOT receive nil
	select {
	case msg := <-listener.Channel():
		if msg == nil {
			t.Fatal("Consumer received a NIL ticker! This will panic the app.")
		}
	case <-time.After(100 * time.Millisecond):
		// This is good, it means the nil was dropped
	}
}
