package consumer_test

import (
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"roselabs.mx/ftso-data-sources/model"
	"roselabs.mx/ftso-data-sources/tickertopic"
)

// ---------------------------------------------------------
// 1. The "Victim" Consumer (Simulates Redis)
// ---------------------------------------------------------
// It expects the timestamp to ALWAYS be the Exchange Timestamp.
// If it sees "System Time" (Now), it flags an error.
type MockReadOnlyConsumer struct {
	TickerCount    int64 // Use atomic for thread safety in counters
	CorruptedCount int64
	ExpectedTime   time.Time
	wg             *sync.WaitGroup
}

func (m *MockReadOnlyConsumer) Listen(topic *tickertopic.TickerTopic, totalMessages int) {
	listener := topic.Broadcaster.Listen()

	go func() {
		// Ensure we clean up if the test exits early
		defer listener.Discard()

		for t := range listener.Channel() {
			ticker := t.(*model.Ticker)

			// CHECK FOR CORRUPTION
			// If the timestamp doesn't match what we expect, the other consumer dirtied it.
			if !ticker.Timestamp.Equal(m.ExpectedTime) {
				atomic.AddInt64(&m.CorruptedCount, 1)
			}

			newCount := atomic.AddInt64(&m.TickerCount, 1)

			// Signal done only when we receive ALL messages
			if int(newCount) == totalMessages {
				m.wg.Done()
				return
			}
		}
	}()
}

// ---------------------------------------------------------
// 2. The "Offender" Consumer (Simulates Buggy Websocket)
// ---------------------------------------------------------
// It ruthlessly overwrites the timestamp on every received pointer.
type MockWritingConsumer struct {
	wg *sync.WaitGroup
}

func (m *MockWritingConsumer) Listen(topic *tickertopic.TickerTopic, totalMessages int) {
	listener := topic.Broadcaster.Listen()
	go func() {
		defer listener.Discard()

		count := 0
		for t := range listener.Channel() {
			ticker := t.(*model.Ticker)

			tickerCopy := *ticker
			tickerCopy.Timestamp = time.Now().UTC()
			// SIMULATE THE BUG: Direct pointer modification
			//ticker.Timestamp = time.Now().UTC()

			count++
			if count == totalMessages {
				m.wg.Done()
				return
			}
		}
	}()
}

// ---------------------------------------------------------
// 3. The Test Runner
// ---------------------------------------------------------
func TestConsumerIsolation_DataIntegrity(t *testing.T) {
	// CONFIG
	totalMessages := 5000
	queueCapacity := 1000 // Give it some buffer so it runs fast
	exchangeTime := time.Date(2023, 1, 1, 0, 0, 0, 0, time.UTC)

	// 1. Setup Topic
	topic := tickertopic.NewTickerTopic([]tickertopic.TransformationOptions{}, queueCapacity)

	// 2. Setup Consumers
	var wg sync.WaitGroup
	wg.Add(2) // We wait for BOTH consumers to finish processing 5000 items

	// The Victim (Redis)
	roConsumer := &MockReadOnlyConsumer{
		ExpectedTime: exchangeTime,
		wg:           &wg,
	}
	roConsumer.Listen(topic, totalMessages)

	// The Offender (Websocket)
	wConsumer := &MockWritingConsumer{
		wg: &wg,
	}
	wConsumer.Listen(topic, totalMessages)

	// 3. Broadcast Loop
	fmt.Println("Starting broadcast...")
	for i := 0; i < totalMessages; i++ {
		// CRITICAL: Create a NEW object every time.
		// If we reuse the pointer, we aren't testing the pipeline,
		// we are just racing against our own loop.
		newTicker := model.Ticker{
			Base:      "BTC",
			Quote:     "USD",
			Timestamp: exchangeTime, // All start clean
		}

		topic.Send(newTicker)

		// Optional: slight sleep to allow context switching
		// (increases chance of race condition manifesting)
		if i%100 == 0 {
			time.Sleep(1 * time.Microsecond)
		}
	}

	// 4. Wait for both consumers to finish reading
	fmt.Println("Broadcast done. Waiting for consumers...")
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	// Wait with timeout
	select {
	case <-done:
		fmt.Printf("Finished processing. Corrupted messages detected: %d\n", roConsumer.CorruptedCount)
	case <-time.After(5 * time.Second):
		t.Fatal("Test Timed Out! Consumers stopped receiving data.")
	}

	// 5. FINAL ASSERTION
	// We expect 0 corrupted messages.
	// If you run this with your CURRENT code, this will fail (likely > 0).
	// If you run this with the FIX (shallow copy), this will pass (0).
	assert.Equal(t, int64(0), roConsumer.CorruptedCount,
		"Data Integrity Failed! The ReadOnly consumer received data modified by the Writer.")
}
