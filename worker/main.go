package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/segmentio/kafka-go"
)

func main() {
	kafkaBroker := os.Getenv("KAFKA_BROKERS")
	if kafkaBroker == "" {
		kafkaBroker = "localhost:9092"
	}

	// Initialize the Kafka Reader (Consumer)
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers: []string{kafkaBroker},
		Topic:   "telemetry_events",
		// The GroupID is critical. It tracks which messages this specific application has already read.
		GroupID: "telemetry-processor-group",
		// If the consumer group is brand new, start reading from the oldest available message.
		StartOffset: kafka.FirstOffset,
	})

	log.Println("[INFO] Worker started. Listening for telemetry events...")

	// Create a context that listens for system interrupt signals (Ctrl+C)
	// This allows us to shut down the worker gracefully without dropping messages in-flight.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	// Start a goroutine to handle shutdown signals
	go func() {
		<-sigChan
		log.Println("\n[INFO] Shutdown signal received. Closing Kafka reader...")
		cancel()
		r.Close()
		os.Exit(0)
	}()

	// Infinite loop to continuously poll Kafka for new messages
	for {
		// ReadMessage blocks until a new message is available or the context is cancelled
		msg, err := r.ReadMessage(ctx)
		if err != nil {
			// If context was cancelled via Ctrl+C, exit the loop cleanly
			if ctx.Err() != nil {
				break
			}
			log.Printf("[ERROR] Failed to read message: %v\n", err)
			continue
		}

		// In a production system, this is where you would process the message
		// (e.g., aggregate it, write it to a database, trigger an alert).
		// For now, we prove we received it by logging it.
		log.Printf("[WORKER] Received Event | Key (User): %s | Payload: %s\n", string(msg.Key), string(msg.Value))
	}
}
