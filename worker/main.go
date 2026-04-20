package main

import (
	"context"
	"encoding/json"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/segmentio/kafka-go"
)

type TelemetryEvent struct {
	UserID    string `json:"user_id"`
	EventType string `json:"event_type"`
	Timestamp string `json:"timestamp"`
}

func main() {
	// ==========================================
	// 1. Initialize PostgreSQL Connection Pool
	// ==========================================
	dbURL := os.Getenv("DATABASE_URL")
	if dbURL == "" {
		dbURL = "postgres://admin:secretpassword@localhost:5432/telemetry"
	}

	dbPool, err := pgxpool.New(context.Background(), dbURL)
	if err != nil {
		log.Fatalf("[FATAL] Unable to connect to database: %v\n", err)
	}
	defer dbPool.Close()

	if err := dbPool.Ping(context.Background()); err != nil {
		log.Fatalf("[FATAL] Database ping failed: %v\n", err)
	}
	log.Println("[INFO] Worker successfully connected to PostgreSQL")

	// ==========================================
	// 2. Initialize Kafka Consumer
	// ==========================================
	kafkaBroker := os.Getenv("KAFKA_BROKERS")
	if kafkaBroker == "" {
		kafkaBroker = "localhost:9092"
	}

	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers:     []string{kafkaBroker},
		Topic:       "telemetry_events",
		GroupID:     "telemetry-processor-group",
		StartOffset: kafka.FirstOffset,
	})

	log.Println("[INFO] Worker started. Listening for telemetry events...")

	// Setup Graceful Shutdown
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		<-sigChan
		log.Println("\n[INFO] Shutdown signal received. Closing connections...")
		cancel()
		r.Close()
		os.Exit(0)
	}()

	// ==========================================
	// 3. The Polling Loop
	// ==========================================
	for {
		msg, err := r.ReadMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				break // Exit cleanly on shutdown
			}
			log.Printf("[ERROR] Failed to read message: %v\n", err)
			continue
		}

		// Decode the JSON payload from Kafka
		var event TelemetryEvent
		if err := json.Unmarshal(msg.Value, &event); err != nil {
			// If a frontend developer sends broken JSON, we log it and skip it.
			// We DO NOT crash the worker, otherwise the queue gets permanently blocked!
			log.Printf("[WARN] Failed to parse message: %v. Payload: %s\n", err, string(msg.Value))
			continue
		}

		occurredAt, err := time.Parse(time.RFC3339, event.Timestamp)
		if err != nil {
			occurredAt = time.Now()
		}

		// Write to PostgreSQL
		_, err = dbPool.Exec(ctx,
			"INSERT INTO events (user_id, event_type, payload, occurred_at) VALUES ($1, $2, $3, $4)",
			event.UserID, event.EventType, msg.Value, occurredAt,
		)

		if err != nil {
			// In an enterprise system, we would send this to a "Dead Letter Queue" for manual review.
			log.Printf("[ERROR] Database write failed for user %s: %v\n", event.UserID, err)
			continue
		}

		log.Printf("[WORKER] Persisted Event to DB | User: %s | Type: %s\n", event.UserID, event.EventType)
	}
}
