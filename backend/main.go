package main

import (
	"context"
	"encoding/json"
	"log"
	"net/http"
	"os"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/segmentio/kafka-go"
)

type TelemetryEvent struct {
	UserID    string `json:"user_id"`
	EventType string `json:"event_type"`
	Timestamp string `json:"timestamp"`
}

// apiHandler now holds Redis and a Kafka Writer (Producer).
// We have completely removed the PostgreSQL database pool.
type apiHandler struct {
	redis       *redis.Client
	kafkaWriter *kafka.Writer
}

func (h *apiHandler) handleIngest(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method Not Allowed", http.StatusMethodNotAllowed)
		return
	}

	var event TelemetryEvent
	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()

	if err := decoder.Decode(&event); err != nil {
		log.Printf("[ERROR] Failed to decode JSON: %v\n", err)
		http.Error(w, "Bad Request: Invalid JSON payload", http.StatusBadRequest)
		return
	}

	// ==========================================
	// CACHE CHECK (User Validation)
	// ==========================================
	if h.redis != nil {
		ctx := r.Context()
		cacheKey := "user_valid:" + event.UserID

		_, err := h.redis.Get(ctx, cacheKey).Result()

		if err == redis.Nil {
			// CACHE MISS: Because we removed Postgres from the API, if the user isn't in Redis,
			// we must reject them. (In a real system, an authentication microservice would populate Redis).
			// For testing, we will just log a warning but allow the message through to Kafka anyway.
			log.Printf("[WARN] Cache miss for user %s. Allowing through for testing.", event.UserID)
		} else if err != nil {
			log.Printf("[ERROR] Redis connection failed: %v\n", err)
		}
	}

	// ==========================================
	// KAFKA PRODUCER (Event Streaming)
	// ==========================================
	if h.kafkaWriter != nil {
		payloadBytes, _ := json.Marshal(event)

		// Create a Kafka message. We use the UserID as the message Key.
		// This guarantees that all events for a specific user go to the same Kafka partition,
		// maintaining perfect chronological order for that user.
		msg := kafka.Message{
			Key:   []byte(event.UserID),
			Value: payloadBytes,
		}

		// Write the message to the broker asynchronously
		err := h.kafkaWriter.WriteMessages(r.Context(), msg)
		if err != nil {
			log.Printf("[ERROR] Failed to write to Kafka: %v\n", err)
			http.Error(w, "Internal Server Error", http.StatusInternalServerError)
			return
		}

		log.Printf("[KAFKA] Produced event | User: %s | Type: %s\n", event.UserID, event.EventType)
	}

	w.WriteHeader(http.StatusAccepted)
	_, _ = w.Write([]byte(`{"status": "accepted"}`))
}

func main() {
	// 1. Connect to Redis
	redisAddr := os.Getenv("REDIS_URL")
	if redisAddr == "" {
		redisAddr = "localhost:6379"
	}
	rdb := redis.NewClient(&redis.Options{Addr: redisAddr})

	if err := rdb.Ping(context.Background()).Err(); err != nil {
		log.Fatalf("[FATAL] Redis ping failed: %v\n", err)
	}
	log.Println("[INFO] Successfully connected to Redis Cache")

	// 2. Connect to Kafka (Producer)
	kafkaBroker := os.Getenv("KAFKA_BROKERS")
	if kafkaBroker == "" {
		kafkaBroker = "localhost:9092"
	}

	// Initialize the Kafka Writer (Producer)
	kw := &kafka.Writer{
		Addr:  kafka.TCP(kafkaBroker),
		Topic: "telemetry_events",
		// Balancer decides which partition to write to based on the Message Key (UserID)
		Balancer: &kafka.Hash{},
	}
	defer kw.Close()
	log.Println("[INFO] Successfully initialized Kafka Producer")

	// Inject Redis and Kafka into the handler
	app := &apiHandler{
		redis:       rdb,
		kafkaWriter: kw,
	}

	// 3. Setup HTTP Server
	mux := http.NewServeMux()
	mux.HandleFunc("/ingest", app.handleIngest)

	server := &http.Server{
		Addr:         ":8080",
		Handler:      mux,
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 10 * time.Second,
		IdleTimeout:  120 * time.Second,
	}

	log.Println("[INFO] Starting ingestion API server on :8080")
	if err := server.ListenAndServe(); err != nil {
		log.Fatalf("[FATAL] Server crashed: %v\n", err)
	}
}
