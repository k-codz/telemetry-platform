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
	
	// Prometheus metrics libraries
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// ==========================================
// PROMETHEUS METRICS DEFINITIONS
// ==========================================
var (
	// Counter: Tracks total HTTP requests broken down by method, path, and HTTP status code
	httpRequestsTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "http_requests_total",
			Help: "Total number of HTTP requests processed by the telemetry API",
		},
		[]string{"method", "endpoint", "status"},
	)

	// Histogram: Tracks the latency (duration) of HTTP requests
	httpRequestDuration = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "http_request_duration_seconds",
			Help:    "Latency of HTTP requests in seconds",
			Buckets: prometheus.DefBuckets, // Default buckets: 5ms, 10ms, 25ms, 50ms, 100ms, 250ms...
		},
		[]string{"method", "endpoint"},
	)
)

type TelemetryEvent struct {
	UserID    string `json:"user_id"`
	EventType string `json:"event_type"`
	Timestamp string `json:"timestamp"`
}

type apiHandler struct {
	redis       *redis.Client
	kafkaWriter *kafka.Writer
}

func (h *apiHandler) handleIngest(w http.ResponseWriter, r *http.Request) {
	// Start the latency timer!
	start := time.Now()
	
	// We use defer to ensure we ALWAYS record the duration, regardless of where the function exits
	defer func() {
		duration := time.Since(start).Seconds()
		httpRequestDuration.WithLabelValues(r.Method, "/ingest").Observe(duration)
	}()

	if r.Method != http.MethodPost {
		httpRequestsTotal.WithLabelValues(r.Method, "/ingest", "405").Inc()
		http.Error(w, "Method Not Allowed", http.StatusMethodNotAllowed)
		return
	}

	var event TelemetryEvent
	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()

	if err := decoder.Decode(&event); err != nil {
		log.Printf("[ERROR] Failed to decode JSON: %v\n", err)
		httpRequestsTotal.WithLabelValues(r.Method, "/ingest", "400").Inc()
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

		msg := kafka.Message{
			Key:   []byte(event.UserID),
			Value: payloadBytes,
		}

		err := h.kafkaWriter.WriteMessages(r.Context(), msg)
		if err != nil {
			log.Printf("[ERROR] Failed to write to Kafka: %v\n", err)
			httpRequestsTotal.WithLabelValues(r.Method, "/ingest", "500").Inc()
			http.Error(w, "Internal Server Error", http.StatusInternalServerError)
			return
		}
		
		log.Printf("[KAFKA] Produced event | User: %s | Type: %s\n", event.UserID, event.EventType)
	}

	// SUCCESS! Record a 202 status metric
	httpRequestsTotal.WithLabelValues(r.Method, "/ingest", "202").Inc()
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

	// 2. Connect to Kafka (Producer)
	kafkaBroker := os.Getenv("KAFKA_BROKERS")
	if kafkaBroker == "" {
		kafkaBroker = "localhost:9092"
	}

	kw := &kafka.Writer{
		Addr:     kafka.TCP(kafkaBroker),
		Topic:    "telemetry_events",
		Balancer: &kafka.Hash{},
	}
	defer kw.Close()

	app := &apiHandler{
		redis:       rdb,
		kafkaWriter: kw,
	}

	// 3. Setup HTTP Server
	mux := http.NewServeMux()
	
	// Add the ingestion endpoint
	mux.HandleFunc("/ingest", app.handleIngest)
	
	// NEW: Expose the Prometheus metrics endpoint!
	mux.Handle("/metrics", promhttp.Handler())

	server := &http.Server{
		Addr:         ":8080",
		Handler:      mux,
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 10 * time.Second,
		IdleTimeout:  120 * time.Second,
	}

	log.Println("[INFO] Starting ingestion API server on :8080 (Metrics on /metrics)")
	if err := server.ListenAndServe(); err != nil {
		log.Fatalf("[FATAL] Server crashed: %v\n", err)
	}
}