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
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	// OpenTelemetry Imports
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
)

// Metrics Definitions (From Project 19)
var (
	httpRequestsTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{Name: "http_requests_total", Help: "Total requests"},
		[]string{"method", "endpoint", "status"},
	)
	httpRequestDuration = promauto.NewHistogramVec(
		prometheus.HistogramOpts{Name: "http_request_duration_seconds", Help: "Latency"},
		[]string{"method", "endpoint"},
	)
)

// initTracer configures OpenTelemetry to send spans to Jaeger
func initTracer(serviceName, endpoint string) *sdktrace.TracerProvider {
	exporter, err := otlptracehttp.New(context.Background(),
		otlptracehttp.WithEndpoint(endpoint),
		otlptracehttp.WithInsecure(),
	)
	if err != nil {
		log.Fatalf("Failed to create OTLP exporter: %v", err)
	}
	
	tp := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exporter),
		sdktrace.WithResource(resource.NewWithAttributes(
			"",
			attribute.String("service.name", serviceName),
		)),
	)
	otel.SetTracerProvider(tp)
	// This tells OTel how to inject/extract Trace IDs into network headers
	otel.SetTextMapPropagator(propagation.TraceContext{})
	return tp
}

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
	start := time.Now()
	defer func() {
		duration := time.Since(start).Seconds()
		httpRequestDuration.WithLabelValues(r.Method, "/ingest").Observe(duration)
	}()

	// 1. Start an OpenTelemetry Span for the HTTP Request
	ctx := r.Context()
	tracer := otel.Tracer("telemetry-api")
	ctx, span := tracer.Start(ctx, "POST /ingest")
	defer span.End()

	if r.Method != http.MethodPost {
		httpRequestsTotal.WithLabelValues(r.Method, "/ingest", "405").Inc()
		http.Error(w, "Method Not Allowed", http.StatusMethodNotAllowed)
		return
	}

	var event TelemetryEvent
	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&event); err != nil {
		httpRequestsTotal.WithLabelValues(r.Method, "/ingest", "400").Inc()
		http.Error(w, "Bad Request", http.StatusBadRequest)
		return
	}

	// Add custom attributes to the trace (e.g., the User ID)
	span.SetAttributes(attribute.String("user.id", event.UserID))
	span.SetAttributes(attribute.String("event.type", event.EventType))

	// Cache Check
	if h.redis != nil {
		_, err := h.redis.Get(ctx, "user_valid:"+event.UserID).Result()
		if err == redis.Nil {
			log.Printf("[WARN] Cache miss for user %s", event.UserID)
		}
	}

	// Kafka Producer
	if h.kafkaWriter != nil {
		payloadBytes, _ := json.Marshal(event)

		// 2. CONTEXT PROPAGATION
		// We inject the current Trace ID into a map of strings
		headers := make(map[string]string)
		otel.GetTextMapPropagator().Inject(ctx, propagation.MapCarrier(headers))
		
		// Convert the map into Kafka-native headers
		var kafkaHeaders []kafka.Header
		for k, v := range headers {
			kafkaHeaders = append(kafkaHeaders, kafka.Header{Key: k, Value: []byte(v)})
		}

		msg := kafka.Message{
			Key:     []byte(event.UserID),
			Value:   payloadBytes,
			Headers: kafkaHeaders, // Stamp the message with the Trace ID!
		}

		if err := h.kafkaWriter.WriteMessages(ctx, msg); err != nil {
			span.RecordError(err) // Attach the error to the trace graph
			httpRequestsTotal.WithLabelValues(r.Method, "/ingest", "500").Inc()
			http.Error(w, "Internal Server Error", http.StatusInternalServerError)
			return
		}
	}

	httpRequestsTotal.WithLabelValues(r.Method, "/ingest", "202").Inc()
	w.WriteHeader(http.StatusAccepted)
	_, _ = w.Write([]byte(`{"status": "accepted"}`))
}

func main() {
	// Initialize Jaeger Tracing
	jaegerURL := os.Getenv("JAEGER_ENDPOINT")
	if jaegerURL == "" {
		jaegerURL = "localhost:4318"
	}
	tp := initTracer("telemetry-api", jaegerURL)
	defer func() { _ = tp.Shutdown(context.Background()) }()

	// Connect Redis & Kafka
	redisAddr := os.Getenv("REDIS_URL")
	if redisAddr == "" { redisAddr = "localhost:6379" }
	rdb := redis.NewClient(&redis.Options{Addr: redisAddr})

	kafkaBroker := os.Getenv("KAFKA_BROKERS")
	if kafkaBroker == "" { kafkaBroker = "localhost:9092" }
	kw := &kafka.Writer{
		Addr:     kafka.TCP(kafkaBroker),
		Topic:    "telemetry_events",
		Balancer: &kafka.Hash{},
	}
	defer kw.Close()

	app := &apiHandler{redis: rdb, kafkaWriter: kw}

	mux := http.NewServeMux()
	mux.HandleFunc("/ingest", app.handleIngest)
	mux.Handle("/metrics", promhttp.Handler())

	server := &http.Server{Addr: ":8080", Handler: mux}
	log.Println("[INFO] Starting API server on :8080 (Traced via OTel)")
	server.ListenAndServe()
}