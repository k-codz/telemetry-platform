package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/redis/go-redis/v9"
	"github.com/segmentio/kafka-go"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
)

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
	mlClient    *http.Client // Fast HTTP client for calling our Python ML service
}

func (h *apiHandler) handleIngest(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	defer func() {
		duration := time.Since(start).Seconds()
		httpRequestDuration.WithLabelValues(r.Method, "/ingest").Observe(duration)
	}()

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

	span.SetAttributes(attribute.String("user.id", event.UserID))
	span.SetAttributes(attribute.String("event.type", event.EventType))

	// ==========================================
	// 1. REDIS: Track User Event Count
	// ==========================================
	var eventCount int64 = 1
	if h.redis != nil {
		key := "daily_count:" + event.UserID
		// Increment the counter for this user
		eventCount, _ = h.redis.Incr(ctx, key).Result()
		if eventCount == 1 {
			// If it's a new counter, expire it after 24 hours
			h.redis.Expire(ctx, key, 24*time.Hour)
		}
	}

	// ==========================================
	// 2. MLOPS: Request Anomaly Prediction
	// ==========================================
	if h.mlClient != nil {
		// Create the JSON payload the FastAPI server expects
		mlReqBody := fmt.Sprintf(`{"total_events": %d}`, eventCount)

		// Make a fast RPC call to the Python service via internal K8s DNS
		mlResp, err := h.mlClient.Post("http://ml-service:8000/predict", "application/json", bytes.NewBufferString(mlReqBody))
		if err == nil {
			defer mlResp.Body.Close()
			var mlResult struct {
				IsAnomaly bool `json:"is_anomaly"`
			}
			if json.NewDecoder(mlResp.Body).Decode(&mlResult) == nil && mlResult.IsAnomaly {
				// THE AI HAS SPOKEN!
				log.Printf("🚨 [SECURITY ALERT] Machine Learning model flagged user %s as anomalous! (Event Count: %d)", event.UserID, eventCount)

				// Tag the OpenTelemetry trace so we can see the anomaly in Jaeger!
				span.SetAttributes(attribute.Bool("anomaly.detected", true))
			}
		} else {
			log.Printf("[WARN] Failed to reach ML Service: %v", err)
		}
	}

	// ==========================================
	// 3. KAFKA: Ingest the Event
	// ==========================================
	if h.kafkaWriter != nil {
		payloadBytes, _ := json.Marshal(event)

		headers := make(map[string]string)
		otel.GetTextMapPropagator().Inject(ctx, propagation.MapCarrier(headers))

		var kafkaHeaders []kafka.Header
		for k, v := range headers {
			kafkaHeaders = append(kafkaHeaders, kafka.Header{Key: k, Value: []byte(v)})
		}

		msg := kafka.Message{
			Key:     []byte(event.UserID),
			Value:   payloadBytes,
			Headers: kafkaHeaders,
		}

		if err := h.kafkaWriter.WriteMessages(ctx, msg); err != nil {
			span.RecordError(err)
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
	jaegerURL := os.Getenv("JAEGER_ENDPOINT")
	if jaegerURL == "" {
		jaegerURL = "localhost:4318"
	}
	tp := initTracer("telemetry-api", jaegerURL)
	defer func() { _ = tp.Shutdown(context.Background()) }()

	redisAddr := os.Getenv("REDIS_URL")
	if redisAddr == "" {
		redisAddr = "localhost:6379"
	}
	rdb := redis.NewClient(&redis.Options{Addr: redisAddr})

	kafkaBroker := os.Getenv("KAFKA_BROKERS")
	if kafkaBroker == "" {
		kafkaBroker = "localhost:9092"
	}
	kw := &kafka.Writer{
		Addr:     kafka.TCP(kafkaBroker),
		Topic:    "telemetry_events",
		Balancer: &kafka.Hash{},
	}
	defer func() { _ = kw.Close() }()

	// Initialize the fast HTTP client for our ML RPC calls (200ms timeout so we don't lag the API)
	mlHTTPClient := &http.Client{Timeout: 200 * time.Millisecond}

	app := &apiHandler{
		redis:       rdb,
		kafkaWriter: kw,
		mlClient:    mlHTTPClient,
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/ingest", app.handleIngest)
	mux.Handle("/metrics", promhttp.Handler())

	server := &http.Server{Addr: ":8080", Handler: mux}
	log.Println("[INFO] Starting API server on :8080 (Traced via OTel, ML Integrated)")
	if err := server.ListenAndServe(); err != nil {
		log.Fatalf("[FATAL] Server crashed: %v\n", err)
	}
}
