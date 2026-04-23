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

	// OpenTelemetry Imports
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
)

type TelemetryEvent struct {
	UserID    string `json:"user_id"`
	EventType string `json:"event_type"`
	Timestamp string `json:"timestamp"`
}

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

func main() {
	// 1. Initialize Jaeger Tracing
	jaegerURL := os.Getenv("JAEGER_ENDPOINT")
	if jaegerURL == "" {
		jaegerURL = "localhost:4318"
	}
	tp := initTracer("telemetry-worker", jaegerURL)
	defer func() { _ = tp.Shutdown(context.Background()) }()

	// 2. Initialize PostgreSQL
	dbURL := os.Getenv("DATABASE_URL")
	if dbURL == "" {
		dbURL = "postgres://admin:secretpassword@localhost:5432/telemetry"
	}
	dbPool, err := pgxpool.New(context.Background(), dbURL)
	if err != nil { log.Fatalf("[FATAL] DB connection failed: %v", err) }
	defer dbPool.Close()

	// 3. Initialize Kafka Consumer
	kafkaBroker := os.Getenv("KAFKA_BROKERS")
	if kafkaBroker == "" { kafkaBroker = "localhost:9092" }
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers:     []string{kafkaBroker},
		Topic:       "telemetry_events",
		GroupID:     "telemetry-processor-group",
		StartOffset: kafka.FirstOffset,
	})

	log.Println("[INFO] Worker started. Listening for telemetry events...")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigChan
		cancel()
		r.Close()
		os.Exit(0)
	}()

	// 4. Polling Loop
	for {
		msg, err := r.ReadMessage(ctx)
		if err != nil {
			if ctx.Err() != nil { break }
			continue
		}

		// ==========================================
		// CONTEXT EXTRACTION
		// ==========================================
		// Read the Trace ID that the API injected into the Kafka headers
		headers := make(map[string]string)
		for _, h := range msg.Headers {
			headers[h.Key] = string(h.Value)
		}
		
		// Create a new context that is linked to the API's trace!
		traceCtx := otel.GetTextMapPropagator().Extract(context.Background(), propagation.MapCarrier(headers))
		tracer := otel.Tracer("telemetry-worker")
		
		// Start a child span
		traceCtx, span := tracer.Start(traceCtx, "Process Event & Insert DB")

		var event TelemetryEvent
		if err := json.Unmarshal(msg.Value, &event); err != nil {
			span.RecordError(err)
			span.End()
			continue 
		}

		span.SetAttributes(attribute.String("user.id", event.UserID))

		occurredAt, _ := time.Parse(time.RFC3339, event.Timestamp)

		// Write to PostgreSQL
		_, err = dbPool.Exec(traceCtx,
			"INSERT INTO events (user_id, event_type, payload, occurred_at) VALUES ($1, $2, $3, $4)",
			event.UserID, event.EventType, msg.Value, occurredAt,
		)

		if err != nil {
			span.RecordError(err)
		}

		// End the span indicating the worker finished processing this specific message
		span.End()
		log.Printf("[WORKER] Processed Event | User: %s", event.UserID)
	}
}