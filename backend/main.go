package main

import (
	"context"
	"encoding/json"
	"log"
	"net/http"
	"os"
	"time"

	// Import the robust pgx connection pool driver
	"github.com/jackc/pgx/v5/pgxpool"
)

type TelemetryEvent struct {
	UserID    string `json:"user_id"`
	EventType string `json:"event_type"`
	Timestamp string `json:"timestamp"`
}

// apiHandler now holds our thread-safe database connection pool
type apiHandler struct {
	db *pgxpool.Pool
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

	occurredAt, err := time.Parse(time.RFC3339, event.Timestamp)
	if err != nil {
		occurredAt = time.Now()
	}

	// ==========================================
	// DATABASE WRITE LOGIC
	// ==========================================
	// We only attempt to write if the DB pool is configured (protects our unit tests)
	if h.db != nil {
		// Serialize the entire struct to dump into our flexible JSONB column
		payloadBytes, _ := json.Marshal(event)

		// r.Context() links this query to the HTTP request lifecycle.
		// Prepared statements ($1, $2) guarantee protection from SQL injection.
		_, err := h.db.Exec(r.Context(),
			"INSERT INTO events (user_id, event_type, payload, occurred_at) VALUES ($1, $2, $3, $4)",
			event.UserID, event.EventType, payloadBytes, occurredAt,
		)

		if err != nil {
			log.Printf("[ERROR] Database write failed: %v\n", err)
			http.Error(w, "Internal Server Error", http.StatusInternalServerError)
			return
		}
	}

	log.Printf("[INFO] Ingested Event | User: %s | Type: %s\n", event.UserID, event.EventType)

	w.WriteHeader(http.StatusAccepted)
	_, _ = w.Write([]byte(`{"status": "accepted"}`))
}

func main() {
	// 1. Establish Database Connection Pool
	// In a real environment, this is injected securely via GitHub Actions / Ansible
	dbURL := os.Getenv("DATABASE_URL")
	if dbURL == "" {
		dbURL = "postgres://admin:secretpassword@localhost:5432/telemetry"
	}

	// Initialize the connection pool with default settings (auto-manages active connections)
	pool, err := pgxpool.New(context.Background(), dbURL)
	if err != nil {
		log.Fatalf("[FATAL] Unable to connect to database: %v\n", err)
	}
	defer pool.Close() // Ensure connections are closed gracefully on server shutdown

	// Actively ping the database to guarantee it is online before starting the API
	if err := pool.Ping(context.Background()); err != nil {
		log.Fatalf("[FATAL] Database ping failed. Is PostgreSQL running?: %v\n", err)
	}

	log.Println("[INFO] Successfully connected to PostgreSQL connection pool")

	// Inject the pool into our handler
	app := &apiHandler{db: pool}

	// 2. Setup HTTP Server
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
