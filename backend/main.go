package main

import (
	"context"
	"encoding/json"
	"log"
	"net/http"
	"os"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
)

type TelemetryEvent struct {
	UserID    string `json:"user_id"`
	EventType string `json:"event_type"`
	Timestamp string `json:"timestamp"`
}

// apiHandler now holds both our Database Pool and our Redis Client
type apiHandler struct {
	db    *pgxpool.Pool
	redis *redis.Client
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
	// CACHE-ASIDE PATTERN (User Validation)
	// ==========================================
	if h.redis != nil && h.db != nil {
		ctx := r.Context()
		cacheKey := "user_valid:" + event.UserID

		// 1. Check Redis Cache
		_, err := h.redis.Get(ctx, cacheKey).Result()

		if err == redis.Nil {
			// CACHE MISS: Redis doesn't know this user. We must query Postgres.
			log.Printf("[CACHE MISS] Validating user %s against DB\n", event.UserID)

			var exists int
			// Query the DB. If the UUID is invalid or doesn't exist, this returns an error.
			err = h.db.QueryRow(ctx, "SELECT 1 FROM users WHERE id = $1", event.UserID).Scan(&exists)

			if err != nil {
				log.Printf("[WARN] Unauthorized ingest attempt for user: %s\n", event.UserID)
				http.Error(w, "Unauthorized: Invalid User ID", http.StatusUnauthorized)
				return
			}

			// DB HIT: User is valid! Save this fact in Redis for 5 minutes.
			err = h.redis.Set(ctx, cacheKey, "true", 5*time.Minute).Err()
			if err != nil {
				log.Printf("[WARN] Failed to write to Redis: %v\n", err) // Non-fatal error
			}
		} else if err != nil {
			log.Printf("[ERROR] Redis connection failed: %v\n", err)
			// In a highly available system, if Redis goes down, we might choose to
			// fallback to DB or fail the request. We will fail softly and log it.
		} else {
			// CACHE HIT: We got the data from RAM! Skip the DB read.
			// (Silent to avoid log spam, but it is working!)
		}

		// ==========================================
		// DATABASE WRITE (Append Ledger)
		// ==========================================
		payloadBytes, _ := json.Marshal(event)

		_, err = h.db.Exec(ctx,
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
	// 1. Connect to PostgreSQL
	dbURL := os.Getenv("DATABASE_URL")
	if dbURL == "" {
		dbURL = "postgres://admin:secretpassword@localhost:5432/telemetry"
	}
	pool, err := pgxpool.New(context.Background(), dbURL)
	if err != nil {
		log.Fatalf("[FATAL] Unable to connect to database: %v\n", err)
	}
	defer pool.Close()

	if err := pool.Ping(context.Background()); err != nil {
		log.Fatalf("[FATAL] Database ping failed: %v\n", err)
	}
	log.Println("[INFO] Successfully connected to PostgreSQL")

	// 2. Connect to Redis
	redisAddr := os.Getenv("REDIS_URL")
	if redisAddr == "" {
		redisAddr = "localhost:6379"
	}
	rdb := redis.NewClient(&redis.Options{
		Addr: redisAddr,
	})

	if err := rdb.Ping(context.Background()).Err(); err != nil {
		log.Fatalf("[FATAL] Redis ping failed. Is Redis running?: %v\n", err)
	}
	log.Println("[INFO] Successfully connected to Redis Cache")

	// Inject both connections into the handler
	app := &apiHandler{
		db:    pool,
		redis: rdb,
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
