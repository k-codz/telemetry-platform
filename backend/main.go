package main

import (
	"encoding/json"
	"log"
	"net/http"
	"time"
)

// TelemetryEvent defines the JSON structure we expect from our clients.
// The struct tags `json:"..."` map the JSON keys to our Go fields.
type TelemetryEvent struct {
	UserID    string `json:"user_id"`
	EventType string `json:"event_type"`
	Timestamp string `json:"timestamp"`
}

// apiHandler acts as the receiver for our HTTP routes.
// As we advance, we will inject dependencies (like DB connections or Kafka producers) into this struct.
type apiHandler struct {
	// e.g., db *sql.DB
}

// ServeHTTP satisfies the http.Handler interface.
// Because it's an interface method, we can map this directly to our routes.
func (h *apiHandler) handleIngest(w http.ResponseWriter, r *http.Request) {
	// 1. Validate the HTTP Method
	if r.Method != http.MethodPost {
		http.Error(w, "Method Not Allowed", http.StatusMethodNotAllowed)
		return
	}

	// 2. Decode the JSON payload
	// We use json.NewDecoder reading directly from the Request Body (an io.Reader).
	// This streams the data, which is highly memory-efficient for large payloads,
	// rather than loading the whole payload into a byte slice first.
	var event TelemetryEvent
	decoder := json.NewDecoder(r.Body)
	// We enforce disallowing unknown fields to ensure payload hygiene
	decoder.DisallowUnknownFields()

	if err := decoder.Decode(&event); err != nil {
		log.Printf("[ERROR] Failed to decode JSON: %v\n", err)
		http.Error(w, "Bad Request: Invalid JSON payload", http.StatusBadRequest)
		return
	}

	// 3. Process the Event (Simulated)
	// Currently, we just log it. Later, we will push this to a Kafka topic.
	log.Printf("[INFO] Received Event | User: %s | Type: %s | Time: %s\n", event.UserID, event.EventType, event.Timestamp)

	// 4. Send the Response
	// HTTP 202 Accepted signifies the request has been accepted for processing,
	// but the processing has not been completed.
	w.WriteHeader(http.StatusAccepted)
	w.Write([]byte(`{"status": "accepted"}`))
}

func main() {
	// Initialize our handler wrapper
	app := &apiHandler{}

	// Define our routing multiplexer (the router)
	mux := http.NewServeMux()
	mux.HandleFunc("/ingest", app.handleIngest)

	// INDUSTRY BEST PRACTICE: Explicit Server Configuration
	// Never use the bare http.ListenAndServe(":8080", mux) in production.
	// It lacks timeouts, leaving you vulnerable to Slowloris attacks.
	server := &http.Server{
		Addr:         ":8080",
		Handler:      mux,
		ReadTimeout:  5 * time.Second,   // Max time to read the request (headers + body)
		WriteTimeout: 10 * time.Second,  // Max time to write the response
		IdleTimeout:  120 * time.Second, // Max time for connections using TCP Keep-Alive
	}

	log.Println("[INFO] Starting ingestion API server on :8080")
	if err := server.ListenAndServe(); err != nil {
		log.Fatalf("[FATAL] Server crashed: %v\n", err)
	}
}
