package main

import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"testing"
)

// TestHandleIngest_Success tests the happy path of our API
func TestHandleIngest_Success(t *testing.T) {
	// 1. Setup the handler
	app := &apiHandler{}

	// 2. Create a mock JSON payload
	payload := []byte(`{"user_id": "test_user", "event_type": "login", "timestamp": "2026-04-15T12:00:00Z"}`)

	// 3. Create an incoming HTTP Request
	// httptest.NewRequest allows us to forge a request without starting a real server
	req := httptest.NewRequest(http.MethodPost, "/ingest", bytes.NewBuffer(payload))

	// 4. Create a ResponseRecorder to capture the handler's output
	rr := httptest.NewRecorder()

	// 5. Execute the handler directly in memory
	app.handleIngest(rr, req)

	// 6. Assert the results
	if status := rr.Code; status != http.StatusAccepted {
		t.Errorf("handler returned wrong status code: got %v want %v", status, http.StatusAccepted)
	}

	expectedBody := `{"status": "accepted"}`
	if rr.Body.String() != expectedBody {
		t.Errorf("handler returned unexpected body: got %v want %v", rr.Body.String(), expectedBody)
	}
}

// TestHandleIngest_MethodNotAllowed ensures we reject GET requests
func TestHandleIngest_MethodNotAllowed(t *testing.T) {
	app := &apiHandler{}

	// Forge a GET request instead of a POST
	req := httptest.NewRequest(http.MethodGet, "/ingest", nil)
	rr := httptest.NewRecorder()

	app.handleIngest(rr, req)

	if status := rr.Code; status != http.StatusMethodNotAllowed {
		t.Errorf("handler returned wrong status code for GET request: got %v want %v", status, http.StatusMethodNotAllowed)
	}
}

// TestHandleIngest_InvalidJSON ensures we catch malformed payloads
func TestHandleIngest_InvalidJSON(t *testing.T) {
	app := &apiHandler{}

	// Malformed JSON (missing closing brace)
	payload := []byte(`{"user_id": "test_user"`)
	req := httptest.NewRequest(http.MethodPost, "/ingest", bytes.NewBuffer(payload))
	rr := httptest.NewRecorder()

	app.handleIngest(rr, req)

	if status := rr.Code; status != http.StatusBadRequest {
		t.Errorf("handler returned wrong status code for invalid JSON: got %v want %v", status, http.StatusBadRequest)
	}
}
