# ==========================================
# STAGE 1: Builder
# ==========================================
# Updated to match the Go 1.26 version running on your host machine.
FROM golang:1.26-alpine AS builder

WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download 
COPY . .

# Compile the Go application statically
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-w -s" -o /worker main.go

# ==========================================
# STAGE 2: Final Runtime
# ==========================================
FROM gcr.io/distroless/static-debian12:nonroot

COPY --from=builder /worker /worker

USER nonroot:nonroot

ENTRYPOINT ["/worker"]