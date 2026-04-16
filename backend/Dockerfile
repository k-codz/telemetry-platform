# ==========================================
# STAGE 1: Builder
# ==========================================
# Use the official Golang image to build the binary.
# We tag this stage as 'builder' to reference it later.
FROM golang:1.26-alpine AS builder

# Set the working directory inside the container
WORKDIR /app

# Copy dependency files FIRST to leverage Docker layer caching.
# (If we don't have go.sum yet, it will safely ignore it or just copy go.mod)
COPY go.mod ./
# RUN go mod download (Uncomment this when you add external packages like pgx or redis)

# Copy the rest of the application source code
COPY . .

# Compile the Go application.
# CGO_ENABLED=0: Disable CGO for static compilation (no dependency on host libc/musl).
# GOOS=linux: Force compilation for Linux, ensuring the binary is compatible with the container OS.
# -ldflags="-w -s": Strip debugging information to further reduce binary size.
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-w -s" -o /api main.go

# ==========================================
# STAGE 2: Final Runtime
# ==========================================
# Use Google's distroless static image. It contains no OS utilities,
# making it incredibly secure and microscopic in size.
FROM gcr.io/distroless/static-debian12:nonroot

# Copy the compiled binary from the 'builder' stage into the final image.
COPY --from=builder /api /api

# Document the port the application listens on
EXPOSE 8080

# Run the binary as a non-root user (provided by the distroless image)
USER nonroot:nonroot

# Define the command to run when the container starts
ENTRYPOINT ["/api"]