# Multi-stage build for Linux binary
FROM rust:latest as builder

WORKDIR /build

# Install protobuf compiler
RUN apt-get update && \
    apt-get install -y protobuf-compiler && \
    rm -rf /var/lib/apt/lists/*

# Copy workspace
COPY Cargo.toml Cargo.lock ./
COPY crates ./crates

# CACHE BUSTER: Add build timestamp to force rebuild
ARG CACHEBUST=1
RUN echo "Build timestamp: ${CACHEBUST}"

# Build release binary
RUN cargo build --release --bin prkdb-server

# Runtime image
FROM debian:bookworm-slim

# Install runtime dependencies
RUN apt-get update && \
    apt-get install -y ca-certificates curl && \
    rm -rf /var/lib/apt/lists/*

# Copy binary from builder
COPY --from=builder /build/target/release/prkdb-server /usr/local/bin/prkdb-server

# Create data directory
RUN mkdir -p /data

# Expose Raft port
EXPOSE 50000

# Set working directory
WORKDIR /data

# Health check
HEALTHCHECK --interval=5s --timeout=3s --start-period=10s --retries=3 \
  CMD pgrep prkdb-server || exit 1

# Run server
CMD ["prkdb-server"]
