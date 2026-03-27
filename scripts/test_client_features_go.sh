#!/bin/bash
set -euo pipefail

# Configuration
WORK_DIR="/tmp/prkdb_client_features_go"
PRKDB_CMD="cargo run --quiet -p prkdb-cli --bin prkdb-cli --"
ADMIN_TOKEN="client_features_go_test_token"
DATABASE_PATH="$WORK_DIR/db"

reserve_port() {
    python3 -c 'import socket; s = socket.socket(); s.bind(("127.0.0.1", 0)); print(s.getsockname()[1]); s.close()'
}

SERVER_PORT=$(reserve_port)
GRPC_PORT=$(reserve_port)
while [ "$SERVER_PORT" = "$GRPC_PORT" ]; do
    GRPC_PORT=$(reserve_port)
done

# Cleanup function
cleanup() {
    echo "🧹 Cleaning up..."
    if [ -f "$WORK_DIR/server.pid" ]; then
        kill $(cat "$WORK_DIR/server.pid") 2>/dev/null || true
    fi
    rm -rf "$WORK_DIR"
}
trap cleanup EXIT

# Setup workspace
rm -rf "$WORK_DIR"
mkdir -p "$WORK_DIR"
mkdir -p "$WORK_DIR/client_go"

# Check dependencies
if ! command -v go &> /dev/null; then
    echo "❌ Go is required but not found."
    exit 1
fi

# Build binary
echo "🏗️  Building prkdb binary..."
cargo build -p prkdb-cli

# Start server
echo "🚀 Starting server on port $SERVER_PORT..."
PRKDB_ADMIN_TOKEN="$ADMIN_TOKEN" \
    $PRKDB_CMD --database "$DATABASE_PATH" --verbose serve --port $SERVER_PORT --grpc-port $GRPC_PORT > "$WORK_DIR/server.log" 2>&1 &
SERVER_PID=$!
echo $SERVER_PID > "$WORK_DIR/server.pid"
echo "Server PID: $SERVER_PID"

# Wait for server
echo "⏳ Waiting for server..."
for i in {1..30}; do
    if curl -sf "http://127.0.0.1:$SERVER_PORT/health" > /dev/null 2>&1 \
        && PRKDB_ADMIN_TOKEN="$ADMIN_TOKEN" $PRKDB_CMD schema --server "http://127.0.0.1:$GRPC_PORT" list > /dev/null 2>&1; then
        echo "✅ Server is ready!"
        break
    fi
    if [ $i -eq 30 ]; then
        echo "❌ Server failed to start. Check logs:"
        cat "$WORK_DIR/server.log"
        exit 1
    fi
    sleep 1
done

# Define Schema
echo "📝 Defining Schema..."
cat > "$WORK_DIR/user.proto" <<EOF
syntax = "proto3";
package user;

message User {
  string id = 1;
  string name = 2;
  int32 age = 3;
}
EOF

# Compile Schema to Descriptor Set
echo "🔨 Compiling Schema..."
protoc --include_imports --descriptor_set_out="$WORK_DIR/user.desc" --proto_path="$WORK_DIR" "$WORK_DIR/user.proto"

# Register Schema
echo "🚀 Registering Schema..."
PRKDB_ADMIN_TOKEN="$ADMIN_TOKEN" $PRKDB_CMD schema --server "http://127.0.0.1:$GRPC_PORT" register --collection users --proto "$WORK_DIR/user.desc"

# Insert Data via CLI
echo "💾 Inserting Test Data..."
$PRKDB_CMD --server "http://127.0.0.1:$GRPC_PORT" collection put users '{"id": "u1", "name": "Alice", "age": 30}'
$PRKDB_CMD --server "http://127.0.0.1:$GRPC_PORT" collection put users '{"id": "u2", "name": "Bob", "age": 25}'
$PRKDB_CMD --server "http://127.0.0.1:$GRPC_PORT" collection put users '{"id": "u3", "name": "Alice", "age": 35}'

# Generate Go Client
echo "⚙️  Generating Go Client..."
PRKDB_ADMIN_TOKEN="$ADMIN_TOKEN" $PRKDB_CMD codegen \
    --server "http://127.0.0.1:$GRPC_PORT" \
    --collection "users" \
    --lang go \
    --out "$WORK_DIR/client_go"

if [ ! -f "$WORK_DIR/client_go/users.go" ]; then
    echo "❌ Go client generation failed!"
    exit 1
fi

echo "✓ Code generation complete!"

# Create Go Module
cd "$WORK_DIR"
go mod init testclient

# Create Verification Script
echo "🏃 Verifying Go Client Features..."
cat > main.go <<EOF
package main

import (
    "fmt"
    "os"
    "testclient/client_go"
)

func main() {
    client := models.NewPrkDbClient("http://127.0.0.1:$SERVER_PORT")

    fmt.Println("Testing QueryBuilder...")

    // Test 1: Simple Equality
    fmt.Println("  - Filter by name='Alice'")
    
    // Use ListRaw with filter string
    results, err := client.ListRaw("users", models.ListOptions{
        Filter: "name=Alice",
    })
    
    if err != nil {
        fmt.Printf("❌ API request failed: %v\n", err)
        os.Exit(1)
    }

    fmt.Printf("    Found %d users\n", len(results))
    
    if len(results) != 2 {
        fmt.Printf("❌ Verification Failed: Expected 2 Alices, found %d\n", len(results))
        os.Exit(1)
    }

    for _, u := range results {
        if u["name"] != "Alice" {
             fmt.Printf("❌ Verification Failed: Expected 'Alice', got %v\n", u["name"])
             os.Exit(1)
        }
    }

    // Test 2: Simple Inequality (Bob)
    fmt.Println("  - Filter by name='Bob'")
    bobs, err := client.ListRaw("users", models.ListOptions{
        Filter: "name=Bob",
    })

    if err != nil {
        fmt.Printf("❌ API request failed: %v\n", err)
        os.Exit(1)
    }

    if len(bobs) != 1 || bobs[0]["name"] != "Bob" {
        fmt.Printf("❌ Verification Failed: Expected 1 Bob, found %d\n", len(bobs))
        os.Exit(1)
    }

    fmt.Println("✅ Client Features Verification Passed!")
}
EOF

# Run Test
echo "🚀 Running Go Test..."
if ! go run main.go; then
    echo "❌ Go Test Failed!"
    echo "📜 Server Log:"
    cat server.log
    exit 1
fi

echo "✅ All checks passed!"
