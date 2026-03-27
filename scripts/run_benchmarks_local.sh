#!/bin/bash
set -euo pipefail

# Colors for output
GREEN='\033[0;32m'
RED='\033[0;31m'
NC='\033[0m' # No Color

echo -e "${GREEN}🚀 Starting Local Benchmark Verification...${NC}"

# 1. Build CLI
echo -e "${GREEN}📦 Building PrkDB CLI...${NC}"
cargo build --release -p prkdb-cli

# Variables
PRKDB_BIN="./target/release/prkdb-cli"
WORK_DIR=$(mktemp -d)
ADMIN_TOKEN="local_benchmark_token"
DATABASE_PATH="$WORK_DIR/db"

reserve_port() {
    python3 -c 'import socket; s = socket.socket(); s.bind(("127.0.0.1", 0)); print(s.getsockname()[1]); s.close()'
}

HTTP_PORT=$(reserve_port)
GRPC_PORT=$(reserve_port)
while [ "$HTTP_PORT" = "$GRPC_PORT" ]; do
    GRPC_PORT=$(reserve_port)
done

SERVER_HTTP_URL="http://127.0.0.1:$HTTP_PORT"
SERVER_GRPC_URL="http://127.0.0.1:$GRPC_PORT"

# Cleanup function
cleanup() {
    echo -e "${GREEN}🧹 Cleaning up...${NC}"
    echo -e "${RED}📜 Server Log:${NC}"
    if [ -f "$WORK_DIR/server.log" ]; then
        cat "$WORK_DIR/server.log"
    else
        echo "Log file not found."
    fi
    kill $SERVER_PID 2>/dev/null || true
    rm -rf "$WORK_DIR"
    rm -rf benches/client_py benches/client_ts bench.desc bench.proto
}
trap cleanup EXIT

# 2. Start Server
echo -e "${GREEN}🔥 Starting PrkDB Server (HTTP: $HTTP_PORT, gRPC: $GRPC_PORT)...${NC}"
PRKDB_ADMIN_TOKEN="$ADMIN_TOKEN" \
    $PRKDB_BIN --database "$DATABASE_PATH" serve --port $HTTP_PORT --grpc-port $GRPC_PORT > "$WORK_DIR/server.log" 2>&1 &
SERVER_PID=$!

for _ in {1..40}; do
    if curl -sf "${SERVER_HTTP_URL}/health" >/dev/null 2>&1 \
        && PRKDB_ADMIN_TOKEN="$ADMIN_TOKEN" "$PRKDB_BIN" schema --server "$SERVER_GRPC_URL" list >/dev/null 2>&1; then
        break
    fi
    sleep 1
done

# 3. Define Schema
echo -e "${GREEN}📜 Defining Schema...${NC}"
cat > bench.proto <<EOF
syntax = "proto3";
package models;
message Benchmark {
  string id = 1;
  string payload = 2;
  int64 timestamp = 3;
}
EOF

# Ensure protoc is installed
if ! command -v protoc &> /dev/null; then
    echo -e "${RED}❌ protoc not found. Please install protobuf-compiler.${NC}"
    exit 1
fi

protoc --descriptor_set_out=bench.desc --include_imports bench.proto

echo -e "${GREEN}📝 Registering Schema...${NC}"
# Schema registration uses gRPC
PRKDB_ADMIN_TOKEN="$ADMIN_TOKEN" "$PRKDB_BIN" schema --server $SERVER_GRPC_URL register --collection benchmark --proto bench.desc

# 4. Generate Clients
echo -e "${GREEN}🛠️ Generating Clients...${NC}"
mkdir -p benches/client_py benches/client_ts

# Python (Uses gRPC to fetch schema)
PRKDB_ADMIN_TOKEN="$ADMIN_TOKEN" "$PRKDB_BIN" codegen --server $SERVER_GRPC_URL --lang python --out benches/client_py --collection benchmark

# TypeScript (Uses gRPC to fetch schema)
PRKDB_ADMIN_TOKEN="$ADMIN_TOKEN" "$PRKDB_BIN" codegen --server $SERVER_GRPC_URL --lang typescript --out benches/client_ts --collection benchmark

# 5. Run Python Benchmark
echo -e "${GREEN}🐍 Running Python Benchmark...${NC}"
# Benchmark uses HTTP client
if python3 -c "import httpx" &> /dev/null; then
    python3 benches/bench_python.py --server $SERVER_HTTP_URL --records 1000
else
    echo -e "${RED}⚠️  Skipping Python bench: 'httpx' module not found.${NC}"
    echo "Run 'pip install httpx' to enable."
fi

# 6. Run TypeScript Benchmark
echo -e "${GREEN}📘 Running TypeScript Benchmark...${NC}"
if command -v ts-node &> /dev/null; then
    # We need to set env vars expected by script
    export PRKDB_SERVER=$SERVER_HTTP_URL
    export NUM_RECORDS=1000
    ts-node benches/bench_ts.ts
else
    echo -e "${RED}⚠️  Skipping TS bench: 'ts-node' not found.${NC}"
    echo "Run 'npm install -g ts-node typescript' to enable."
fi

echo -e "${GREEN}✅ Verification Complete!${NC}"
