#!/bin/bash
set -euo pipefail

GREEN='\033[0;32m'
RED='\033[0;31m'
NC='\033[0m'

echo -e "${GREEN}🚀 Starting Local Benchmark Verification...${NC}"

echo -e "${GREEN}📦 Building PrkDB CLI...${NC}"
cargo build --release -p prkdb-cli

PRKDB_BIN="./target/release/prkdb-cli"
WORK_DIR=$(mktemp -d)
GO_BENCH_DIR=$(mktemp -d)
ADMIN_TOKEN="local_benchmark_token"
DATABASE_PATH="$WORK_DIR/db"
COLLECTION_NAME="benchmark"
BENCH_RECORDS="${NUM_RECORDS:-1000}"
DEFAULT_CLIENT_DIR="benches/client_py"

reserve_port() {
    python3 -c 'import socket; s = socket.socket(); s.bind(("127.0.0.1", 0)); print(s.getsockname()[1]); s.close()'
}

require_command() {
    local command_name="$1"
    local install_hint="$2"

    if ! command -v "$command_name" >/dev/null 2>&1; then
        echo -e "${RED}❌ Required command not found: ${command_name}.${NC}"
        echo "$install_hint"
        exit 1
    fi
}

require_python_module() {
    local module_name="$1"
    local install_hint="$2"

    if ! python3 -c "import ${module_name}" >/dev/null 2>&1; then
        echo -e "${RED}❌ Required Python module not found: ${module_name}.${NC}"
        echo "$install_hint"
        exit 1
    fi
}

cleanup() {
    echo -e "${GREEN}🧹 Cleaning up...${NC}"
    echo -e "${RED}📜 Server Log:${NC}"
    if [ -f "$WORK_DIR/server.log" ]; then
        cat "$WORK_DIR/server.log"
    else
        echo "Log file not found."
    fi
    kill "${SERVER_PID:-}" 2>/dev/null || true
    rm -rf "$WORK_DIR" "$GO_BENCH_DIR"
    rm -rf benches/client_py benches/client_ts benches/client_go bench.desc bench.proto
}
trap cleanup EXIT

require_command protoc "Install protobuf-compiler to generate the benchmark schema descriptor."
require_command go "Install Go to run the local Go benchmark."
require_command tsx "Install tsx to match CI TypeScript benchmark execution."
require_python_module httpx "Install httpx so the generated Python client can run."

HTTP_PORT=$(reserve_port)
GRPC_PORT=$(reserve_port)
while [ "$HTTP_PORT" = "$GRPC_PORT" ]; do
    GRPC_PORT=$(reserve_port)
done

SERVER_HTTP_URL="http://127.0.0.1:$HTTP_PORT"
SERVER_GRPC_URL="http://127.0.0.1:$GRPC_PORT"

echo -e "${GREEN}🔥 Starting PrkDB Server (HTTP: $HTTP_PORT, gRPC: $GRPC_PORT)...${NC}"
PRKDB_ADMIN_TOKEN="$ADMIN_TOKEN" \
    "$PRKDB_BIN" --database "$DATABASE_PATH" serve --port "$HTTP_PORT" --grpc-port "$GRPC_PORT" > "$WORK_DIR/server.log" 2>&1 &
SERVER_PID=$!

for _ in {1..40}; do
    if curl -sf "${SERVER_HTTP_URL}/health" >/dev/null 2>&1 \
        && PRKDB_ADMIN_TOKEN="$ADMIN_TOKEN" "$PRKDB_BIN" schema --server "$SERVER_GRPC_URL" list >/dev/null 2>&1; then
        break
    fi
    sleep 1
done

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

protoc --descriptor_set_out=bench.desc --include_imports bench.proto

echo -e "${GREEN}📝 Registering Schema...${NC}"
PRKDB_ADMIN_TOKEN="$ADMIN_TOKEN" "$PRKDB_BIN" schema --server "$SERVER_GRPC_URL" register --collection "$COLLECTION_NAME" --proto bench.desc

echo -e "${GREEN}🛠️ Generating Clients...${NC}"
mkdir -p benches/client_py benches/client_ts benches/client_go
PRKDB_ADMIN_TOKEN="$ADMIN_TOKEN" "$PRKDB_BIN" codegen --server "$SERVER_GRPC_URL" --lang python --out benches/client_py --collection "$COLLECTION_NAME"
PRKDB_ADMIN_TOKEN="$ADMIN_TOKEN" "$PRKDB_BIN" codegen --server "$SERVER_GRPC_URL" --lang typescript --out benches/client_ts --collection "$COLLECTION_NAME"
PRKDB_ADMIN_TOKEN="$ADMIN_TOKEN" "$PRKDB_BIN" codegen --server "$SERVER_GRPC_URL" --lang go --out benches/client_go --collection "$COLLECTION_NAME"

echo -e "${GREEN}🐍 Running Python Benchmark...${NC}"
PRKDB_SERVER="$SERVER_HTTP_URL" \
NUM_RECORDS="$BENCH_RECORDS" \
PRKDB_COLLECTION="$COLLECTION_NAME" \
PRKDB_ID_PREFIX="bench_py" \
PRKDB_CLIENT_DIR="$DEFAULT_CLIENT_DIR" \
python3 benches/bench_python.py

echo -e "${GREEN}📘 Running TypeScript Benchmark...${NC}"
PRKDB_SERVER="$SERVER_HTTP_URL" \
NUM_RECORDS="$BENCH_RECORDS" \
PRKDB_COLLECTION="$COLLECTION_NAME" \
PRKDB_ID_PREFIX="bench_ts" \
tsx benches/bench_ts.ts

echo -e "${GREEN}🐹 Running Go Benchmark...${NC}"
cp benches/bench_go.go "$GO_BENCH_DIR/main.go"
cp -R benches/client_go "$GO_BENCH_DIR/client_go"
(
    cd "$GO_BENCH_DIR"
    go mod init benchclient >/dev/null 2>&1
    PRKDB_SERVER="$SERVER_HTTP_URL" \
    NUM_RECORDS="$BENCH_RECORDS" \
    PRKDB_COLLECTION="$COLLECTION_NAME" \
    PRKDB_ID_PREFIX="bench_go" \
    go run .
)

echo -e "${GREEN}✅ Verification Complete!${NC}"
