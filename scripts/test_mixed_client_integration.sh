#!/bin/bash
set -euo pipefail

PRKDB_BIN="${PRKDB_BIN:-./target/debug/prkdb-cli}"
ADMIN_TOKEN="mixed_client_integration_token"
COLLECTION_NAME="benchmark"
PY_COUNT=700
TS_COUNT=1000
GO_COUNT=1300

reserve_port() {
    python3 -c 'import socket; s = socket.socket(); s.bind(("127.0.0.1", 0)); print(s.getsockname()[1]); s.close()'
}

sample_ids_for() {
    local prefix="$1"
    local count="$2"
    local mid=$(( (count + 1) / 2 ))
    printf '%s-%06d\n' "$prefix" 1
    if [ "$mid" -ne 1 ] && [ "$mid" -ne "$count" ]; then
        printf '%s-%06d\n' "$prefix" "$mid"
    fi
    if [ "$count" -ne 1 ]; then
        printf '%s-%06d\n' "$prefix" "$count"
    fi
}

append_sample_args() {
    local prefix="$1"
    local count="$2"

    while IFS= read -r sample_id; do
        ALL_SAMPLE_ARGS+=("--sample-id" "$sample_id")
    done < <(sample_ids_for "$prefix" "$count")
}

require_command() {
    local command_name="$1"
    local install_hint="$2"

    if ! command -v "$command_name" >/dev/null 2>&1; then
        echo "❌ Required command not found: ${command_name}."
        echo "$install_hint"
        exit 1
    fi
}

require_command protoc "Install protobuf-compiler to generate the benchmark schema descriptor."
require_command go "Install Go to run the mixed-client integration test."
require_command python3 "Install Python 3 to run the mixed-client integration test."
require_command tsx "Install tsx to execute the TypeScript runner."

if [ "${SKIP_BUILD:-0}" != "1" ]; then
    echo "🏗️  Building prkdb binary..."
    cargo build -p prkdb-cli --bin prkdb-cli
fi

if [ ! -x "$PRKDB_BIN" ]; then
    echo "❌ Expected prkdb binary at $PRKDB_BIN"
    exit 1
fi

WORK_DIR=$(mktemp -d)
GO_WORK_DIR="$WORK_DIR/go_runner"
DATABASE_PATH="$WORK_DIR/db"
SERVER_LOG="$WORK_DIR/server.log"
PY_LOG_DIR="$WORK_DIR/logs/py"
TS_LOG_DIR="$WORK_DIR/logs/ts"
GO_LOG_DIR="$WORK_DIR/logs/go"

mkdir -p "$GO_WORK_DIR" "$PY_LOG_DIR" "$TS_LOG_DIR" "$GO_LOG_DIR"

HTTP_PORT=$(reserve_port)
GRPC_PORT=$(reserve_port)
while [ "$HTTP_PORT" = "$GRPC_PORT" ]; do
    GRPC_PORT=$(reserve_port)
done

SERVER_HTTP_URL="http://127.0.0.1:$HTTP_PORT"
SERVER_GRPC_URL="http://127.0.0.1:$GRPC_PORT"

cleanup() {
    local exit_code=$?
    if [ -n "${SERVER_PID:-}" ]; then
        kill "$SERVER_PID" 2>/dev/null || true
        wait "$SERVER_PID" 2>/dev/null || true
    fi
    if [ "$exit_code" -ne 0 ]; then
        echo "📜 Server log:"
        if [ -f "$SERVER_LOG" ]; then
            cat "$SERVER_LOG"
        fi
    fi
    rm -rf "$WORK_DIR"
}
trap cleanup EXIT

echo "🚀 Starting server on HTTP $HTTP_PORT / gRPC $GRPC_PORT..."
# PRKDB_BOOTSTRAP_TOKEN, not just PRKDB_ADMIN_TOKEN: the latter guards the deprecated
# admin_token message field and creates no principal, so `serve` refuses to start with it
# alone. Using the same value for both means the generated clients authenticate with the
# credential they already pass, and this test runs against an *authorized* server rather
# than an anonymous one — which is the point of the acceptance item it covers.
PRKDB_ADMIN_TOKEN="$ADMIN_TOKEN" \
PRKDB_BOOTSTRAP_TOKEN="$ADMIN_TOKEN" \
    "$PRKDB_BIN" --database "$DATABASE_PATH" serve --port "$HTTP_PORT" --grpc-port "$GRPC_PORT" > "$SERVER_LOG" 2>&1 &
SERVER_PID=$!

echo "⏳ Waiting for server..."
SERVER_READY=0
for _ in {1..40}; do
    if curl -sf "${SERVER_HTTP_URL}/health" >/dev/null 2>&1 \
        && PRKDB_ADMIN_TOKEN="$ADMIN_TOKEN" "$PRKDB_BIN" schema --server "$SERVER_GRPC_URL" list >/dev/null 2>&1; then
        SERVER_READY=1
        break
    fi
    sleep 1
done

if [ "$SERVER_READY" -ne 1 ]; then
    echo "❌ Server failed to become ready"
    exit 1
fi

echo "📝 Defining benchmark schema..."
cat > "$WORK_DIR/bench.proto" <<EOF
syntax = "proto3";
package models;

message Benchmark {
  string id = 1;
  string payload = 2;
  int64 timestamp = 3;
}
EOF

protoc --include_imports --descriptor_set_out="$WORK_DIR/bench.desc" --proto_path="$WORK_DIR" "$WORK_DIR/bench.proto"

echo "🚀 Registering benchmark schema..."
PRKDB_ADMIN_TOKEN="$ADMIN_TOKEN" "$PRKDB_BIN" schema --server "$SERVER_GRPC_URL" register --collection "$COLLECTION_NAME" --proto "$WORK_DIR/bench.desc"

echo "⚙️  Generating clients..."
PRKDB_ADMIN_TOKEN="$ADMIN_TOKEN" "$PRKDB_BIN" codegen --server "$SERVER_GRPC_URL" --lang python --out "$WORK_DIR/client_py" --collection "$COLLECTION_NAME"
PRKDB_ADMIN_TOKEN="$ADMIN_TOKEN" "$PRKDB_BIN" codegen --server "$SERVER_GRPC_URL" --lang typescript --out "$WORK_DIR/client_ts" --collection "$COLLECTION_NAME"
PRKDB_ADMIN_TOKEN="$ADMIN_TOKEN" "$PRKDB_BIN" codegen --server "$SERVER_GRPC_URL" --lang go --out "$WORK_DIR/client_go" --collection "$COLLECTION_NAME"

if [ ! -f "$WORK_DIR/client_py/prkdb_client.py" ]; then
    echo "❌ Python client generation failed"
    exit 1
fi
if [ ! -f "$WORK_DIR/client_ts/${COLLECTION_NAME}.ts" ]; then
    echo "❌ TypeScript client generation failed"
    exit 1
fi
if [ ! -f "$WORK_DIR/client_go/${COLLECTION_NAME}.go" ]; then
    echo "❌ Go client generation failed"
    exit 1
fi

cp scripts/mixed_client_runner.go "$GO_WORK_DIR/main.go"
cp -R "$WORK_DIR/client_go" "$GO_WORK_DIR/client_go"
(cd "$GO_WORK_DIR" && go mod init mixedclient >/dev/null 2>&1)

ALL_SAMPLE_ARGS=()
append_sample_args py "$PY_COUNT"
append_sample_args ts "$TS_COUNT"
append_sample_args go "$GO_COUNT"

run_runner() {
    local log_dir="$1"
    local work_dir="$2"
    shift
    shift
    mkdir -p "$log_dir"
    (
        cd "$work_dir"
        "$@" >"$log_dir/stdout.log" 2>"$log_dir/stderr.log"
    ) &
    RUNNER_PID=$!
}

wait_for_pid() {
    local pid="$1"
    local label="$2"
    local log_dir="$3"

    if wait "$pid"; then
        echo "✅ ${label} completed"
        if [ -s "$log_dir/stdout.log" ]; then
            echo "📄 ${label} stdout:"
            cat "$log_dir/stdout.log"
        fi
        if [ -s "$log_dir/stderr.log" ]; then
            echo "📄 ${label} stderr:"
            cat "$log_dir/stderr.log"
        fi
        return 0
    fi

    echo "❌ ${label} failed"
    echo "📄 ${label} stdout:"
    cat "$log_dir/stdout.log" 2>/dev/null || true
    echo "📄 ${label} stderr:"
    cat "$log_dir/stderr.log" 2>/dev/null || true
    return 1
}

echo "🐍 Running Python writer..."
run_runner "$PY_LOG_DIR/write" "$PWD" env \
    PRKDB_MODE=write \
    PRKDB_SERVER="$SERVER_HTTP_URL" \
    PRKDB_CREDENTIAL="$ADMIN_TOKEN" \
    PRKDB_COLLECTION="$COLLECTION_NAME" \
    NUM_RECORDS="$PY_COUNT" \
    PRKDB_ID_PREFIX=py \
    PRKDB_CLIENT_DIR="$WORK_DIR/client_py" \
    python3 scripts/mixed_client_runner.py
PY_WRITE_PID=$RUNNER_PID

echo "📘 Running TypeScript writer..."
run_runner "$TS_LOG_DIR/write" "$PWD" env \
    PRKDB_MODE=write \
    PRKDB_SERVER="$SERVER_HTTP_URL" \
    PRKDB_CREDENTIAL="$ADMIN_TOKEN" \
    PRKDB_COLLECTION="$COLLECTION_NAME" \
    NUM_RECORDS="$TS_COUNT" \
    PRKDB_ID_PREFIX=ts \
    PRKDB_CLIENT_DIR="$WORK_DIR/client_ts" \
    tsx scripts/mixed_client_runner.ts
TS_WRITE_PID=$RUNNER_PID

echo "🐹 Running Go writer..."
run_runner "$GO_LOG_DIR/write" "$GO_WORK_DIR" env \
    PRKDB_MODE=write \
    PRKDB_SERVER="$SERVER_HTTP_URL" \
    PRKDB_CREDENTIAL="$ADMIN_TOKEN" \
    PRKDB_COLLECTION="$COLLECTION_NAME" \
    NUM_RECORDS="$GO_COUNT" \
    PRKDB_ID_PREFIX=go \
    PRKDB_CLIENT_DIR="$GO_WORK_DIR/client_go" \
    go run . \
    --mode write \
    --records "$GO_COUNT" \
    --id-prefix go \
    --server "$SERVER_HTTP_URL" \
    --collection "$COLLECTION_NAME" \
    --client-dir "$GO_WORK_DIR/client_go"
GO_WRITE_PID=$RUNNER_PID

wait_for_pid "$PY_WRITE_PID" "Python writer" "$PY_LOG_DIR/write"
wait_for_pid "$TS_WRITE_PID" "TypeScript writer" "$TS_LOG_DIR/write"
wait_for_pid "$GO_WRITE_PID" "Go writer" "$GO_LOG_DIR/write"

echo "🐍 Running Python reader..."
run_runner "$PY_LOG_DIR/read" "$PWD" env \
    PRKDB_MODE=read \
    PRKDB_SERVER="$SERVER_HTTP_URL" \
    PRKDB_CREDENTIAL="$ADMIN_TOKEN" \
    PRKDB_COLLECTION="$COLLECTION_NAME" \
    PRKDB_ID_PREFIX=py \
    PRKDB_CLIENT_DIR="$WORK_DIR/client_py" \
    python3 scripts/mixed_client_runner.py "${ALL_SAMPLE_ARGS[@]}"
PY_READ_PID=$RUNNER_PID

echo "📘 Running TypeScript reader..."
run_runner "$TS_LOG_DIR/read" "$PWD" env \
    PRKDB_MODE=read \
    PRKDB_SERVER="$SERVER_HTTP_URL" \
    PRKDB_CREDENTIAL="$ADMIN_TOKEN" \
    PRKDB_COLLECTION="$COLLECTION_NAME" \
    PRKDB_ID_PREFIX=ts \
    PRKDB_CLIENT_DIR="$WORK_DIR/client_ts" \
    tsx scripts/mixed_client_runner.ts "${ALL_SAMPLE_ARGS[@]}"
TS_READ_PID=$RUNNER_PID

echo "🐹 Running Go reader..."
run_runner "$GO_LOG_DIR/read" "$GO_WORK_DIR" env \
    PRKDB_MODE=read \
    PRKDB_SERVER="$SERVER_HTTP_URL" \
    PRKDB_CREDENTIAL="$ADMIN_TOKEN" \
    PRKDB_COLLECTION="$COLLECTION_NAME" \
    PRKDB_ID_PREFIX=go \
    PRKDB_CLIENT_DIR="$GO_WORK_DIR/client_go" \
    go run . "${ALL_SAMPLE_ARGS[@]}" \
    --mode read \
    --server "$SERVER_HTTP_URL" \
    --collection "$COLLECTION_NAME" \
    --client-dir "$GO_WORK_DIR/client_go"
GO_READ_PID=$RUNNER_PID

wait_for_pid "$PY_READ_PID" "Python reader" "$PY_LOG_DIR/read"
wait_for_pid "$TS_READ_PID" "TypeScript reader" "$TS_LOG_DIR/read"
wait_for_pid "$GO_READ_PID" "Go reader" "$GO_LOG_DIR/read"

echo "🔎 Verifying aggregate results..."
# The verifier reads the collection back over HTTP, so it needs the same credential the
# three writers used; the server this test runs against enforces authorization.
PRKDB_CREDENTIAL="$ADMIN_TOKEN" \
python3 scripts/verify_mixed_client_results.py \
    --server "$SERVER_HTTP_URL" \
    --collection "$COLLECTION_NAME" \
    --expect-py "$PY_COUNT" \
    --expect-ts "$TS_COUNT" \
    --expect-go "$GO_COUNT" \
    "${ALL_SAMPLE_ARGS[@]}"

echo "✅ Mixed-client integration test passed"
