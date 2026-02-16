#!/bin/bash
set -e
# Schema CLI Integration Test

echo "🏗️  Building prkdb binary..."
cargo build -p prkdb-cli --quiet

PRKDB_BIN="./target/debug/prkdb"
if [ ! -f "$PRKDB_BIN" ]; then
    if [ -f "./target/debug/prkdb-cli" ]; then
        PRKDB_BIN="./target/debug/prkdb-cli"
    elif [ -f "./target/debug/prkdb-server" ]; then
        PRKDB_BIN="./target/debug/prkdb-server"
    fi
fi

if [ ! -f "$PRKDB_BIN" ]; then
    echo "❌ Could not find prkdb binary"
    exit 1
fi

SERVER_PORT=50052
GRPC_PORT=50054
SERVER_URL="http://127.0.0.1:${SERVER_PORT}"
GRPC_URL="http://127.0.0.1:${GRPC_PORT}"
WORK_DIR="/tmp/prkdb_schema_cli_test"
LOG_FILE="$WORK_DIR/server.log"

# Cleanup function
cleanup() {
    echo "🧹 Cleaning up..."
    if [ -f "$WORK_DIR/server.pid" ]; then
        kill $(cat "$WORK_DIR/server.pid") 2>/dev/null || true
    fi
    rm -rf "$WORK_DIR"
}
trap cleanup EXIT

mkdir -p "$WORK_DIR"

# 1. Start server
echo "🚀 Starting server on port $SERVER_PORT (gRPC $GRPC_PORT)..."
$PRKDB_BIN serve --port $SERVER_PORT --grpc-port $GRPC_PORT > "$LOG_FILE" 2>&1 &
SERVER_PID=$!
echo $SERVER_PID > "$WORK_DIR/server.pid"
echo "Server PID: $SERVER_PID"

echo "⏳ Waiting for server to be ready..."
for i in {1..30}; do
    if curl -s "http://127.0.0.1:$SERVER_PORT/health" > /dev/null; then
        echo "✅ Server is ready!"
        break
    fi
    if [ $i -eq 30 ]; then
        echo "❌ Server failed to start"
        cat "$LOG_FILE"
        exit 1
    fi
    sleep 1
done

# 2. Prepare Schema
echo "📝 Creating test schema..."
cat > "$WORK_DIR/test_schema.proto" <<EOF
syntax = "proto3";
package test;
message TestMessage {
  string id = 1;
  string content = 2;
}
EOF

# Compile to descriptor if needed, but CLI might take raw proto? 
# Wait, CLI checks showed it takes raw proto in previous iterations?
# No, in test_client_features_ts.sh I used descriptor set.
# CLI `schema register` takes `fs::read(&proto)`. It sends bytes to server.
# Server expects descriptor set bytes usually.
# So I should compile it.

if ! command -v protoc &> /dev/null; then
    echo "⚠️ protoc not found, skipping comprehensive schema tests (or installing?)"
    # We can try to run without compilation if server accepts raw proto (which it doesn't currently)
    echo "❌ protoc is required for this test"
    exit 1
fi

echo "🔨 Compiling Schema..."
protoc --include_imports --descriptor_set_out="$WORK_DIR/test_schema.desc" --proto_path="$WORK_DIR" "$WORK_DIR/test_schema.proto"

# 3. Test Register
echo "🧪 Testing 'schema register'..."
if $PRKDB_BIN schema --server "$GRPC_URL" register --collection test_col --proto "$WORK_DIR/test_schema.desc"; then
    echo "✅ Schema registered successfully"
else
    echo "❌ Schema registration failed"
    exit 1
fi

# 4. Test List
echo "🧪 Testing 'schema list'..."
LIST_OUT=$($PRKDB_BIN schema --server "$GRPC_URL" list)
echo "$LIST_OUT"
if echo "$LIST_OUT" | grep -q "test_col"; then
    echo "✅ Schema list contains test_col"
else
    echo "❌ Schema list missing test_col"
    exit 1
fi

# 5. Test Get
echo "🧪 Testing 'schema get'..."
if $PRKDB_BIN schema --server "$GRPC_URL" get --collection test_col > "$WORK_DIR/fetched.desc"; then
    echo "✅ Schema fetched successfully"
    # Verify size > 0
    if [ -s "$WORK_DIR/fetched.desc" ]; then
         echo "✅ Fetched schema is not empty"
    else
         echo "❌ Fetched schema is empty"
         exit 1
    fi
else
    echo "❌ Schema get failed"
    exit 1
fi

# 6. Test Check (Compatibility)
echo "🧪 Testing 'schema check'..."
if $PRKDB_BIN schema --server "$GRPC_URL" check --collection test_col --proto "$WORK_DIR/test_schema.desc"; then
    echo "✅ Schema compatibility check passed"
else
    echo "❌ Schema compatibility check failed"
    exit 1
fi

echo "🎉 All Schema CLI tests passed!"
