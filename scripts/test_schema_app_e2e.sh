#!/bin/bash
set -euo pipefail
# End-to-End Schema Application Test
# Simulates: Schema V1 -> App V1 -> Schema V2 (Breaking) -> Migration -> App V2

PRKDB_BIN="${PRKDB_BIN:-./target/debug/prkdb-cli}"
ADMIN_TOKEN="schema_e2e_test_token"
LOG_FILE="/tmp/prkdb_server_e2e.log"
WORK_DIR="/tmp/prkdb_e2e"
DATABASE_PATH="$WORK_DIR/db"
rm -rf "$WORK_DIR"
mkdir -p "$WORK_DIR"

if [ "${SKIP_BUILD:-0}" != "1" ]; then
    echo "🏗️  Building prkdb binary..."
    cargo build -p prkdb-cli --bin prkdb-cli --quiet
fi

if [ ! -x "$PRKDB_BIN" ]; then
    echo "❌ Expected prkdb binary at $PRKDB_BIN"
    exit 1
fi

reserve_port() {
    python3 -c 'import socket; s = socket.socket(); s.bind(("127.0.0.1", 0)); print(s.getsockname()[1]); s.close()'
}

HTTP_PORT=$(reserve_port)
GRPC_PORT=$(reserve_port)
while [ "$HTTP_PORT" = "$GRPC_PORT" ]; do
    GRPC_PORT=$(reserve_port)
done

SERVER_HTTP_URL="http://127.0.0.1:${HTTP_PORT}"
SERVER_GRPC_URL="http://127.0.0.1:${GRPC_PORT}"

wait_for_server() {
    echo "⏳ Waiting for server..."
    for _ in {1..40}; do
        if curl -sf "${SERVER_HTTP_URL}/health" >/dev/null 2>&1 \
            && PRKDB_ADMIN_TOKEN="$ADMIN_TOKEN" "$PRKDB_BIN" schema --server "$SERVER_GRPC_URL" list >/dev/null 2>&1; then
            return 0
        fi
        sleep 1
    done

    echo "❌ Server failed to become ready"
    cat "$LOG_FILE"
    exit 1
}

cleanup() {
    echo "🧹 Cleaning up..."
    if [ -n "${SERVER_PID:-}" ]; then
        kill $SERVER_PID || true
    fi
    # rm -rf "$WORK_DIR"
}
trap cleanup EXIT

# 1. Start server
echo "🚀 Starting server on HTTP $HTTP_PORT / gRPC $GRPC_PORT..."
PRKDB_ADMIN_TOKEN="$ADMIN_TOKEN" \
    $PRKDB_BIN --database "$DATABASE_PATH" serve --port "$HTTP_PORT" --grpc-port "$GRPC_PORT" > "$LOG_FILE" 2>&1 &
SERVER_PID=$!
echo "Server PID: $SERVER_PID"
wait_for_server

# 2. Define Schema V1
echo "📝 Defining Schema V1..."
cat > "$WORK_DIR/user_v1.proto" <<EOF
syntax = "proto3";
package models;
message User {
  string id = 1;
  string name = 2;
  string email = 3;
}
EOF

# Compile to descriptor set
protoc --include_imports --descriptor_set_out="$WORK_DIR/user_v1.desc" -I "$WORK_DIR" "$WORK_DIR/user_v1.proto"

# 3. Register V1
# Note: --server must be passed to `schema` command, before `register` subcommand
echo "🚀 Registering Schema V1..."
PRKDB_ADMIN_TOKEN="$ADMIN_TOKEN" "$PRKDB_BIN" schema --server "$SERVER_GRPC_URL" register \
    --collection users \
    --proto "$WORK_DIR/user_v1.desc"

# 4. Generate Client V1
echo "⚙️  Generating Client V1..."
PRKDB_ADMIN_TOKEN="$ADMIN_TOKEN" "$PRKDB_BIN" codegen \
    --server "$SERVER_GRPC_URL" \
    --lang python \
    --out "$WORK_DIR/client_v1"

# 5. Simulate App V1 (Python)
echo "🏃 Running App V1..."
# Create a dummy python script that uses the generated client
cat > "$WORK_DIR/app_v1.py" <<EOF
import sys
import os
sys.path.append("$WORK_DIR/client_v1")
# Generated file is 'users.py' (from collection name 'users') which contains class 'User'
from users import User

print("✅ App V1: Creating user...")
u = User(id="u1", name="Alice", email="alice@example.com")
print(f"User created: {u}")
EOF

python3 "$WORK_DIR/app_v1.py"


# 6. Define Schema V2 (Breaking Change: email field removed/renamed)
echo "📝 Defining Schema V2 (Breaking)..."
cat > "$WORK_DIR/user_v2.proto" <<EOF
syntax = "proto3";
package models;
message User {
  string id = 1;
  string full_name = 2; // Renamed
  // email removed
  int32 age = 4; // New field
}
EOF

protoc --include_imports --descriptor_set_out="$WORK_DIR/user_v2.desc" -I "$WORK_DIR" "$WORK_DIR/user_v2.proto"

# 7. Check Compatibility (Should Fail or Warn)
echo "🔍 Checking Compatibility (Expected to fail/warn for BACKWARD compatibility)..."
# Make it clearly incompatible by changing type of field 1
cat > "$WORK_DIR/user_v2_break.proto" <<EOF
syntax = "proto3";
package models;
message User {
  int32 id = 1; // Changed from string to int32 - BREAKING
  string name = 2;
}
EOF
protoc --include_imports --descriptor_set_out="$WORK_DIR/user_v2_break.desc" -I "$WORK_DIR" "$WORK_DIR/user_v2_break.proto"

if PRKDB_ADMIN_TOKEN="$ADMIN_TOKEN" "$PRKDB_BIN" schema --server "$SERVER_GRPC_URL" check --collection users --proto "$WORK_DIR/user_v2_break.desc"; then
    echo "❌ Unexpected compatibility success!"
    exit 1
else
    echo "✅ Compatibility check failed (as expected)"
fi


# 8. Register V2 with Override (Force / migration)
# We use COMPATIBILITY_NONE to force it
echo "🚀 Registering Schema V2 (Forced)..."
PRKDB_ADMIN_TOKEN="$ADMIN_TOKEN" "$PRKDB_BIN" schema --server "$SERVER_GRPC_URL" register \
    --collection users \
    --proto "$WORK_DIR/user_v2_break.desc" \
    --compatibility none

# 9. Generate Client V2
echo "⚙️  Generating Client V2..."
PRKDB_ADMIN_TOKEN="$ADMIN_TOKEN" "$PRKDB_BIN" codegen \
    --server "$SERVER_GRPC_URL" \
    --lang python \
    --out "$WORK_DIR/client_v2"

# 10. Simulate App V2
echo "🏃 Running App V2..."
cat > "$WORK_DIR/app_v2.py" <<EOF
import sys
import os
sys.path.append("$WORK_DIR/client_v2")
# Generated file is 'users.py' (from collection name)
from users import User

print("✅ App V2: Creating user with int ID...")
u = User(id=123, name="Bob")
print(f"User created: {u}")
EOF

python3 "$WORK_DIR/app_v2.py"

echo "✅ E2E Application Test Passed!"
