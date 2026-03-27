#!/bin/bash
set -euo pipefail
# End-to-End Schema Application Test - Nested Types
# Simulates: Schema with nested messages -> Client Gen (Py/TS) -> Usage

PRKDB_BIN="${PRKDB_BIN:-./target/debug/prkdb-cli}"
LOG_FILE="/tmp/prkdb_nested_server.log"
WORK_DIR="/tmp/prkdb_nested_e2e"
ADMIN_TOKEN="schema_nested_e2e_test_token"
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
}
trap cleanup EXIT

# 1. Start server
echo "🚀 Starting server on HTTP $HTTP_PORT / gRPC $GRPC_PORT..."
PRKDB_ADMIN_TOKEN="$ADMIN_TOKEN" \
    $PRKDB_BIN --database "$DATABASE_PATH" serve --port "$HTTP_PORT" --grpc-port "$GRPC_PORT" > "$LOG_FILE" 2>&1 &
SERVER_PID=$!
echo "Server PID: $SERVER_PID"
wait_for_server

# 2. Define Nested Schema
echo "📝 Defining Nested Schema..."
cat > "$WORK_DIR/user_nested.proto" <<EOF
syntax = "proto3";
package models;

message Address {
  string street = 1;
  string city = 2;
  int32 zip = 3;
}

message User {
  string id = 1;
  string name = 2;
  Address address = 3;
  repeated Address past_addresses = 4;
}
EOF

# Compile to descriptor set
protoc --include_imports --descriptor_set_out="$WORK_DIR/user_nested.desc" -I "$WORK_DIR" "$WORK_DIR/user_nested.proto"

# 3. Register
echo "🚀 Registering Schema..."
PRKDB_ADMIN_TOKEN="$ADMIN_TOKEN" "$PRKDB_BIN" schema --server "$SERVER_GRPC_URL" register \
    --collection users \
    --proto "$WORK_DIR/user_nested.desc"

# 4. Generate Client (Python)
echo "⚙️  Generating Client (Python)..."
PRKDB_ADMIN_TOKEN="$ADMIN_TOKEN" "$PRKDB_BIN" codegen \
    --server "$SERVER_GRPC_URL" \
    --lang python \
    --out "$WORK_DIR/client_py"

# 5. Generate Client (TypeScript)
echo "⚙️  Generating Client (TypeScript)..."
PRKDB_ADMIN_TOKEN="$ADMIN_TOKEN" "$PRKDB_BIN" codegen \
    --server "$SERVER_GRPC_URL" \
    --lang typescript \
    --out "$WORK_DIR/client_ts"

# 6. Verify Python
echo "🏃 Verifying Python Client..."
cat > "$WORK_DIR/app.py" <<EOF
import sys
import os
import json
sys.path.append("$WORK_DIR/client_py")
# The module name is 'users' (from collection), and it should contain User and Address classes
from users import User, Address

print("✅ Creating nested objects...")
addr = Address(street="123 Main St", city="Techville", zip=90210)
user = User(id="u1", name="Alice", address=addr, past_addresses=[])

print(f"User: {user}")

# Verify nested access
assert user.address.city == "Techville"
assert user.id == "u1"

# Verify to_bytes serialization
data = user.to_bytes()
print(f"Serialized: {data}")

# Verify deserialization
user2 = User.from_bytes(data)
print(f"Deserialized: {user2}")
assert user2.address.city == "Techville"
assert user2.address.zip == 90210
# Note: simple json deserialization might give us dicts instead of Address objects for nested fields 
# unless we have smarter deserialization.
# Let's check what we got.
print(f"Type of user2.address: {type(user2.address)}")

# If our codegen uses cls(**d), nested dicts remain dicts unless __post_init__ or manual conversion happens.
# The current codegen is simple. So we EXPECT dicts for nested fields if we didn't add logic.
# However, if we improve codegen later, this test might need update.
# For now, let's just assert the data is correct.
if isinstance(user2.address, dict):
    print("⚠️  Nested field is a dict (expected with simple codegen)")
    assert user2.address['city'] == "Techville"
else:
    assert user2.address.city == "Techville"

EOF

python3 "$WORK_DIR/app.py"

# 7. Verify TypeScript
echo "🏃 Verifying TypeScript Client code..."
TS_FILE="$WORK_DIR/client_ts/users.ts"

if grep -q "export interface Address" "$TS_FILE"; then
    echo "✅ Found 'Address' interface"
else
    echo "❌ Missing 'Address' interface"
    exit 1
fi

if grep -q "export interface User" "$TS_FILE"; then
    echo "✅ Found 'User' interface"
else
    echo "❌ Missing 'User' interface"
    exit 1
fi

# Check nested field type
if grep -q "address?: Address" "$TS_FILE"; then
    echo "✅ User has 'address' of type 'Address'"
else
    echo "❌ User missing correct 'address' field"
    cat "$TS_FILE"
    exit 1
fi

echo "✅ Nested Schema Test Passed!"
