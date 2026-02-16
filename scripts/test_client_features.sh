#!/bin/bash
set -e

# Configuration
SERVER_PORT=50055
GRPC_PORT=50052
WORK_DIR="/tmp/prkdb_client_features"
PRKDB_BIN="./target/debug/prkdb-cli"

mkdir -p "$WORK_DIR"
rm -rf "$WORK_DIR"/*

echo "🏗️  Building prkdb binary..."
cargo build -p prkdb-cli

echo "🚀 Starting server on port $SERVER_PORT..."
# Start server in background
$PRKDB_BIN --verbose serve --port $SERVER_PORT --grpc-port $GRPC_PORT > "$WORK_DIR/server.log" 2>&1 &
SERVER_PID=$!
echo "Server PID: $SERVER_PID"

cleanup() {
    echo "🧹 Cleaning up..."
    kill $SERVER_PID || true
    wait $SERVER_PID || true
    rm -rf prkdb.db # Cleanup default db if created
}
trap cleanup EXIT

echo "⏳ Waiting for server..."
sleep 2

# Define Schema
echo "📝 Defining Schema..."
cat > "$WORK_DIR/user.proto" <<EOF
syntax = "proto3";
package models;

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
$PRKDB_BIN schema --server "http://127.0.0.1:$GRPC_PORT" register --collection users --proto "$WORK_DIR/user.desc"

# Insert Data via CLI (since Python client is read-only HTTP for now)
echo "💾 Inserting Test Data..."
$PRKDB_BIN --server "http://127.0.0.1:$GRPC_PORT" collection put users '{"id": "u1", "name": "Alice", "age": 30}'
$PRKDB_BIN --server "http://127.0.0.1:$GRPC_PORT" collection put users '{"id": "u2", "name": "Bob", "age": 25}'
$PRKDB_BIN --server "http://127.0.0.1:$GRPC_PORT" collection put users '{"id": "u3", "name": "Alice", "age": 35}'

# Generate Client
echo "⚙️  Generating Python Client..."
$PRKDB_BIN codegen \
    --server "http://127.0.0.1:$GRPC_PORT" \
    --lang python \
    --out "$WORK_DIR/client_py" \
    --collection users

# Verify Generated Files
if [ ! -f "$WORK_DIR/client_py/prkdb_client.py" ]; then
    echo "❌ Error: prkdb_client.py was not generated!"
    exit 1
fi
if [ ! -f "$WORK_DIR/client_py/users.py" ]; then
    echo "❌ Error: users.py was not generated!"
    exit 1
fi

# Debug: Curl request to see actual error
# Debug: Curl request to see actual error
echo "🔎 Debugging Data Endpoint..."
curl -v "http://127.0.0.1:$SERVER_PORT/health"
curl -v "http://127.0.0.1:$SERVER_PORT/collections"
curl -v "http://127.0.0.1:$SERVER_PORT/collections/users"
curl -v "http://127.0.0.1:$SERVER_PORT/collections/users/data?filter=name=Alice" || true
echo ""

# Run Python Verification Script
echo "🏃 Verifying Python Client Features..."
cat > "$WORK_DIR/test_client.py" <<EOF
import asyncio
import sys
import os

# Add generated client to path
sys.path.append("$WORK_DIR")

from client_py.users import User
from client_py.prkdb_client import PrkDbClient

async def test_query_builder():
    print("Testing QueryBuilder...")
    # httpx client needs to be used as async context manager or closed
    async with PrkDbClient(host="http://127.0.0.1:$SERVER_PORT") as client:
    
        # Test 1: Simple Equality
        print("  - Filter by name='Alice'")
        # execute() should be awaitable
        query = User.select().where_name_eq("Alice")
        # generated code might return a coroutine from execute()
        users = await query.execute(client)
        
        print(f"    Found {len(users)} users")
        assert len(users) == 2, f"Expected 2 Alices, found {len(users)}"
        for u in users:
            assert u.name == "Alice"
            
        # Test 2: Filter by name='Bob'
        print("  - Filter by name='Bob'")
        users = await User.select().where_name_eq("Bob").execute(client)
        assert len(users) == 1
        assert users[0].name == "Bob"
        
        # Test 3: No Match
        print("  - Filter by name='Charlie'")
        users = await User.select().where_name_eq("Charlie").execute(client)
        assert len(users) == 0

async def test_replay():
    print("Testing Stateful Compute (Replay)...")
    async with PrkDbClient(host="http://127.0.0.1:$SERVER_PORT") as client:
    
        class MutableHandler:
            def init_state(self):
                return {"total_age": 0}
                
            def handle(self, state, item):
                state["total_age"] += item.get('age', 0)
                
        handler = MutableHandler()
        final_state = await client.replay_collection("users", handler)
        print(f"  - Final state: {final_state}")
        
        expected_sum = 30 + 25 + 35 # 90
        assert final_state["total_age"] == expected_sum, f"Expected 90, got {final_state['total_age']}"

async def main():
    try:
        await test_query_builder()
        await test_replay()
        print("✅ Client Features Verification Passed!")
    except Exception as e:
        print(f"❌ Verification Failed: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    asyncio.run(main())
EOF

python3 "$WORK_DIR/test_client.py"
