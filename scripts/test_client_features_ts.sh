#!/bin/bash
set -euo pipefail

# Configuration
WORK_DIR="/tmp/prkdb_client_features_ts"
PRKDB_BIN="${PRKDB_BIN:-./target/debug/prkdb-cli}"
ADMIN_TOKEN="client_features_ts_test_token"
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
        # If we used cargo run, the PID might be the cargo process.
        # Killing it usually kills the child if it's the main process.
        # But we spawn server with &
        kill $(cat "$WORK_DIR/server.pid") 2>/dev/null || true
    fi
    rm -rf "$WORK_DIR"
}
trap cleanup EXIT

# Setup workspace
rm -rf "$WORK_DIR"
mkdir -p "$WORK_DIR"
mkdir -p "$WORK_DIR/client_ts"

# Check dependencies
if ! command -v node &> /dev/null; then
    echo "❌ Node.js is required but not found."
    exit 1
fi

if ! command -v npx &> /dev/null; then
    echo "❌ npx is required but not found."
    exit 1
fi

if [ "${SKIP_BUILD:-0}" != "1" ]; then
    echo "🏗️  Building prkdb binary..."
    cargo build -p prkdb-cli --bin prkdb-cli
fi

if [ ! -x "$PRKDB_BIN" ]; then
    echo "❌ Expected prkdb binary at $PRKDB_BIN"
    exit 1
fi

# Start server
echo "🚀 Starting server on port $SERVER_PORT..."
PRKDB_ADMIN_TOKEN="$ADMIN_TOKEN" \
    "$PRKDB_BIN" --database "$DATABASE_PATH" --verbose serve --port $SERVER_PORT --grpc-port $GRPC_PORT > "$WORK_DIR/server.log" 2>&1 &
SERVER_PID=$!
echo $SERVER_PID > "$WORK_DIR/server.pid"
echo "Server PID: $SERVER_PID"

# Wait for server
echo "⏳ Waiting for server..."
for i in {1..30}; do
    if curl -sf "http://127.0.0.1:$SERVER_PORT/health" > /dev/null 2>&1 \
        && PRKDB_ADMIN_TOKEN="$ADMIN_TOKEN" "$PRKDB_BIN" schema --server "http://127.0.0.1:$GRPC_PORT" list > /dev/null 2>&1; then
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
PRKDB_ADMIN_TOKEN="$ADMIN_TOKEN" "$PRKDB_BIN" schema --server "http://127.0.0.1:$GRPC_PORT" register --collection users --proto "$WORK_DIR/user.desc"

# Insert Data via CLI (since HTTP API is read-only)
echo "💾 Inserting Test Data..."
"$PRKDB_BIN" --server "http://127.0.0.1:$GRPC_PORT" collection put users '{"id": "u1", "name": "Alice", "age": 30}'
"$PRKDB_BIN" --server "http://127.0.0.1:$GRPC_PORT" collection put users '{"id": "u2", "name": "Bob", "age": 25}'
"$PRKDB_BIN" --server "http://127.0.0.1:$GRPC_PORT" collection put users '{"id": "u3", "name": "Alice", "age": 35}'


# Generate TypeScript Client
echo "⚙️  Generating TypeScript Client..."
PRKDB_ADMIN_TOKEN="$ADMIN_TOKEN" "$PRKDB_BIN" codegen \
    --server "http://127.0.0.1:$GRPC_PORT" \
    --collection "users" \
    --lang typescript \
    --out "$WORK_DIR/client_ts"

if [ ! -f "$WORK_DIR/client_ts/users.ts" ]; then
    echo "❌ TypeScript client generation failed!"
    exit 1
fi

echo "✓ Code generation complete!"

# Create Verification Script
echo "🏃 Verifying TypeScript Client Features..."
cat > "$WORK_DIR/test_client.ts" <<EOF
import { PrkDbClient, UserMeta } from './client_ts/users';

async function main() {
    const client = new PrkDbClient("http://127.0.0.1:$SERVER_PORT");

    console.log("Testing QueryBuilder...");

    // Test 1: Simple Equality
    console.log("  - Filter by name='Alice'");
    const users = await UserMeta.select(client)
        .whereNameEq("Alice")
        .execute();
    
    console.log(\`    Found \${users.length} users\`);
    
    if (users.length !== 2) {
        console.error(\`❌ Verification Failed: Expected 2 Alices, found \${users.length}\`);
        process.exit(1);
    }

    // Verify IDs (assuming returned list has IDs)
    const names = users.map(u => u.name);
    if (!names.every(n => n === "Alice")) {
         console.error(\`❌ Verification Failed: Expected all 'Alice', got \${names}\`);
         process.exit(1);
    }

    // Test 2: Simple Inequality (Bob)
    console.log("  - Filter by name='Bob'");
    const bobs = await UserMeta.select(client)
        .whereNameEq("Bob")
        .execute();

    if (bobs.length !== 1 || bobs[0].name !== "Bob") {
        console.error(\`❌ Verification Failed: Expected 1 Bob, found \${bobs.length}\`);
        process.exit(1);
    }

    console.log("✅ Client Features Verification Passed!");
}

main().catch(err => {
    console.error("❌ Test failed with exception:", err);
    process.exit(1);
});
EOF

# Initialize NPM project (needed for module resolution)
cd "$WORK_DIR"
npm init -y > /dev/null
npm install --save-dev typescript ts-node @types/node > /dev/null

# Configure tsconfig
cat > tsconfig.json <<EOF
{
  "compilerOptions": {
    "target": "es2020",
    "module": "commonjs",
    "strict": true,
    "esModuleInterop": true,
    "skipLibCheck": true,
    "forceConsistentCasingInFileNames": true,
    "types": ["node"]
  }
}
EOF

# Run Test
echo "🚀 Running TypeScript Test..."
if ! npx ts-node test_client.ts; then
    echo "❌ TypeScript Test Failed!"
    echo "📜 Server Log:"
    cat server.log
    exit 1
fi

echo "✅ All checks passed!"
