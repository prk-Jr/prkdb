#!/bin/bash
set -e

# Configuration
HTTP_PORT=9091
GRPC_PORT=50051
SERVER_URL="http://127.0.0.1:$GRPC_PORT"
ADMIN_TOKEN="secret-token-123"
WRONG_TOKEN="wrong-token"
DB_FILE="./remote_test.db"

# Cleanup
rm -rf "$DB_FILE"
lsof -ti:$HTTP_PORT | xargs kill -9 2>/dev/null || true
lsof -ti:$GRPC_PORT | xargs kill -9 2>/dev/null || true

echo "🚀 Starting PrkDB Server (HTTP: $HTTP_PORT, gRPC: $GRPC_PORT)..."
PRKDB_ADMIN_TOKEN="$ADMIN_TOKEN" cargo run -p prkdb-cli -- --database "$DB_FILE" serve --port $HTTP_PORT --grpc-port $GRPC_PORT --host 127.0.0.1 > server.log 2>&1 &
SERVER_PID=$!
echo "Server PID: $SERVER_PID"

# Wait for gRPC server port to be open
echo "Waiting for gRPC server to listen on port $GRPC_PORT..."
for i in {1..30}; do
    if lsof -i:$GRPC_PORT >/dev/null; then
        echo "✅ Server is listening on gRPC port!"
        break
    fi
    sleep 1
    if [ $i -eq 30 ]; then
        echo "❌ Server failed to start within 30 seconds."
        cat server.log
        kill $SERVER_PID
        exit 1
    fi
done

# Function to run CLI command
run_cli() {
    cargo run -q -p prkdb-cli -- --server "$SERVER_URL" "$@"
}

echo "📂 Listing collections (should be empty)..."
PRKDB_ADMIN_TOKEN="$ADMIN_TOKEN" run_cli collection list

echo "🆕 Creating collection 'demo_users'..."
PRKDB_ADMIN_TOKEN="$ADMIN_TOKEN" run_cli collection create demo_users

echo "📂 Listing collections (should have demo_users)..."
PRKDB_ADMIN_TOKEN="$ADMIN_TOKEN" run_cli collection list

echo "👥 Double check: Listing with WRONG token (should fail)..."
if PRKDB_ADMIN_TOKEN="$WRONG_TOKEN" run_cli collection list; then
    echo "❌ Expected failure (authentication), but succeeded!"
    exit 1
else
    echo "✅ Authentication check passed (access denied)."
fi

echo "🗑️ Dropping collection 'demo_users'..."
PRKDB_ADMIN_TOKEN="$ADMIN_TOKEN" run_cli collection drop demo_users

echo "📂 Listing collections (should be empty)..."
PRKDB_ADMIN_TOKEN="$ADMIN_TOKEN" run_cli collection list

echo "✅ Verification Complete!"

# Cleanup
kill $SERVER_PID
rm -rf "$DB_FILE"
rm -f server.log
