#!/bin/bash
# PrkDB 3-Node Raft Cluster - One Command Start
# Usage: ./scripts/start_cluster.sh

set -e

echo ""
echo "🔐 PrkDB 3-Node Raft Cluster"
echo "============================"
echo ""

# Cleanup any existing cluster
pkill -f "prkdb-server" 2>/dev/null || true

# Create data directories
DATA_DIR="/tmp/prkdb_cluster"
rm -rf "$DATA_DIR" 2>/dev/null || true
mkdir -p "$DATA_DIR/node1" "$DATA_DIR/node2" "$DATA_DIR/node3"

cd "$(dirname "$0")/.."

# Build if needed
if [ ! -f "target/release/prkdb-server" ]; then
    echo "⏳ Building prkdb-server..."
    cargo build --release -p prkdb --bin prkdb-server
fi

echo "📁 Data: $DATA_DIR"
echo ""

# Start all 3 nodes
echo "🚀 Starting 3 nodes..."
echo ""

NODE_ID=1 \
CLUSTER_NODES=1@127.0.0.1:8080,2@127.0.0.1:8081,3@127.0.0.1:8082 \
STORAGE_PATH="$DATA_DIR/node1" \
RUST_LOG=info \
./target/release/prkdb-server > /tmp/prkdb_server1.log 2>&1 &
PID1=$!

sleep 0.5

NODE_ID=2 \
CLUSTER_NODES=1@127.0.0.1:8080,2@127.0.0.1:8081,3@127.0.0.1:8082 \
STORAGE_PATH="$DATA_DIR/node2" \
RUST_LOG=info \
./target/release/prkdb-server > /tmp/prkdb_server2.log 2>&1 &
PID2=$!

sleep 0.5

NODE_ID=3 \
CLUSTER_NODES=1@127.0.0.1:8080,2@127.0.0.1:8081,3@127.0.0.1:8082 \
STORAGE_PATH="$DATA_DIR/node3" \
RUST_LOG=info \
./target/release/prkdb-server > /tmp/prkdb_server3.log 2>&1 &
PID3=$!

sleep 1

echo "✅ Cluster Started!"
echo ""
echo "   Node 1: 127.0.0.1:8080 (PID: $PID1)"
echo "   Node 2: 127.0.0.1:8081 (PID: $PID2)"
echo "   Node 3: 127.0.0.1:8082 (PID: $PID3)"
echo ""
echo "� Logs:"
echo "   tail -f /tmp/prkdb_server1.log"
echo "   tail -f /tmp/prkdb_server2.log"
echo "   tail -f /tmp/prkdb_server3.log"
echo ""
echo "🛑 To stop: pkill -f prkdb-server"
echo ""

# Verify ports
sleep 1
echo "📊 Verifying ports..."
if lsof -i :8080 -i :8081 -i :8082 2>/dev/null | grep -q LISTEN; then
    echo "✅ All 3 gRPC servers are LISTENING!"
else
    echo "⚠️  Some nodes may not have started yet. Check logs."
fi

echo ""
echo "🎉 Cluster is ready!"
echo ""
echo "Press Ctrl+C to stop watching (cluster keeps running)"
echo "──────────────────────────────────────────────────────"
echo ""

# Show combined logs
tail -f /tmp/prkdb_server1.log /tmp/prkdb_server2.log /tmp/prkdb_server3.log
