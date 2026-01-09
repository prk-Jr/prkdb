#!/bin/bash
# PrkDB 3-Node Raft Cluster - One Command Start
# Usage: ./scripts/start_cluster.sh

set -e

echo ""
echo "🔐 PrkDB 3-Node Raft Cluster"
echo "============================"
echo ""

# Cleanup any existing cluster
pkill -f "raft_node" 2>/dev/null || true

# Create data directories
DATA_DIR="/tmp/prkdb_cluster"
rm -rf "$DATA_DIR" 2>/dev/null || true
mkdir -p "$DATA_DIR/node1" "$DATA_DIR/node2" "$DATA_DIR/node3"

cd "$(dirname "$0")/.."

# Build if needed
if [ ! -f "target/release/examples/raft_node" ]; then
    echo "⏳ Building raft_node..."
    cargo build --release --example raft_node
fi

echo "📁 Data: $DATA_DIR"
echo ""

# Start all 3 nodes
echo "🚀 Starting 3 nodes..."
echo ""

RUST_LOG=info ./target/release/examples/raft_node \
    --node-id 1 \
    --listen 127.0.0.1:50051 \
    --peers 2=127.0.0.1:50052,3=127.0.0.1:50053 \
    --data-dir "$DATA_DIR/node1" > /tmp/raft_node1.log 2>&1 &
PID1=$!

sleep 0.5

RUST_LOG=info ./target/release/examples/raft_node \
    --node-id 2 \
    --listen 127.0.0.1:50052 \
    --peers 1=127.0.0.1:50051,3=127.0.0.1:50053 \
    --data-dir "$DATA_DIR/node2" > /tmp/raft_node2.log 2>&1 &
PID2=$!

sleep 0.5

RUST_LOG=info ./target/release/examples/raft_node \
    --node-id 3 \
    --listen 127.0.0.1:50053 \
    --peers 1=127.0.0.1:50051,2=127.0.0.1:50052 \
    --data-dir "$DATA_DIR/node3" > /tmp/raft_node3.log 2>&1 &
PID3=$!

sleep 1

echo "✅ Cluster Started!"
echo ""
echo "   Node 1: 127.0.0.1:50051 (PID: $PID1)"
echo "   Node 2: 127.0.0.1:50052 (PID: $PID2)"
echo "   Node 3: 127.0.0.1:50053 (PID: $PID3)"
echo ""
echo "� Logs:"
echo "   tail -f /tmp/raft_node1.log"
echo "   tail -f /tmp/raft_node2.log"
echo "   tail -f /tmp/raft_node3.log"
echo ""
echo "🛑 To stop: pkill -f raft_node"
echo ""

# Verify ports
sleep 1
echo "📊 Verifying ports..."
if lsof -i :50051 -i :50052 -i :50053 2>/dev/null | grep -q LISTEN; then
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
tail -f /tmp/raft_node1.log /tmp/raft_node2.log /tmp/raft_node3.log
