#!/bin/bash
# Start a 3-node PrkDB cluster locally to test batched WAL performance

set -e

echo "🚀 Starting 3-Node PrkDB Cluster (Local)"
echo "========================================"
echo ""

# Clean up any old data
rm -rf /tmp/prkdb-node{1,2,3} 2>/dev/null || true

# Kill any old processes
pkill -f "prkdb-server" 2>/dev/null || true
sleep 2

# Start Node 1
echo "Starting Node 1 (ports: 50051/9091)..."
RUST_LOG=info NODE_ID=1 GRPC_PORT=50051 \
  CLUSTER_NODES="1@127.0.0.1:50051,2@127.0.0.1:50052,3@127.0.0.1:50053" \
  STORAGE_PATH=/tmp/prkdb-node1 \
  NUM_PARTITIONS=9 \
  ./target/release/prkdb-server > /tmp/node1.log 2>&1 &
NODE1_PID=$!

sleep 2

# Start Node 2  
echo "Starting Node 2 (ports: 50052/9092)..."
RUST_LOG=info NODE_ID=2 GRPC_PORT=50052 \
  CLUSTER_NODES="1@127.0.0.1:50051,2@127.0.0.1:50052,3@127.0.0.1:50053" \
  STORAGE_PATH=/tmp/prkdb-node2 \
  NUM_PARTITIONS=9 \
  ./target/release/prkdb-server > /tmp/node2.log 2>&1 &
NODE2_PID=$!

sleep 2

# Start Node 3
echo "Starting Node 3 (ports: 50053/9093)..."
RUST_LOG=info NODE_ID=3 GRPC_PORT=50053 \
  CLUSTER_NODES="1@127.0.0.1:50051,2@127.0.0.1:50052,3@127.0.0.1:50053" \
  STORAGE_PATH=/tmp/prkdb-node3 \
  NUM_PARTITIONS=9 \
  ./target/release/prkdb-server > /tmp/node3.log 2>&1 &
NODE3_PID=$!

echo ""
echo "✅ All nodes started!"
echo "   Node 1 PID: $NODE1_PID"
echo "   Node 2 PID: $NODE2_PID"  
echo "   Node 3 PID: $NODE3_PID"
echo ""
echo "Waiting 10 seconds for cluster to stabilize..."
sleep 10

echo ""
echo "📊 Checking node health..."
for port in 9091 9092 9093; do
    if curl -s http://localhost:$port/metrics > /dev/null 2>&1; then
        echo "  ✅ Node on port $port: HEALTHY"
    else
        echo "  ❌ Node on port $port: NOT RESPONDING"
    fi
done

echo ""
echo "🎯 Cluster is ready!"
echo ""
echo "To stop the cluster:"
echo "  kill $NODE1_PID $NODE2_PID $NODE3_PID"
echo ""
echo "Logs at:"
echo "  /tmp/node{1,2,3}.log"
echo ""
echo "Metrics at:"
echo "  http://localhost:9091/metrics"
echo "  http://localhost:9092/metrics"
echo "  http://localhost:9093/metrics"
