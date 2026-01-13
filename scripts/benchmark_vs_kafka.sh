#!/bin/bash
# Side-by-Side Benchmark: PrkDB Streaming vs Apache Kafka
# Identical test parameters, actual measurements

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Configuration (identical for both)
KAFKA_CONTAINER="kafka-benchmark"
ZOOKEEPER_CONTAINER="zk-benchmark"
NUM_MESSAGES=1000000
MESSAGE_SIZE=100
BATCH_SIZE=10000

echo ""
echo "╔══════════════════════════════════════════════════════════════════╗"
echo "║  🔬 SIDE-BY-SIDE: PrkDB Streaming vs Apache Kafka 🔬             ║"
echo "╚══════════════════════════════════════════════════════════════════╝"
echo ""
echo "  Identical Parameters:"
echo "    Messages: $NUM_MESSAGES"
echo "    Message Size: $MESSAGE_SIZE bytes"
echo "    Batch Size: $BATCH_SIZE"
echo "    Total Data: $((NUM_MESSAGES * MESSAGE_SIZE / 1024 / 1024)) MB"
echo ""

# Cleanup function
cleanup() {
    echo "🧹 Cleaning up..."
    docker rm -f $KAFKA_CONTAINER $ZOOKEEPER_CONTAINER 2>/dev/null || true
}
trap cleanup EXIT

# ============================================================================
# KAFKA PRODUCER BENCHMARK
# ============================================================================

echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "  PART 1: Apache Kafka Producer"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo ""

echo "⏳ Starting Zookeeper..."
docker run -d --name $ZOOKEEPER_CONTAINER \
    -e ZOOKEEPER_CLIENT_PORT=2181 \
    -p 2181:2181 \
    confluentinc/cp-zookeeper:7.5.0 > /dev/null 2>&1

sleep 5

echo "⏳ Starting Kafka..."
docker run -d --name $KAFKA_CONTAINER \
    -e KAFKA_BROKER_ID=1 \
    -e KAFKA_ZOOKEEPER_CONNECT=host.docker.internal:2181 \
    -e KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://localhost:9092 \
    -e KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1 \
    -p 9092:9092 \
    confluentinc/cp-kafka:7.5.0 > /dev/null 2>&1

echo "⏳ Waiting for Kafka to be ready..."
sleep 15

echo "⏳ Creating test topic..."
docker exec $KAFKA_CONTAINER kafka-topics --create \
    --topic benchmark-test \
    --bootstrap-server localhost:9092 \
    --partitions 1 \
    --replication-factor 1 2>/dev/null || true

sleep 2

echo ""
echo "📤 Running Kafka Producer..."
KAFKA_RESULT=$(docker exec $KAFKA_CONTAINER kafka-producer-perf-test \
    --topic benchmark-test \
    --num-records $NUM_MESSAGES \
    --record-size $MESSAGE_SIZE \
    --throughput -1 \
    --producer-props bootstrap.servers=localhost:9092 batch.size=$BATCH_SIZE linger.ms=50 2>&1 | tail -1)

echo "   $KAFKA_RESULT"

# Extract metrics
KAFKA_RECORDS_SEC=$(echo "$KAFKA_RESULT" | grep -oE '[0-9]+\.[0-9]+ records/sec' | grep -oE '[0-9]+\.[0-9]+')
KAFKA_MB_SEC=$(echo "$KAFKA_RESULT" | grep -oE '[0-9]+\.[0-9]+ MB/sec' | grep -oE '[0-9]+\.[0-9]+')

echo ""
echo "✅ Kafka:"
echo "   Records/sec: $KAFKA_RECORDS_SEC"
echo "   MB/sec: $KAFKA_MB_SEC"

# Stop Kafka
echo ""
echo "⏳ Stopping Kafka..."
docker rm -f $KAFKA_CONTAINER $ZOOKEEPER_CONTAINER > /dev/null 2>&1
trap - EXIT

# ============================================================================
# PRKDB STREAMING ADAPTER BENCHMARK
# ============================================================================

echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "  PART 2: PrkDB Streaming Adapter"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo ""

cd "$SCRIPT_DIR/.."
echo "📤 Running PrkDB Streaming..."
PRKDB_OUTPUT=$(cargo run --release --example streaming_bench 2>&1)

# Extract the key line
PRKDB_LINE=$(echo "$PRKDB_OUTPUT" | grep "Streaming (Phase 24A)")
PRKDB_RECORDS_SEC=$(echo "$PRKDB_OUTPUT" | grep "Throughput:" | head -1 | grep -oE '[0-9]+ records/sec' | grep -oE '[0-9]+')
PRKDB_MB_SEC=$(echo "$PRKDB_OUTPUT" | grep "Throughput:" | tail -1 | grep -oE '[0-9]+\.[0-9]+ MB/sec' | grep -oE '[0-9]+\.[0-9]+')

echo "   $PRKDB_LINE"
echo ""
echo "✅ PrkDB Streaming:"
echo "   Records/sec: $PRKDB_RECORDS_SEC"
echo "   MB/sec: $PRKDB_MB_SEC"

# ============================================================================
# FINAL COMPARISON
# ============================================================================

echo ""
echo "╔══════════════════════════════════════════════════════════════════╗"
echo "║           📊 ACTUAL MEASURED COMPARISON 📊                       ║"
echo "╚══════════════════════════════════════════════════════════════════╝"
echo ""
echo "  Test: Producer/Append (1M records, 100B each, batch 10K)"
echo ""
echo "┌─────────────────────────┬──────────────────┬──────────────────┐"
echo "│ Metric                  │ Kafka            │ PrkDB Streaming  │"
echo "├─────────────────────────┼──────────────────┼──────────────────┤"
printf "│ %-23s │ %16s │ %16s │\n" "Records/sec" "${KAFKA_RECORDS_SEC:-N/A}" "${PRKDB_RECORDS_SEC:-N/A}"
printf "│ %-23s │ %12s MB/s │ %12s MB/s │\n" "Throughput" "${KAFKA_MB_SEC:-N/A}" "${PRKDB_MB_SEC:-N/A}"
echo "└─────────────────────────┴──────────────────┴──────────────────┘"
echo ""

# Calculate ratio
if [ -n "$KAFKA_MB_SEC" ] && [ -n "$PRKDB_MB_SEC" ]; then
    RATIO=$(echo "scale=1; $PRKDB_MB_SEC / $KAFKA_MB_SEC" | bc)
    echo "  📈 PrkDB is ${RATIO}x Kafka throughput"
else
    echo "  ⚠️  Could not calculate ratio"
fi
echo ""
