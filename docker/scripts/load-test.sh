#!/bin/bash
# PrkDB Load Testing Script with Real-time Monitoring
# Usage: ./load-test.sh [scenario] [duration]

set -e

SCENARIO=${1:-"sustained"}
DURATION=${2:-300}
BASE_PORT=50051
CLUSTER_NODES="localhost:50051,localhost:50052,localhost:50053"

echo "═══════════════════════════════════════════════════"
echo "  PrkDB Load Test - $SCENARIO scenario"
echo "  Duration: ${DURATION}s"
echo "  Grafana: http://localhost:3000"
echo "  Prometheus: http://localhost:9090"
echo "═══════════════════════════════════════════════════"
echo ""

# Check if cluster is running
check_cluster() {
    echo "Checking cluster health..."
    for port in 50051 50052 50053; do
        if ! curl -s "http://localhost:${port}/metrics" > /dev/null; then
            echo "❌ Node on port $port is not responding"
            echo "   Run: docker-compose up -d"
            exit 1
        fi
    done
    echo "✅ Cluster is healthy"
    echo ""
}

# Sustained write load
sustained_load() {
    echo "🔥 Running SUSTAINED WRITE LOAD test"
    echo "   Target: 1,000 writes/sec for ${DURATION}s"
    echo "   Watch Grafana for real-time metrics!"
    echo ""
    
    cargo run --release --example distributed_grpc_benchmark -- \
        --cluster "$CLUSTER_NODES" \
        --rate 1000 \
        --duration $DURATION \
        --operation write \
        --report-interval 10
}

# Burst traffic pattern
burst_load() {
    echo "💥 Running BURST TRAFFIC test"
    echo "   Pattern: 100 → 10,000 → 100 writes/sec"
    echo ""
    
    local phase_duration=$((DURATION / 3))
    
    echo "Phase 1: Baseline (100 writes/sec)"
    cargo run --release --example distributed_grpc_benchmark -- \
        --cluster "$CLUSTER_NODES" \
        --rate 100 \
        --duration $phase_duration \
        --operation write
    
    echo "Phase 2: BURST (10,000 writes/sec)"
    cargo run --release --example distributed_grpc_benchmark -- \
        --cluster "$CLUSTER_NODES" \
        --rate 10000 \
        --duration $phase_duration \
        --operation write
    
    echo "Phase 3: Recovery (100 writes/sec)"
    cargo run --release --example distributed_grpc_benchmark -- \
        --cluster "$CLUSTER_NODES" \
        --rate 100 \
        --duration $phase_duration \
        --operation write
}

# Mixed read/write workload
mixed_load() {
    echo "📊 Running MIXED WORKLOAD test"
    echo "   70% reads, 30% writes at 5,000 ops/sec total"
    echo ""
    
    # Run in background: 3500 reads/sec
    cargo run --release --example distributed_grpc_benchmark -- \
        --cluster "$CLUSTER_NODES" \
        --rate 3500 \
        --duration $DURATION \
        --operation read &
    READ_PID=$!
    
    # Run in foreground: 1500 writes/sec
    cargo run --release --example distributed_grpc_benchmark -- \
        --cluster "$CLUSTER_NODES" \
        --rate 1500 \
        --duration $DURATION \
        --operation write
    
    wait $READ_PID
}

# Concurrent clients scaling
concurrent_load() {
    echo "👥 Running CONCURRENT CLIENTS test"
    echo "   Scaling: 100 → 500 → 1000 clients"
    echo ""
    
    local phase_duration=$((DURATION / 3))
    
    for clients in 100 500 1000; do
        echo "Phase: $clients concurrent clients (10 writes/sec each)"
        cargo run --release --example distributed_grpc_benchmark -- \
            --cluster "$CLUSTER_NODES" \
            --clients $clients \
            --rate-per-client 10 \
            --duration $phase_duration \
            --operation write
        echo ""
    done
}

# Chaos testing with failures
chaos_load() {
    echo "💀 Running CHAOS SCENARIO test"
    echo "   Continuous load with leader kill at 60s mark"
    echo ""
    
    # Start background load
    cargo run --release --example distributed_grpc_benchmark -- \
        --cluster "$CLUSTER_NODES" \
        --rate 1000 \
        --duration $DURATION \
        --operation write &
    LOAD_PID=$!
    
    # Wait 60 seconds
    echo "⏳ Running for 60s before chaos..."
    sleep 60
    
    # Kill leader (node 1)
    echo "💀 KILLING LEADER (node 1)..."
    docker-compose stop prkdb-node1
    
    echo "⏳ Observing leader election and recovery..."
    sleep 30
    
    # Restart node
    echo "♻️  Restarting node 1..."
    docker-compose start prkdb-node1
    
    # Wait for load test to complete
    wait $LOAD_PID
}

# Quick benchmark
quick_benchmark() {
    echo "⚡ Running QUICK BENCHMARK"
    echo "   30-second snapshot across all operations"
    echo ""
    
    echo "1. Writes..."
    cargo run --release --example distributed_grpc_benchmark -- \
        --cluster "$CLUSTER_NODES" --rate 1000 --duration 30 --operation write
    
    echo "2. Reads..."
    cargo run --release --example distributed_grpc_benchmark -- \
        --cluster "$CLUSTER_NODES" --rate 5000 --duration 30 --operation read
    
    echo "3. Mixed..."
    cargo run --release --example distributed_grpc_benchmark -- \
        --cluster "$CLUSTER_NODES" --rate 2000 --duration 30 --operation mixed
}

# Main execution
check_cluster

case $SCENARIO in
    sustained)
        sustained_load
        ;;
    burst)
        burst_load
        ;;
    mixed)
        mixed_load
        ;;
    concurrent)
        concurrent_load
        ;;
    chaos)
        chaos_load
        ;;
    quick)
        quick_benchmark
        ;;
    *)
        echo "Unknown scenario: $SCENARIO"
        echo "Usage: $0 [sustained|burst|mixed|concurrent|chaos|quick] [duration]"
        exit 1
        ;;
esac

echo ""
echo "═══════════════════════════════════════════════════"
echo "  ✅ Load test complete!"
echo "  📊 Check Grafana for detailed metrics"
echo "  📈 Prometheus: http://localhost:9090"
echo "═══════════════════════════════════════════════════"
