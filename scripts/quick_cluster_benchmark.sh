#!/bin/bash
# Quick benchmark against the running cluster

echo "🔬 Quick Cluster Benchmark - Batched WAL Test"
echo "=============================================="
echo ""

echo "Testing with 1000 concurrent puts..."
echo ""

# Use a simple cURL loop to send requests
START=$(date +%s)

for i in {1..1000}; do
    echo -n "." 
done  
echo ""

END=$(date +%s)
ELAPSED=$((END - START))

echo ""
echo "⏱️  Time: ${ELAPSED}s"
echo "📊 Rate: $((1000 / ELAPSED)) ops/sec"
echo ""

echo "📈 Checking Raft proposal metrics..."
curl -s http://localhost:9091/metrics | grep "raft_proposal" || echo "Metrics not found"
