#!/bin/bash
# Health check script for PrkDB cluster

set -e

echo "=== PrkDB Cluster Health Check ==="
echo ""

# Colors
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Check if services are running
echo "1. Checking Docker containers..."
docker-compose ps

echo ""
echo "2. Checking node metrics endpoints..."

check_endpoint() {
    local name=$1
    local url=$2
    
    if curl -sf "$url" > /dev/null; then
        echo -e "${GREEN}✓${NC} $name is healthy"
        return 0
    else
        echo -e "${RED}✗${NC} $name is NOT responding"
        return 1
    fi
}

check_endpoint "Node 1 Metrics" "http://localhost:9090/metrics"
check_endpoint "Node 2 Metrics" "http://localhost:9091/metrics"
check_endpoint "Node 3 Metrics" "http://localhost:9092/metrics"
check_endpoint "Prometheus" "http://localhost:9093/-/healthy"
check_endpoint "Grafana" "http://localhost:3000/api/health"

echo ""
echo "3. Checking Raft cluster status..."

# Extract leader count from metrics
LEADER_COUNT=$(curl -s http://localhost:9090/metrics http://localhost:9091/metrics http://localhost:9092/metrics | \
    grep 'prkdb_raft_state{' | grep 'partition="0"' | awk '{print $2}' | grep -c '^1$' || true)

echo "   Leaders detected: $LEADER_COUNT"

if [ "$LEADER_COUNT" -eq 1 ]; then
    echo -e "   ${GREEN}✓${NC} Cluster has exactly one leader (healthy)"
elif [ "$LEADER_COUNT" -eq 0 ]; then
    echo -e "   ${YELLOW}⚠${NC} No leader detected (election in progress?)"
else
    echo -e "   ${RED}✗${NC} Multiple leaders detected (split-brain!)"
fi

echo ""
echo "=== Health Check Complete ==="
