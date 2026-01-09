#!/bin/bash
# Test script for PrkDB cluster - performs basic write/read operations

set -e

echo "=== PrkDB Cluster Test ==="
echo ""

# Colors
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
NC='\033[0m'

# Check if grpcurl is installed
if ! command -v grpcurl &> /dev/null; then
    echo -e "${RED}✗${NC} grpcurl is not installed"
    echo "   Install with: brew install grpcurl (Mac) or apt-get install grpcurl (Linux)"
    exit 1
fi

echo "1. Testing write operation..."

# Base64 encode test data
KEY_B64=$(echo -n "test_key" | base64)
VALUE_B64=$(echo -n "test_value_$(date +%s)" | base64)

# Try to write to node 1
WRITE_RESULT=$(grpcurl -plaintext -d "{\"key\":\"$KEY_B64\",\"value\":\"$VALUE_B64\"}" \
    localhost:8080 prkdb.PrkDbService.Put 2>&1 || true)

if echo "$WRITE_RESULT" | grep -q "success.*true"; then
    echo -e "${GREEN}✓${NC} Write succeeded"
else
    echo -e "${RED}✗${NC} Write failed: $WRITE_RESULT"
    exit 1
fi

sleep 1

echo ""
echo "2. Testing read operation..."

# Try to read from node 2 (tests replication)
READ_RESULT=$(grpcurl -plaintext -d "{\"key\":\"$KEY_B64\"}" \
    localhost:8081 prkdb.PrkDbService.Get 2>&1 || true)

if echo "$READ_RESULT" | grep -q "found.*true"; then
    echo -e "${GREEN}✓${NC} Read succeeded (data replicated)"
    
    # Extract and decode value
    DECODED_VALUE=$(echo "$READ_RESULT" | grep -o '"value": "[^"]*"' | cut -d'"' -f4 | base64 -d 2>/dev/null || echo "")
    echo "   Retrieved value: $DECODED_VALUE"
else
    echo -e "${YELLOW}⚠${NC} Read failed (replication delay?): $READ_RESULT"
fi

echo ""
echo "3. Testing read from node 3..."

READ_RESULT_3=$(grpcurl -plaintext -d "{\"key\":\"$KEY_B64\"}" \
    localhost:8082 prkdb.PrkDbService.Get 2>&1 || true)

if echo "$READ_RESULT_3" | grep -q "found.*true"; then
    echo -e "${GREEN}✓${NC} Read from node 3 succeeded"
else
    echo -e "${YELLOW}⚠${NC} Read from node 3 failed (replication delay?)"
fi

echo ""
echo "4. Metrics check..."

# Check commit index progression
NODE1_COMMIT=$(curl -s http://localhost:9090/metrics | grep 'prkdb_raft_commit_index{' | head -1 | awk '{print $2}')
NODE2_COMMIT=$(curl -s http://localhost:9091/metrics | grep 'prkdb_raft_commit_index{' | head -1 | awk '{print $2}')
NODE3_COMMIT=$(curl -s http://localhost:9092/metrics | grep 'prkdb_raft_commit_index{' | head -1 | awk '{print $2}')

echo "   Node 1 commit index: $NODE1_COMMIT"
echo "   Node 2 commit index: $NODE2_COMMIT"
echo "   Node 3 commit index: $NODE3_COMMIT"

if [ ! -z "$NODE1_COMMIT" ] && [ "$NODE1_COMMIT" -gt 0 ]; then
    echo -e "   ${GREEN}✓${NC} Cluster is processing operations"
fi

echo ""
echo "=== Cluster Test Complete ==="
