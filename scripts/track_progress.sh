#!/bin/bash

# Performance Roadmap Progress Tracker
# Usage: ./scripts/track_progress.sh [command]

set -e

ROADMAP="PERFORMANCE_ROADMAP.md"
DOCS_DIR="docs"

# Colors
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

show_status() {
    echo -e "${BLUE}═══════════════════════════════════════════════════════${NC}"
    echo -e "${BLUE}        PrkDB Performance Roadmap Status${NC}"
    echo -e "${BLUE}═══════════════════════════════════════════════════════${NC}"
    echo ""
    
    # Count checkboxes
    total=$(grep -c '\- \[ \]' "$ROADMAP" || true)
    completed=$(grep -c '\- \[x\]' "$ROADMAP" || true)
    
    if [ "$total" -eq 0 ]; then
        total=1  # Avoid division by zero
    fi
    
    percentage=$((completed * 100 / total))
    
    echo -e "${GREEN}✅ Completed:${NC} $completed tasks"
    echo -e "${YELLOW}⏳ Remaining:${NC} $total tasks"
    echo -e "${BLUE}📊 Progress:${NC} $percentage%"
    echo ""
    
    # Show phase status
    echo -e "${BLUE}Phase Status:${NC}"
    grep "^- \[.\] \*\*Phase" "$ROADMAP" | while read line; do
        if echo "$line" | grep -q "\[x\]"; then
            echo -e "  ${GREEN}✅${NC} $line"
        else
            echo -e "  ${RED}⏳${NC} $line"
        fi
    done
    
    echo ""
    echo -e "${BLUE}═══════════════════════════════════════════════════════${NC}"
}

show_next_tasks() {
    echo -e "${BLUE}Next Tasks:${NC}"
    echo ""
    
    # Find first uncompleted phase
    phase_started=false
    grep -A 5 "^- \[ \] \*\*Phase" "$ROADMAP" | head -20 | while read line; do
        if echo "$line" | grep -q "^- \[ \]"; then
            echo -e "  ${YELLOW}➤${NC} $line"
        fi
    done
}

log_progress() {
    local message="$1"
    local date=$(date "+%B %d, %Y")
    
    echo ""
    echo "📝 Logging progress..."
    
    # Find the progress log section and add entry
    # This is a simplified version - you'd want to use a proper text editor
    echo ""
    echo -e "${GREEN}Add this to Progress Log section:${NC}"
    echo ""
    echo "### $date"
    echo "- $message"
    echo ""
}

run_benchmarks() {
    echo -e "${BLUE}Running benchmarks...${NC}"
    echo ""
    
    # Run key benchmarks
    echo "1. Batch operations..."
    cargo bench --bench batch_bench -- --quick 2>&1 | grep -E "time:|thrpt:" || true
    
    echo ""
    echo "2. MmapParallelWal..."
    cargo bench --bench mmap_parallel_wal_bench -- --quick 2>&1 | grep -E "time:|thrpt:" || true
    
    echo ""
    echo -e "${GREEN}✅ Benchmarks complete${NC}"
}

show_docs() {
    echo -e "${BLUE}Performance Documentation:${NC}"
    echo ""
    echo "📋 $ROADMAP - Main roadmap (START HERE)"
    echo "📊 $DOCS_DIR/BENCHMARK_VERIFICATION.md - Current state verification"
    echo "🗺️  $DOCS_DIR/PERFORMANCE_IMPLEMENTATION_PLAN.md - Detailed implementation guide"
    echo "📖 $DOCS_DIR/PERFORMANCE_README.md - How to use the docs"
    echo ""
}

show_help() {
    cat << EOF
Performance Roadmap Progress Tracker

USAGE:
    ./scripts/track_progress.sh [COMMAND]

COMMANDS:
    status      Show current progress and phase status (default)
    next        Show next tasks to work on
    log [msg]   Add progress log entry
    bench       Run key benchmarks
    docs        Show all documentation files
    help        Show this help message

EXAMPLES:
    # Show current status
    ./scripts/track_progress.sh

    # Show next tasks
    ./scripts/track_progress.sh next

    # Log progress
    ./scripts/track_progress.sh log "Fixed storage_bench.rs"

    # Run benchmarks
    ./scripts/track_progress.sh bench

EOF
}

# Main command dispatcher
case "${1:-status}" in
    status)
        show_status
        ;;
    next)
        show_next_tasks
        ;;
    log)
        if [ -z "$2" ]; then
            echo "Error: Please provide a message"
            echo "Usage: $0 log \"your message\""
            exit 1
        fi
        log_progress "$2"
        ;;
    bench)
        run_benchmarks
        ;;
    docs)
        show_docs
        ;;
    help|--help|-h)
        show_help
        ;;
    *)
        echo "Unknown command: $1"
        echo ""
        show_help
        exit 1
        ;;
esac
