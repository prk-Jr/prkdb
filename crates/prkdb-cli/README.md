# prkdb-cli

**Command-line interface and HTTP server for PrkDB**

## Overview

`prkdb-cli` provides a powerful CLI tool for managing and monitoring PrkDB clusters, plus an optional HTTP server for web-based access and real-time updates.

## Installation

```bash
# Build from source
cd crates/prkdb-cli
cargo build --release

# Binary location
./target/release/prkdb
```

## Quick Start

```bash
# Show help
prkdb --help

# List collections
prkdb collection list

# View metrics
prkdb metrics show

# Start HTTP server
prkdb serve --port 8080 --prometheus --cors
```

## Commands

### Collection Management

```bash
# List all collections
prkdb collection list

# Create collection
prkdb collection create my_collection

# Get collection info
prkdb collection info my_collection

# Delete collection
prkdb collection delete my_collection
```

### Consumer Groups

```bash
# List consumer groups
prkdb consumer list

# Create consumer group
prkdb consumer create my_group

# Show consumer lag
prkdb consumer lag my_group
```

### Partition Operations

```bash
# List partitions
prkdb partition list

# Show partition details
prkdb partition info 0

# Rebalance partitions
prkdb partition rebalance
```

### Replication Management

```bash
# Show replication status
prkdb replication status

# Add replica
prkdb replication add-replica --partition 0 --node 2

# Remove replica
prkdb replication remove-replica --partition 0 --node 2
```

### Metrics & Monitoring

```bash
# Show current metrics
prkdb metrics show

# Export metrics (Prometheus format)
prkdb metrics export

# Watch metrics in real-time
prkdb metrics watch
```

### Database Operations

```bash
# Show database info
prkdb database info

# Backup database
prkdb database backup --path /backup/prkdb

# Restore database
prkdb database restore --path /backup/prkdb

# Compact database
prkdb database compact
```

## HTTP Server

The `serve` command starts an HTTP server with multiple endpoints:

```bash
prkdb serve --port 8080 --prometheus --cors --websockets
```

### Options

- `--port <PORT>` - HTTP port (default: 8080)
- `--host <HOST>` - Bind address (default: 127.0.0.1)
- `--prometheus` - Enable Prometheus metrics endpoint
- `--cors` - Enable CORS for web dashboards
- `--websockets` - Enable WebSocket for real-time updates

### Endpoints

With `--prometheus` enabled:
- `GET /metrics` - Prometheus metrics
- `GET /health` - Health check
- `GET /ready` - Readiness check

With `--websockets` enabled:
- `WS /ws` - Real-time updates
- `GET /subscribe/{topic}` - Topic subscription

## Configuration

### Database Location

```bash
# Specify database path
prkdb --database /path/to/db.db collection list

# Or use environment variable
export PRKDB_DATABASE=/path/to/db.db
prkdb collection list
```

### Output Formats

```bash
# Table format (default)
prkdb --format table collection list

# JSON format
prkdb --format json collection list

# YAML format
prkdb --format yaml collection list
```

### Verbosity

```bash
# Verbose output
prkdb --verbose collection list

# Or short form
prkdb -v collection list
```

## Examples

### Monitor Cluster Health

```bash
# Watch metrics in real-time
prkdb metrics watch

#Export to file
prkdb metrics export > metrics.txt

# Check replication status
prkdb replication status
```

### Manage Collections

```bash
# Create and populate
prkdb collection create users
# ... use client to add data ...
prkdb collection info users

# List all with stats
prkdb collection list --format json | jq
```

### HTTP Server for Dashboard

```bash
# Start server with all features
prkdb serve \
  --port 8080 \
  --prometheus \
  --cors \
  --websockets

# Access Prometheus metrics
curl http://localhost:8080/metrics

# WebSocket connection
wscat -c ws://localhost:8080/ws
```

## Implementation Status

### ✅ Fully Implemented
- **Collection commands** (list, describe, count, sample, data)
  - Full schema analysis
  - Pagination & filtering  
  - Multiple output formats
- **Database commands** (info, health, compact, backup)
- **Metrics commands** (show, partition, consumer, reset)
- **Serve command** - HTTP server with WebSockets
- **Output formatting** (table, JSON, YAML)

### ⚠️ Partially Implemented
- **Consumer commands** - Basic structure, needs testing
- **Partition commands** - Interface defined, implementation in progress
- **Replication commands** - Basic commands, needs cluster integration

### 📝 Verified Working
All collection commands have been tested and work with the storage backend:
```bash
# These actually work right now:
prkdb collection list
prkdb collection describe my_collection
prkdb collection count my_collection  
prkdb collection sample my_collection --limit 10
prkdb collection data my_collection --limit 20 --filter "field=value"
```

## Architecture

### CLI Flow

```
User Command
     │
     ▼
CLI Parser (clap)
     │
     ▼
Database Manager
     │
     ▼
Storage Backend
     │
     ▼
Output Formatter
     │
     ▼
Console/JSON/YAML
```

### HTTP Server

Built with `axum`:
```
HTTP Request
     │
     ▼
Router (axum)
     │
     ├─→ /metrics → Prometheus exporter
     ├─→ /health → Health checker
     └─→ /ws → WebSocket handler
```

## Development

### Building

```bash
cargo build --release
```

### Testing

```bash
# Run tests
cargo test

# Run with sample data
cargo run -- collection list
```

### Adding Commands

1. Add command enum in `src/commands.rs`
2. Implement handler in `src/commands/{name}.rs`
3. Add to match in `src/main.rs`
4. Add tests in `tests/`

## Dependencies

- `clap` - Command-line parsing
- `axum` - HTTP server
- `tokio` - Async runtime
- `serde_json` - JSON output
- `tabled` - Table formatting
- `colored` - Terminal colors

## Status

**Implemented**:
- ✅ CLI framework
- ✅ Command structure
- ✅ Output formatting
- ✅ HTTP server skeleton

**In Progress**:
- ⚠️ Collection commands
- ⚠️ Consumer commands
- ⚠️ Metrics integration

**Planned**:
- [ ] WebSocket real-time updates
- [ ] Interactive mode (REPL)
- [ ] Autocomplete
- [ ] Config file support

## Usage Tips

### Shell Completion

Generate shell completions:
```bash
# Bash
prkdb --completions bash > /etc/bash_completion.d/prkdb

# Zsh
prkdb --completions zsh > ~/.zsh/completions/_prkdb

# Fish
prkdb --completions fish > ~/.config/fish/completions/prkdb.fish
```

### Aliases

Add to `.bashrc` or `.zshrc`:
```bash
alias pk='prkdb'
alias pkm='prkdb metrics'
alias pkc='prkdb collection'
```

### JSON Processing

Combine with `jq`:
```bash
# Get collection names
prkdb collection list --format json | jq -r '.[].name'

# Filter by status
prkdb replication status --format json | jq '.[] | select(.healthy == true)'
```

## Contributing

See [CONTRIBUTING.md](../../CONTRIBUTING.md)

## License

MIT OR Apache-2.0
