# PrkDB Grafana Dashboard

## Overview

Professional monitoring dashboard for PrkDB Raft consensus operations. Provides real-time visualization of cluster health, replication status, and system metrics.

## Features

### 7 Monitoring Panels

1. **Raft Node States** - Current state of each node (Leader/Follower/Candidate)
2. **Raft Term Progression** - Term evolution tracking
3. **Leader Elections** - Election frequency monitoring
4. **Commit Index Progression** - Log replication progress
5. **Heartbeat Rates** - Sent vs Failed heartbeats per peer
6. **AppendEntries Success Rate** - Replication health gauge
7. **Server Health Status** - Up/Down status per node

### Key Metrics

- `prkdb_raft_state` - Node roles
- `prkdb_raft_leader_elections_total` - Election counter
- `prkdb_raft_commit_index` - Log position
- `prkdb_raft_heartbeats_sent/failed_total` - Network health
- `prkdb_up` - Server availability

## Quick Start

### 1. Start PrkDB with Metrics

```bash
# Build server
cargo build --release --bin prkdb-server

# Run (metrics on port 9090)
./target/release/prkdb-server
```

### 2. Start Prometheus

Create `prometheus.yml`:
```yaml
global:
  scrape_interval: 5s

scrape_configs:
  - job_name: 'prkdb'
    static_configs:
      - targets: ['localhost:9090']
        labels:
          cluster: 'local'
```

Run Prometheus:
```bash
docker run -d -p 9091:9090 \
  -v $(pwd)/prometheus.yml:/etc/prometheus/prometheus.yml \
  prom/prometheus
```

### 3. Start Grafana

```bash
docker run -d -p 3000:3000 grafana/grafana
```

### 4. Import Dashboard

1. Open Grafana: http://localhost:3000 (admin/admin)
2. Add Prometheus data source:
   - Configuration → Data Sources → Add data source
   - Select "Prometheus"
   - URL: `http://host.docker.internal:9091` (Mac/Windows) or `http://172.17.0.1:9091` (Linux)
   - Click "Save & Test"
3. Import dashboard:
   - + → Import
   - Upload `prkdb_raft.json`
   - Select Prometheus data source
   - Click Import

## Dashboard Panels Explained

### Raft Node States
Shows current role of each node:
- **Green (1)**: Leader - accepting writes
- **Blue (2)**: Follower - replicating from leader
- **Yellow (3)**: Candidate - election in progress

**Alert if**: Multiple leaders (split-brain) or no leader for >30s

### Leader Elections (Rate)
Tracks elections over time.
- **Normal**: 1 initial election, then stable
- **Warning**: >2 elections per 5 minutes indicates network issues

### Commit Index Progression
Visualizes log replication progress.
- **Healthy**: All nodes have similar commit index (within 10-100 entries)
- **Problem**: Large gaps (>1000) indicate slow follower or partition

### Heartbeat Rates
Shows heartbeat traffic between nodes.
- **Sent**: Expected ~5/sec (200ms interval)
- **Failed**: Should be near 0
- **High failure rate**: Network problems or crashed nodes

### AppendEntries Success Rate
Overall replication health gauge.
- **Green (>95%)**: Healthy cluster
- **Yellow (80-95%)**: Some issues, investigate
- **Red (<80%)**: Major problems, check network/nodes

## Alerting Examples

Add to Prometheus `alerts.yml`:

```yaml
groups:
  - name: prkdb_raft
    rules:
      - alert: HighElectionRate
        expr: rate(prkdb_raft_leader_elections_total[5m]) > 2
        for: 5m
        labels:
          severity: warning
        annotations:
          summary: "Excessive leader elections"
          description: "Node {{ $labels.node_id }} has {{ $value }} elections/5min"

      - alert: CommitIndexLag
        expr: max(prkdb_raft_commit_index) - min(prkdb_raft_commit_index) > 1000
        for: 5m
        labels:
          severity: warning
        annotations:
          summary: "Large commit index lag"
          description: "Lag is {{ $value }} entries"

      - alert: ServerDown
        expr: prkdb_up == 0
        for: 1m
        labels:
          severity: critical
        annotations:
          summary: "PrkDB server is down"
          description: "Node {{ $labels.node_id }} is offline"
```

## Customization

### Refresh Rate
Default: 5 seconds  
Change: Dashboard settings → Time options → Refresh

### Time Range
Default: Last 15 minutes  
Change: Top-right time picker

### Variables
Add template variables for dynamic filtering:
1. Dashboard settings → Variables → Add variable
2. Example: `node_id` query: `label_values(prkdb_raft_state, node_id)`
3. Update panel queries to use `$node_id`

## Troubleshooting

### No Data Showing
1. Check Prometheus is scraping: http://localhost:9091/targets
2. Verify metrics endpoint: `curl http://localhost:9090/metrics | grep prkdb`
3. Check data source connection in Grafana

### Incorrect Values
- Clear browser cache
- Verify query in Prometheus directly
- Check metric labels match dashboard queries

### Dashboard won't import
- Ensure Grafana version ≥8.0
- Check JSON file is valid
- Try manual panel creation

## Production Deployment

### Docker Compose

```yaml
version: '3.8'
services:
  prkdb:
    build: .
    ports:
      - "9090:9090"  # Metrics
      - "8080:8080"  # gRPC
    environment:
      - NODE_ID=1
      - CLUSTER_NODES=1@prkdb:50000

  prometheus:
    image: prom/prometheus
    ports:
      - "9091:9090"
    volumes:
      - ./prometheus.yml:/etc/prometheus/prometheus.yml
      - prometheus_data:/prometheus

  grafana:
    image: grafana/grafana
    ports:
      - "3000:3000"
    volumes:
      - ./docker/grafana/dashboards:/etc/grafana/provisioning/dashboards
      - grafana_data:/var/lib/grafana
    environment:
      - GF_DASHBOARDS_DEFAULT_HOME_DASHBOARD_PATH=/etc/grafana/provisioning/dashboards/prkdb_raft.json

volumes:
  prometheus_data:
  grafana_data:
```

## Support

For issues or questions:
- Check PrkDB documentation
- Review Prometheus query syntax
- Grafana panel types documentation

## License

Dashboard configuration is part of PrkDB project.
