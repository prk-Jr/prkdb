# PrkDB Deployment Guide

## Requirements

- **Rust**: 1.70+
- **Docker**: For monitoring stack (optional)
- **Memory**: 4GB+ recommended for high throughput
- **Disk**: SSD recommended for WAL performance

## Single Node Deployment

### Build

```bash
cargo build --release
```

### Configuration

```rust
let db = PrkDb::builder()
    .with_data_dir("/var/lib/prkdb/data")
    .with_optimized_storage("/var/lib/prkdb", OptimizationLevel::Legendary)
    .register_collection::<MyCollection>()
    .build()?;
```

### Systemd Service

Create `/etc/systemd/system/prkdb.service`:

```ini
[Unit]
Description=PrkDB Database
After=network.target

[Service]
Type=simple
User=prkdb
ExecStart=/usr/local/bin/prkdb serve --data-dir /var/lib/prkdb
Restart=always
RestartSec=5

[Install]
WantedBy=multi-user.target
```

```bash
sudo systemctl enable prkdb
sudo systemctl start prkdb
```

## Cluster Deployment

### 3-Node Minimum

For fault tolerance, deploy at least 3 nodes (tolerates 1 failure).

### Node Configuration

**Node 1** (`node1.example.com`):
```bash
./raft_node \
  --node-id 1 \
  --listen 0.0.0.0:50051 \
  --peers 2=node2.example.com:50051,3=node3.example.com:50051 \
  --data-dir /var/lib/prkdb
```

**Node 2** (`node2.example.com`):
```bash
./raft_node \
  --node-id 2 \
  --listen 0.0.0.0:50051 \
  --peers 1=node1.example.com:50051,3=node3.example.com:50051 \
  --data-dir /var/lib/prkdb
```

**Node 3** (`node3.example.com`):
```bash
./raft_node \
  --node-id 3 \
  --listen 0.0.0.0:50051 \
  --peers 1=node1.example.com:50051,2=node2.example.com:50051 \
  --data-dir /var/lib/prkdb
```

### Docker Compose

```yaml
version: '3.8'
services:
  prkdb-node1:
    image: prkdb:latest
    command: >
      --node-id 1
      --listen 0.0.0.0:50051
      --peers 2=prkdb-node2:50051,3=prkdb-node3:50051
    volumes:
      - prkdb-data1:/data
    ports:
      - "50051:50051"

  prkdb-node2:
    image: prkdb:latest
    command: >
      --node-id 2
      --listen 0.0.0.0:50051
      --peers 1=prkdb-node1:50051,3=prkdb-node3:50051
    volumes:
      - prkdb-data2:/data

  prkdb-node3:
    image: prkdb:latest
    command: >
      --node-id 3
      --listen 0.0.0.0:50051
      --peers 1=prkdb-node1:50051,2=prkdb-node2:50051
    volumes:
      - prkdb-data3:/data

volumes:
  prkdb-data1:
  prkdb-data2:
  prkdb-data3:
```

## Monitoring

### Prometheus + Grafana

```bash
docker compose -f docker/docker-compose.yml up -d
```

### Metrics Endpoint

Each node exposes metrics at `:8092/metrics`:

```
prkdb_reads_total
prkdb_writes_total
prkdb_cache_hits_total
prkdb_collection_ops_total{collection="users"}
prkdb_operation_duration_seconds
```

### Alerting

Example Prometheus alert rules:

```yaml
groups:
  - name: prkdb
    rules:
      - alert: HighLatency
        expr: histogram_quantile(0.99, prkdb_operation_duration_seconds) > 0.1
        for: 5m
        labels:
          severity: warning
      
      - alert: LowThroughput
        expr: rate(prkdb_writes_total[5m]) < 1000
        for: 10m
        labels:
          severity: warning
```

## Performance Tuning

### WAL Configuration

```rust
let config = WalConfig {
    segment_bytes: 512 * 1024 * 1024,  // 512MB segments
    batch_size: 500,                    // Optimal batch size
    ..WalConfig::benchmark_config()
};
```

### Memory

- Increase file descriptor limits: `ulimit -n 65536`
- Use huge pages if available
- Allocate sufficient heap for caching

### Network

- Use private network between cluster nodes
- Enable TCP keepalive
- Consider dedicated NICs for replication traffic

## Backup & Recovery

### Snapshot (TODO)

```bash
prkdb snapshot create --output /backup/snapshot.db
prkdb snapshot restore --input /backup/snapshot.db
```

### WAL Backup

Copy WAL directory while node is stopped:

```bash
systemctl stop prkdb
rsync -av /var/lib/prkdb/ /backup/prkdb/
systemctl start prkdb
```

## Security

### TLS (TODO)

```bash
./raft_node \
  --tls-cert /etc/prkdb/server.crt \
  --tls-key /etc/prkdb/server.key
```

### Firewall

```bash
# Allow only cluster nodes
ufw allow from node1.example.com to any port 50051
ufw allow from node2.example.com to any port 50051
ufw allow from node3.example.com to any port 50051
```

## Troubleshooting

### Node Won't Start

1. Check logs: `journalctl -u prkdb -f`
2. Verify data directory permissions
3. Check port availability: `lsof -i :50051`

### Cluster Not Electing Leader

1. Verify network connectivity between nodes
2. Check firewall rules
3. Ensure all nodes have same peer configuration
4. Check for clock skew

### High Latency

1. Check disk I/O: `iostat -x 1`
2. Verify SSD usage for WAL
3. Monitor memory usage
4. Check network latency between nodes
