# PrkDB Deployment Guide

This guide covers deploying PrkDB v2 in production environments.

## Architecture

PrkDB v2 uses a **shared-nothing, multi-leader** architecture (via Multi-Raft).

- **Nodes**: Independent processes with local storage.
- **Raft Groups**: Data is partitioned into shards, each managed by a Raft consensus group.
- **Discovery**: Nodes communicate via gRPC (default port 50051).

## Hardware Recommendations

| Component   | Minimum  | Recommended   | Note                                           |
| ----------- | -------- | ------------- | ---------------------------------------------- |
| **CPU**     | 2 cores  | 4+ cores      | Heavily threaded (gRPC + Raft)                 |
| **RAM**     | 4 GB     | 16 GB+        | OS page cache is critical for read performance |
| **Disk**    | NVMe SSD | NVMe SSD RAID | WAL fsync latency dominates write throughput   |
| **Network** | 1 Gbps   | 10 Gbps       | Low latency is crucial for Raft consensus      |

## Production Cluster (3-Node Setup)

A minimal high-availability cluster requires 3 nodes.

### 1. Binary Setup

Compile the server binary:

```bash
cargo build --release --bin prkdb-server
cp target/release/prkdb-server /usr/local/bin/
```

### 2. Systemd Service

Create `/etc/systemd/system/prkdb.service` on each node.

**Node 1 Configuration:**

```ini
[Unit]
Description=PrkDB Server
After=network.target

[Service]
Type=simple
User=prkdb
# Node ID 1, listening on 0.0.0.0:8081
ExecStart=/usr/local/bin/prkdb-server \
  --id 1 \
  --listen 0.0.0.0:8081 \
  --peers "1=10.0.0.1:8081,2=10.0.0.2:8081,3=10.0.0.3:8081" \
  --data-dir /var/lib/prkdb
Restart=always
LimitNOFILE=65536

[Install]
WantedBy=multi-user.target
```

**Node 2 Configuration:**
Update `--id 2` and `--listen 0.0.0.0:8081` (binds to local interface).

**Node 3 Configuration:**
Update `--id 3` and `--listen 0.0.0.0:8081`.

### 3. Start Cluster

Enable and start the service on all nodes:

```bash
sudo systemctl daemon-reload
sudo systemctl enable prkdb
sudo systemctl start prkdb
```

### 4. Verify Cluster

Use the CLI from any node to check health:

```bash
prkdb-cli --target http://127.0.0.1:8081 health
```

## Docker Deployment

PrkDB can be deployed using Docker.

```yaml
version: '3'
services:
  node1:
    image: prkdb/server:latest
    command: --id 1 --peers "1=node1:8081,2=node2:8081,3=node3:8081"
    volumes:
      - data1:/var/lib/prkdb
    ports:
      - '8081:8081'

  node2:
    image: prkdb/server:latest
    command: --id 2 --peers "1=node1:8081,2=node2:8081,3=node3:8081"
    volumes:
      - data2:/var/lib/prkdb

  node3:
    image: prkdb/server:latest
    command: --id 3 --peers "1=node1:8081,2=node2:8081,3=node3:8081"
    volumes:
      - data3:/var/lib/prkdb

volumes:
  data1:
  data2:
  data3:
```

## OS Tuning

For maximum performance, apply these sysctl settings:

```bash
# Increase max open files
fs.file-max = 1000000

# TCP optimizations for internal traffic
net.ipv4.tcp_keepalive_time = 60
net.ipv4.tcp_keepalive_intvl = 10
net.ipv4.tcp_keepalive_probes = 6
```

## Security (Coming Soon)

V2.1 will add native TLS support. For now, we verify deployment inside a private VPC or using a service mesh (Linkerd/Istio) for mTLS termination.
