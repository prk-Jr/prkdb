# PrkDB Docker Deployment

Quick start guide for running PrkDB in Docker with full observability stack.

## Quick Start

```bash
# 1. Build and start all services
docker-compose up -d

# 2. View logs
docker-compose logs -f

# 3. Check cluster health
./docker/scripts/health-check.sh

# 4. Test cluster operations
./docker/scripts/test-cluster.sh

# 5. Access Grafana dashboard
open http://localhost:3000  # Login: admin/admin
```

## Architecture

The deployment includes:
- **3 PrkDB nodes** - Separate single-node Raft clusters (node1, node2, node3)
- **Prometheus** - Metrics collection (port 9093)
- **Grafana** - Dashboards and visualization (port 3000)

Note: Current setup runs 3 independent single-node clusters. For a multi-node Raft cluster, update `CLUSTER_NODES` in docker-compose.yml.

## Services & Ports

| Service | gRPC API | Raft RPC | Metrics | UI |
|---------|----------|----------|---------|-----|
| Node 1 | 8081 | 50001 | - | - |
| Node 2 | 8082 | 50002 | - | - |
| Node 3 | 8083 | 50003 | - | - |
| Prometheus | - | - | - | 9093 |
| Grafana | - | - | - | 3000 |

## Configuration Files

### Core Files
- `Dockerfile` - Multi-stage build for optimized image
- `docker-compose.yml` - Service orchestration
- `.env.example` - Environment variables template

### Prometheus
- `docker/prometheus/prometheus.yml` - Scrape configuration

### Grafana
- `docker/grafana/provisioning/datasources/prometheus.yml` - Auto-config datasource
- `docker/grafana/provisioning/dashboards/default.yml` - Auto-load dashboards
- `docker/grafana/dashboards/prkdb_raft.json` - Raft monitoring dashboard

### Scripts
- `docker/scripts/health-check.sh` - Cluster health verification
- `docker/scripts/test-cluster.sh` - Basic operation testing

## Common Commands

### Build & Start
```bash
# Build images
docker-compose build

# Start in background
docker-compose up -d

# Start with logs
docker-compose up

# Start specific service
docker-compose up -d node1
```

### Monitoring
```bash
# View all logs
docker-compose logs -f

# View specific service logs
docker-compose logs -f node1

# Check service status
docker-compose ps

# View resource usage
docker stats
```

### Testing
```bash
# Run health check
./docker/scripts/health-check.sh

# Run operation tests
./docker/scripts/test-cluster.sh

# Manual gRPC test (requires grpcurl)
grpcurl -plaintext -d '{"key":"dGVzdA==","value":"dmFsdWU="}' \
  localhost:8081 prkdb.PrkDbService.Put
```

### Cleanup
```bash
# Stop services
docker-compose down

# Stop and remove volumes (data loss!)
docker-compose down -v

# Remove images
docker-compose down --rmi all
```

## Converting to Multi-Node Cluster

To run a true 3-node Raft cluster (instead of 3 separate clusters):

1. Update `docker-compose.yml` environment for each node:
```yaml
# All nodes should have the same CLUSTER_NODES value
CLUSTER_NODES: "1@node1:50000,2@node2:50000,3@node3:50000"
```

2. Ensure all nodes have the SAME `NUM_PARTITIONS` value

3. Restart cluster:
```bash
docker-compose down -v  # Remove old data
docker-compose up -d
```

## Grafana Dashboard

### Access
1. Open http://localhost:3000
2. Login with `admin` / `admin`
3. Dashboard auto-loads: "PrkDB Raft Monitoring"

### Features
- Raft state visualization (Leader/Follower/Candidate)
- Leader election frequency
- Commit index progression
- Heartbeat success/failure rates
- Server health status

### Manual Import (if needed)
1. Navigate to: + → Import
2. Upload: `docker/grafana/dashboards/prkdb_raft.json`
3. Select "Prometheus" datasource
4. Click Import

## Prometheus

### Access
- UI: http://localhost:9093
- Targets: http://localhost:9093/targets
- Metrics: http://localhost:9093/graph

### Query Examples
```promql
# Current Raft state
prkdb_raft_state

# Election rate (per 5 mins)
rate(prkdb_raft_leader_elections_total[5m])

# Heartbeat success rate
sum(rate(prkdb_raft_heartbeats_sent_total[1m])) 
/ 
(sum(rate(prkdb_raft_heartbeats_sent_total[1m])) + sum(rate(prkdb_raft_heartbeats_failed_total[1m])))

# Commit index lag
max(prkdb_raft_commit_index) - min(prkdb_raft_commit_index)
```

## Troubleshooting

### Containers won't start
```bash
# Check logs
docker-compose logs

# Check disk space
df -h

# Remove old containers
docker-compose down -v
docker system prune -a
```

### Metrics not showing in Grafana
1. Check Prometheus targets: http://localhost:9093/targets
2. Verify data source in Grafana: Configuration → Data Sources
3. Test query in Prometheus directly
4. Check firewall/network settings

### Build fails
```bash
# Clear build cache
docker-compose build --no-cache

# Check Rust installation in container
docker-compose run node1 prkdb-server --version
```

### grpcurl not found (for testing)
```bash
# macOS
brew install grpcurl

# Linux
apt-get install grpcurl

# Or use Docker
docker run fullstorydev/grpcurl -plaintext host.docker.internal:8081 list
```

## Production Deployment

### Security Checklist
- [ ] Change Grafana admin password
- [ ] Enable HTTPS/TLS for gRPC
- [ ] Configure firewall rules
- [ ] Set resource limits in docker-compose.yml
- [ ] Configure persistent volume backups
- [ ] Enable Prometheus authentication
- [ ] Review log levels (change from `info` to `warn`)

### Resource Limits Example
```yaml
services:
  node1:
    deploy:
      resources:
        limits:
          cpus: '2'
          memory: 4G
        reservations:
          cpus: '1'
          memory: 2G
```

### Backup Volumes
```bash
# Backup node data
docker run --rm -v prkdb_node1-data:/data -v $(pwd):/backup \
  alpine tar czf /backup/node1-backup.tar.gz /data

# Restore
docker run --rm -v prkdb_node1-data:/data -v $(pwd):/backup \
  alpine tar xzf /backup/node1-backup.tar.gz -C /
```

## Development Tips

### Hot Reload
Changes to Rust code require rebuild:
```bash
docker-compose build
docker-compose up -d
```

### Debug Mode
```yaml
environment:
  - RUST_LOG=debug  # or trace
  - RUST_BACKTRACE=1
```

### Attach to Container
```bash
docker exec -it prkdb-node1 /bin/bash
```

## Additional Resources

- [Grafana Dashboard README](docker/grafana/README.md)
- [Prometheus Configuration](https://prometheus.io/docs/prometheus/latest/configuration/configuration/)
- [Docker Compose Reference](https://docs.docker.com/compose/compose-file/)

## Support

For issues:
1. Check logs: `docker-compose logs`
2. Run health check: `./docker/scripts/health-check.sh`
3. Verify Prometheus targets: http://localhost:9093/targets
4. Check Grafana datasource connection
