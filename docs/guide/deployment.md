# PrkDB Deployment Guide

This guide reflects the current `prkdb-server` binary and the current `prkdb-cli` verification workflow.

## Architecture

- Inter-node Raft traffic uses the addresses in `CLUSTER_NODES`.
- Client gRPC traffic is multiplexed on the same address and port as the local node entry in `CLUSTER_NODES`.
- `GRPC_PORT` is optional. When set, it must match the local node's `CLUSTER_NODES` port.
- Use `PRKDB_ADVERTISED_GRPC_ADDR` when the bind address differs from the dialable client address.
- Use `PRKDB_ADVERTISED_NODE_ADDRS` when peer nodes also need explicit dialable client addresses in metadata, for example `2=http://db-2.example.com:8081,3=http://db-3.example.com:8082`.
- Metrics bind to `127.0.0.1:(9090 + NODE_ID)` by default. Set `PRKDB_METRICS_ADDR` to override or `PRKDB_DISABLE_METRICS=1` to disable them.
- Schema registry data is persisted under `${STORAGE_PATH}/schemas`.

## Build

```bash
cargo build --release --bin prkdb-server --bin prkdb-cli
cp target/release/prkdb-server /usr/local/bin/
cp target/release/prkdb-cli /usr/local/bin/prkdb
```

## Example 3-Node Cluster

### Node addresses

- Node 1 address: `10.0.0.1:8080`
- Node 2 address: `10.0.0.2:8081`
- Node 3 address: `10.0.0.3:8082`

### Systemd unit

Create `/etc/systemd/system/prkdb.service` on each node.

```ini
[Unit]
Description=PrkDB Server
After=network.target

[Service]
Type=simple
User=prkdb
WorkingDirectory=/var/lib/prkdb
Environment=NODE_ID=1
Environment=CLUSTER_NODES=1@10.0.0.1:8080,2@10.0.0.2:8081,3@10.0.0.3:8082
Environment=STORAGE_PATH=/var/lib/prkdb/node1
Environment=PRKDB_ADMIN_TOKEN=change-me
Environment=PRKDB_ADVERTISED_GRPC_ADDR=http://db-1.example.com:8080
Environment=PRKDB_ADVERTISED_NODE_ADDRS=2=http://db-2.example.com:8081,3=http://db-3.example.com:8082
ExecStart=/usr/local/bin/prkdb-server
Restart=always
RestartSec=5
LimitNOFILE=65536

[Install]
WantedBy=multi-user.target
```

For node 2 and node 3, change:

- `NODE_ID`
- `STORAGE_PATH`
- the local address inside `CLUSTER_NODES`

## Start the Cluster

```bash
sudo systemctl daemon-reload
sudo systemctl enable prkdb
sudo systemctl start prkdb
```

## Verify the Deployment

### Check metrics

On node 1:

```bash
curl http://127.0.0.1:9091/metrics | grep prkdb_up
```

On node 2:

```bash
curl http://127.0.0.1:9092/metrics | grep prkdb_up
```

### Check the gRPC API

```bash
export PRKDB_ADMIN_TOKEN=change-me
prkdb --server http://127.0.0.1:8080 collection list
```

### Check schema registry persistence

```bash
export PRKDB_ADMIN_TOKEN=change-me
prkdb schema list --server http://127.0.0.1:8080
```

## Operational Notes

- `CLUSTER_NODES` should contain every node in the cluster, including the local node.
- Smart clients consume the addresses returned by metadata. Do not advertise `0.0.0.0`; set `PRKDB_ADVERTISED_GRPC_ADDR` if clients connect through DNS or a load balancer.
- If peer nodes have different bind and public addresses, configure `PRKDB_ADVERTISED_NODE_ADDRS` so metadata never falls back to an internal-only socket.
- `PRKDB_ADMIN_TOKEN` protects admin RPCs such as collection management and schema registration.
- If you expose the HTTP server from `prkdb-cli serve`, restrict CORS origins explicitly with `PRKDB_CORS_ORIGINS`.
- WebSocket auth is header-based. Set `PRKDB_WS_TOKEN` when you want bearer-token enforcement for `/ws/collections/:name`.

## Security Checklist

- Run the cluster behind TLS termination or a private network boundary.
- Keep `PRKDB_ADMIN_TOKEN` and `PRKDB_WS_TOKEN` out of shell history and process listings where possible.
- Persist `STORAGE_PATH` on durable local disks.
- Scrape metrics from the node-local metrics bind address instead of exposing it publicly.
