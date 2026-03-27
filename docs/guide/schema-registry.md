# Schema Registry

PrkDB includes a built-in schema registry that ensures data consistency across languages and versions. Instead of generic unstructured JSON or raw bytes, PrkDB collections are strictly typed.

## Defining Collections

In Rust, you define collections using the `#[derive(Collection)]` macro. This automatically generates a Protocol Buffers (`FileDescriptorSet`) schema for your struct.

```rust
use prkdb_macros::Collection;
use serde::{Deserialize, Serialize};

#[derive(Collection, Serialize, Deserialize, Clone, Debug)]
pub struct Product {
    #[key]
    pub sku: String,
    pub name: String,
    pub price: f64,
    pub inventory: u32,
}
```

## Schema Registration

Before a client can interact with a collection, the schema must be registered with the server. You can register schemas via the `prkdb` CLI tool.

### Registering via CLI

`register` and `list` are admin operations, so set `PRKDB_ADMIN_TOKEN` first:

```bash
export PRKDB_ADMIN_TOKEN=change-me
prkdb schema register --server http://127.0.0.1:8080 --collection Product --proto ./schemas/product.bin
```

To view all registered schemas:

```bash
prkdb schema list --server http://127.0.0.1:8080
```

## Compatibility Modes

When you evolve your schemas (e.g., adding or removing fields), PrkDB validates the new schema against the existing one based on the collection's compatibility mode:

- **Backward (Default)**: New schema can read data written by old schema. (e.g. adding an optional field)
- **Forward**: Old schema can read data written by new schema.
- **Full**: Schema changes must be both backward and forward compatible.
- **None**: No compatibility checks are performed.

You can specify the compatibility mode during registration:

```bash
prkdb schema register --server http://127.0.0.1:8080 --collection Product --proto ./schemas/product_v2.bin --compatibility backward
```

If a change is incompatible, the registry will reject the update unless you provide a `--migration-id`.

When you are using the development HTTP server (`prkdb-cli serve`), the schema-registry gRPC endpoint remains `http://127.0.0.1:50051`.
