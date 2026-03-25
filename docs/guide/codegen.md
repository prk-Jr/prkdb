# Cross-Language SDK (Codegen)

Using PrkDB's centralized Schema Registry, you can automatically generate strongly-typed client libraries for other languages to safely interact with your database. This eliminates the need to manually keep API interfaces in sync between microservices.

## Supported Languages

PrkDB currently supports generating raw native types and API wrappers for:

- TypeScript
- Python
- Go

_(More languages are planned on the roadmap.)_

## Generating the SDK

Once you have defined your schema in Rust and registered it via the `prkdb schema register` command, you can generate client code for your other services.

Note: You must have a running PrkDB server to fetch the latest schemas.

```bash
# Output TypeScript definitions to ./frontend/src/api
prkdb codegen --lang typescript --out ./frontend/src/api --server http://localhost:8081

# Generate a Go SDK client wrapper
prkdb codegen --lang go --out ./backend/pkg/prkdb
```

To generate code for **all** supported languages at once:

```bash
prkdb codegen --lang all --out ./generated
```

## Generated Code Example

The code generator will create:

1. Native language representations of your Rust `#[derive(Collection)]` structs.
2. An automatically generated HTTP API client class to interact with PrkDB.

### TypeScript Example

```typescript
import { PrkDbClient } from './prkdb_client'
import { User } from './user'

async function main() {
  const db = new PrkDbClient('http://localhost:8081')

  // Type-safe put
  await db.user.put({
    id: '1001',
    name: 'Alice',
    age: 30,
  })

  // Type-safe get
  const user = await db.user.get('1001')
  console.log(user.name)
}
```

### Python Example

```python
from prkdb_client import PrkDbClient
from user import User

client = PrkDbClient(["http://localhost:8081"])

# Fetch dynamically typed objects
user: User = client.user.get("1001")
print(user.name)
```
