# Cross-Language SDK Code Generation

PrkDB can generate TypeScript, Python, and Go client code from schemas stored in the schema registry.

## Requirements

- A running PrkDB gRPC endpoint
- Registered schemas in the schema registry
- `PRKDB_ADMIN_TOKEN` if the server protects schema listing

## Generate Code

```bash
export PRKDB_ADMIN_TOKEN=change-me

# TypeScript
prkdb codegen --server http://127.0.0.1:8080 --lang typescript --out ./generated/ts

# Python
prkdb codegen --server http://127.0.0.1:8080 --lang python --out ./generated/python

# Go
prkdb codegen --server http://127.0.0.1:8080 --lang go --out ./generated/go

# All supported languages
prkdb codegen --server http://127.0.0.1:8080 --lang all --out ./generated
```

To generate code for one collection only:

```bash
prkdb codegen \
  --server http://127.0.0.1:8080 \
  --lang typescript \
  --collection users \
  --out ./generated/ts
```

If you are pointing codegen at the local gRPC endpoint created by `prkdb-cli serve`, use `http://127.0.0.1:50051` instead.

## Generated Client Shape

The generated HTTP client libraries target the `prkdb-cli serve` HTTP API, which defaults to `http://127.0.0.1:8080`.

### TypeScript

```typescript
import { PrkDbClient } from './prkdb_client'
import type { User } from './user'

async function main() {
  const db = new PrkDbClient('http://127.0.0.1:8080')

  await db.put('users', {
    id: '1001',
    name: 'Alice',
    age: 30,
  })

  const user = await db.get<User>('users', '1001')
  const users = await db.list<User>('users', { filter: 'id=1001' })

  console.log(user)
  console.log(users.length)
}
```

### Python

```python
import asyncio
from generated.python.prkdb_client import PrkDbClient

async def main() -> None:
    async with PrkDbClient("http://127.0.0.1:8080") as client:
        await client.put("users", {
            "id": "1001",
            "name": "Alice",
            "age": 30,
        })

        user = await client.get("users", "1001")
        users = await client.list("users", filter="id=1001")

        print(user)
        print(len(users))

asyncio.run(main())
```

### Go

```go
package main

import (
    "fmt"
    "log"
    "your_project/models"
)

func main() {
    client := models.NewPrkDbClient("http://127.0.0.1:8080")

    results, err := client.ListRaw("users", models.ListOptions{
        Filter: "id=1001",
    })
    if err != nil {
        log.Fatal(err)
    }

    fmt.Println(results)
}
```

## Notes

- Schema registration and schema listing are admin operations.
- Generated TypeScript and Python clients support `get(collection, id)` against `GET /collections/:name/data/:id`.
- Code generation does not create collection-specific `db.users.put(...)` wrappers today; the generated clients expose `list`, `get`, `put`, and `delete`.
