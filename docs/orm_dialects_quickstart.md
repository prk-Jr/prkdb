# ORM dialect quickstart (SQLite vs Postgres)

PrkDB ORM ships fluent builders that are dialect-aware. Enable dialect features via `prkdb-orm` features (default: `sqlite`, optional: `postgres`, `mysql`).

## SQLite (default)
```rust
use prkdb_orm::dialect::SqlDialect;
use prkdb_orm::executor::SqlxSqliteExecutor;
use prkdb_orm::{Table, TableHelpers};
use prkdb_orm_macros::Table;
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize, Table, Clone, PartialEq, Eq)]
#[table(name = "people", primary_key = "id")]
struct Person {
    #[auto_increment]
    id: i64,
    name: String,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let pool = SqlxSqliteExecutor::connect_in_memory().await?;
    let exec = SqlxSqliteExecutor::new(&pool);
    sqlx::query(&Person::create_table_sql(SqlDialect::Sqlite))
        .execute(&pool)
        .await?;

    Person::insert()
        .insert_name("Ada")
        .exec(&exec, SqlDialect::Sqlite)
        .await?;

    let ada = Person::select()
        .where_name_eq("Ada")
        .fetch_one(&exec, SqlDialect::Sqlite)
        .await?;
    println!("Fetched via SQLite: {:?}", ada);
    Ok(())
}
```

## Postgres (feature = `postgres`)
```rust
use prkdb_orm::dialect::SqlDialect;
use prkdb_orm::executor::SqlxPostgresExecutor;
use prkdb_orm::{Relations, Table, TableHelpers};
use prkdb_orm_macros::Table;
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize, Table, Clone, PartialEq, Eq)]
#[table(name = "customers", primary_key = "id")]
struct Customer {
    #[auto_increment]
    id: i64,
    name: String,
}

#[derive(Debug, Serialize, Deserialize, Table, Clone, PartialEq, Eq)]
#[table(name = "orders", primary_key = "id")]
struct Order {
    #[auto_increment]
    id: i64,
    #[foreign(Customer::id)]
    customer_id: i64,
    total: f64,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let pool = SqlxPostgresExecutor::connect_default_env().await?;
    let exec = SqlxPostgresExecutor::new(&pool);

    for ddl in [
        Order::drop_table_sql(SqlDialect::Postgres),
        Customer::drop_table_sql(SqlDialect::Postgres),
        Customer::create_table_sql(SqlDialect::Postgres),
        Order::create_table_sql(SqlDialect::Postgres),
    ] {
        let _ = sqlx::query(&ddl).execute(&pool).await;
    }

    let customer_id = exec
        .insert_and_return_id(
            &Customer::insert_sql(SqlDialect::Postgres),
            &Customer {
                id: 0,
                name: "Alice".into(),
            }
            .insert_params(),
            Customer::pk_column(),
        )
        .await?;

    Order::insert()
        .insert_customer_id(customer_id)
        .insert_total(99.5)
        .exec(&exec, SqlDialect::Postgres)
        .await?;

    let sql = select_with_joins_sql::<Customer>(SqlDialect::Postgres);
    println!("Join-worthy SQL: {sql}");
    println!("Declared joins: {:?}", Customer::joins());
    Ok(())
}
```

## Tips
- Field helpers are generated per struct: `Table::insert()` exposes `.insert_<field>()` for non-`#[auto_increment]` columns, while `.update_*`/`.where_*` exist for all fields.
- JSON/nested fields use `#[column(json)]` and remain dialect-aware.
- Use `Sqlx*Executor` helpers for drivers, or implement your own executor to reuse the builder/SQL generation with another driver.
