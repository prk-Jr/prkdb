//! Integration test for the built-in sqlx executor (sqlite feature).

#![cfg(feature = "sqlite")]

use prkdb_orm::dialect::SqlDialect;
use prkdb_orm::executor::SqlxSqliteExecutor;
use prkdb_orm::schema::TableHelpers;
use prkdb_orm_macros::Table;
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize, Table, PartialEq, Eq)]
#[table(name = "customers", primary_key = "id")]
struct Customer {
    id: i64,
    name: String,
}

#[tokio::test]
async fn fetch_one_via_sqlx_sqlite_executor() {
    let pool = sqlx::sqlite::SqlitePoolOptions::new()
        .max_connections(1)
        .connect("sqlite::memory:")
        .await
        .unwrap();

    sqlx::query("CREATE TABLE customers (id INTEGER PRIMARY KEY, name TEXT)")
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query("INSERT INTO customers (id, name) VALUES (1, 'Alice')")
        .execute(&pool)
        .await
        .unwrap();

    let exec = SqlxSqliteExecutor::new(&pool);
    let customer = Customer::select()
        .where_id_eq(1)
        .fetch_one(&exec, SqlDialect::Sqlite)
        .await
        .unwrap();

    assert_eq!(
        customer,
        Customer {
            id: 1,
            name: "Alice".into()
        }
    );
}
