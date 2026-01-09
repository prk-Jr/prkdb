use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use prkdb_orm::builders::QueryExecutor;
use prkdb_orm::dialect::SqlDialect;
use prkdb_orm::schema::{Row, TableHelpers};
use prkdb_orm::types::SqlValue;
use prkdb_orm_macros::Table;
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize, Table, PartialEq, Eq)]
#[table(name = "customers", primary_key = "id")]
struct Customer {
    id: i64,
    name: String,
}

type CapturedParams = Vec<(String, Vec<SqlValue>)>;

#[derive(Clone)]
struct MockExecutor {
    rows: Vec<Row>,
    captured: Arc<Mutex<CapturedParams>>,
    rows_affected: u64,
}

#[async_trait]
impl QueryExecutor for MockExecutor {
    type Error = String;

    async fn query(
        &self,
        sql: &str,
        params: &[SqlValue],
        _dialect: SqlDialect,
    ) -> Result<Vec<Row>, Self::Error> {
        self.captured
            .lock()
            .unwrap()
            .push((sql.to_string(), params.to_vec()));
        Ok(self.rows.clone())
    }

    async fn execute(
        &self,
        sql: &str,
        params: &[SqlValue],
        _dialect: SqlDialect,
    ) -> Result<u64, Self::Error> {
        self.captured
            .lock()
            .unwrap()
            .push((sql.to_string(), params.to_vec()));
        Ok(self.rows_affected)
    }
}

fn mock_row(id: i64, name: &str) -> Row {
    let mut row = HashMap::new();
    row.insert("id".to_string(), SqlValue::Int(id));
    row.insert("name".to_string(), SqlValue::Text(name.to_string()));
    row
}

#[tokio::test]
async fn fetch_one_decodes_row() {
    let rows = vec![mock_row(1, "Alice")];
    let exec = MockExecutor {
        rows,
        captured: Arc::new(Mutex::new(Vec::new())),
        rows_affected: 1,
    };

    let customer = Customer::select()
        .where_id_eq(1)
        .fetch_one(&exec, SqlDialect::Postgres)
        .await
        .expect("fetch_one should decode row");

    assert_eq!(
        customer,
        Customer {
            id: 1,
            name: "Alice".to_string()
        }
    );

    let captured = exec.captured.lock().unwrap();
    assert_eq!(captured.len(), 1);
    let (sql, params) = &captured[0];
    assert!(
        sql.contains("SELECT id, name FROM customers WHERE id = ?"),
        "unexpected SQL: {sql}"
    );
    assert_eq!(params.len(), 1);
}

#[tokio::test]
async fn fetch_optional_handles_empty() {
    let exec = MockExecutor {
        rows: Vec::new(),
        captured: Arc::new(Mutex::new(Vec::new())),
        rows_affected: 0,
    };

    let result = Customer::select()
        .where_id_eq(42)
        .fetch_optional(&exec, SqlDialect::Sqlite)
        .await
        .expect("fetch_optional should not error on empty set");

    assert!(result.is_none());
}

#[tokio::test]
async fn insert_update_delete_executes() {
    let captured: Arc<Mutex<CapturedParams>> = Arc::new(Mutex::new(Vec::new()));
    let exec = MockExecutor {
        rows: Vec::new(),
        captured: captured.clone(),
        rows_affected: 3,
    };

    let inserted = Customer::insert()
        .insert_id(1)
        .insert_name("Alice")
        .exec(&exec, SqlDialect::Sqlite)
        .await
        .expect("insert exec");
    assert_eq!(inserted, 3);

    let updated = Customer::update()
        .update_name_with_value("Bob")
        .where_id_eq(1)
        .exec(&exec, SqlDialect::Sqlite)
        .await
        .expect("update exec");
    assert_eq!(updated, 3);

    let deleted = Customer::delete()
        .where_id_eq(1)
        .exec(&exec, SqlDialect::Sqlite)
        .await
        .expect("delete exec");
    assert_eq!(deleted, 3);

    let calls = captured.lock().unwrap();
    assert!(
        calls
            .iter()
            .any(|(sql, _)| sql.contains("INSERT INTO customers")),
        "expected insert call, got: {:?}",
        calls
    );
    assert!(
        calls
            .iter()
            .any(|(sql, _)| sql.contains("UPDATE customers SET name = ? WHERE id = ?")),
        "expected update call, got: {:?}",
        calls
    );
    assert!(
        calls
            .iter()
            .any(|(sql, _)| sql.contains("DELETE FROM customers WHERE id = ?")),
        "expected delete call, got: {:?}",
        calls
    );
}
