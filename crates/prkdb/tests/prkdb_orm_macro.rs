use chrono::{DateTime, NaiveDateTime, Utc};
use prkdb_orm::{dialect::SqlDialect, types::SqlValue, Table, TableHelpers};
use prkdb_orm_macros::Table;

#[derive(Debug, Clone, PartialEq, Table)]
#[table(name = "orders", primary_key = "id")]
pub struct Order {
    pub id: i64,
    pub customer_id: i64,
    #[column(sql_type = "JSONB")]
    pub metadata: String,
    pub created_at: NaiveDateTime,
    pub processed_at: DateTime<Utc>,
}

fn sample_order() -> Order {
    let created_at = DateTime::<Utc>::from_timestamp(1_650_000_000, 0)
        .expect("valid timestamp")
        .naive_utc();
    let processed_at = DateTime::<Utc>::from_timestamp(1_650_100_000, 0).expect("valid timestamp");

    Order {
        id: 100,
        customer_id: 50,
        metadata: r#"{"key":"value"}"#.to_string(),
        created_at,
        processed_at,
    }
}

#[test]
fn basic_test() {
    let table_name = Order::table_name();
    assert_eq!(table_name, "orders");
}

#[test]
fn ddl_respects_column_override_and_datetime_mapping() {
    let sql_pg = Order::create_table_sql(SqlDialect::Postgres);
    assert!(sql_pg.contains("JSONB"), "Expected override to be used");
    assert!(
        sql_pg.to_uppercase().contains("TIMESTAMP"),
        "Expected NaiveDateTime mapped"
    );

    let sql_sqlite = Order::create_table_sql(SqlDialect::Sqlite);
    assert!(sql_sqlite.contains("TEXT") || sql_sqlite.contains("DATETIME"));
}

#[test]
fn insert_params_for_order_are_in_struct_field_order() {
    let o = sample_order();
    let params = o.insert_params();
    assert_eq!(params.len(), 5);
    assert_eq!(params[0], SqlValue::Int(100));
    assert_eq!(params[1], SqlValue::Int(50));
    assert_eq!(params[2], SqlValue::Text(r#"{"key":"value"}"#.to_string()));
    // Datetime values should convert to Text or Int depending on SqlValue implementation
}

#[test]
fn update_with_datetime_and_override_field() {
    let (sql, params) = Order::update()
        .update_metadata_with_value(r#"{"updated":true}"#.to_string())
        .update_processed_at_with_value(sample_order().processed_at)
        .where_id_eq(100)
        .to_sql();

    assert!(sql.contains("UPDATE"));
    assert!(sql.contains("metadata"));
    assert!(sql.contains("processed_at"));
    assert_eq!(params.len(), 3);
}

#[test]
fn select_where_like_gt_and_between() {
    let (sql, params) = Order::select()
        .where_metadata_like("%value%".to_string())
        .where_id_gt(10)
        .where_customer_id_between(1, 1000)
        .to_sql();

    dbg!(&sql, &params);

    assert!(sql.contains("LIKE"));
    assert!(sql.contains(">"));
    assert!(params.len() == 4);
}

#[test]
fn delete_with_datetime_condition() {
    let (sql, params) = Order::delete()
        .where_created_at_eq(sample_order().created_at)
        .to_sql();

    assert!(sql.starts_with("DELETE"));
    assert_eq!(params.len(), 1);
}

#[test]
fn dialect_affects_json_mapping_when_no_override() {
    #[derive(Debug, Clone, PartialEq, Table)]
    #[table(name = "events", primary_key = "id")]
    pub struct Event {
        pub id: i64,
        pub payload: serde_json::Value,
    }

    let sql_pg = Event::create_table_sql(SqlDialect::Postgres);
    assert!(
        sql_pg.contains("JSON"),
        "Postgres should map serde_json::Value to JSON"
    );

    let sql_sqlite = Event::create_table_sql(SqlDialect::Sqlite);
    assert!(
        sql_sqlite.contains("TEXT"),
        "SQLite should map serde_json::Value to TEXT"
    );
}
