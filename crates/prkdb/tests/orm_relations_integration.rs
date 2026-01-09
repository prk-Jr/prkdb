use prkdb_orm::dialect::SqlDialect;
use prkdb_orm::schema::{select_with_joins_sql, Relations};
use prkdb_orm_macros::Table;
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize, Table, Clone)]
#[table(name = "customers", primary_key = "id")]
struct Customer {
    id: i64,
    name: String,
    // Inferred join from Vec<Order>
    orders: Vec<Order>,
}

#[derive(Debug, Serialize, Deserialize, Table, Clone)]
#[table(name = "orders", primary_key = "id")]
struct Order {
    id: i64,
    #[foreign(Customer::id)]
    customer_id: i64,
    total: f64,
}

#[test]
fn integration_relations_metadata() {
    assert_eq!(Order::foreign_keys(), &[("customer_id", "Customer::id")]);
    assert_eq!(
        Customer::joins(),
        &[("orders", "orders", "customers.id = orders.customer_id")]
    );

    let join_sql = select_with_joins_sql::<Customer>(SqlDialect::Postgres);
    assert!(
        join_sql.contains("INNER JOIN \"orders\" ON customers.id = orders.customer_id"),
        "generated SQL should include the inferred join"
    );
}
