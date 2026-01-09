// Postgres demo for the fluent ORM API (requires `--features postgres`).
// Run with a Postgres instance available and DATABASE_URL set:
// `DATABASE_URL=postgres://user:pass@localhost/db cargo run -p prkdb-orm --example postgres_demo --features postgres`

#[cfg(not(feature = "postgres"))]
fn main() {
    eprintln!("Enable the `postgres` feature to run this example.");
}

#[cfg(feature = "postgres")]
mod pg_demo {
    use prkdb_orm::dialect::SqlDialect;
    use prkdb_orm::executor::SqlxPostgresExecutor;
    use prkdb_orm::schema::{Table, TableHelpers};
    use prkdb_orm::{QueryExecutor, Relations};
    use prkdb_orm_macros::Table;
    use serde::{Deserialize, Serialize};
    use tracing::info;

    #[derive(Debug, Serialize, Deserialize, Table, PartialEq, Clone)]
    #[table(name = "customers", primary_key = "id")]
    struct Customer {
        #[auto_increment]
        id: i64,
        name: String,
        email: String,
    }

    #[derive(Debug, Serialize, Deserialize, Table, PartialEq, Clone)]
    #[table(name = "orders", primary_key = "id")]
    struct Order {
        #[auto_increment]
        id: i64,
        #[foreign(Customer::id)]
        customer_id: i64,
        total: f64,
    }

    pub async fn run() -> Result<(), Box<dyn std::error::Error>> {
        tracing_subscriber::fmt()
            .with_target(false)
            .with_level(true)
            .init();

        let ddl_customers = Customer::create_table_sql(SqlDialect::Postgres);
        let ddl_orders = Order::create_table_sql(SqlDialect::Postgres);
        info!("DDL for customers:\n{ddl_customers}");
        info!("DDL for orders:\n{ddl_orders}");

        let pool = SqlxPostgresExecutor::connect_default_env().await?;
        let exec = SqlxPostgresExecutor::new(&pool);

        // Recreate tables for demo freshness
        for ddl in [
            Order::drop_table_sql(SqlDialect::Postgres),
            Customer::drop_table_sql(SqlDialect::Postgres),
        ] {
            let _ = sqlx::query(&ddl).execute(&pool).await;
        }
        for ddl in [
            Customer::create_table_sql(SqlDialect::Postgres),
            Order::create_table_sql(SqlDialect::Postgres),
        ] {
            sqlx::query(&ddl).execute(&pool).await?;
        }

        // Insert a customer + orders via generated SQL/params
        let customer = Customer {
            id: 0,
            name: "Alice".into(),
            email: "alice@example.com".into(),
        };
        let customer_id = exec
            .insert_and_return_id(
                &Customer::insert_sql(SqlDialect::Postgres),
                &customer.insert_params(),
                Customer::pk_column(),
            )
            .await?;
        info!("Inserted customer id={customer_id}");

        for total in [99.5, 42.0] {
            let order = Order {
                id: 0,
                customer_id,
                total,
            };
            let _ = exec
                .execute(
                    &Order::insert_sql(SqlDialect::Postgres),
                    &order.insert_params(),
                    SqlDialect::Postgres,
                )
                .await?;
        }

        // Fetch back
        let fetched = Customer::select()
            .where_id_eq(customer_id)
            .fetch_one(&exec, SqlDialect::Postgres)
            .await?;
        info!("Fetched customer: {:?}", fetched);

        // Show inferred relations
        info!("Customer joins: {:?}", Customer::joins());
        info!("Order FKs: {:?}", Order::foreign_keys());

        Ok(())
    }
}

#[cfg(feature = "postgres")]
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    pg_demo::run().await
}
