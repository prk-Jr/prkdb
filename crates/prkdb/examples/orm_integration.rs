use prkdb::prelude::*;
use prkdb_orm::{Table as TableTrait, TableHelpers};
use prkdb_orm_macros::Table;
use serde::{Deserialize, Serialize}; // Import TableHelpers for select()

#[derive(Collection, Table, Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
#[table(name = "users")] // Try inferring PK from #[id]
struct User {
    #[id]
    id: u64,
    username: String,
    email: String,
    active: bool,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // 1. Initialize DB with SQL storage (using InMemory for now, but simulating SQL generation)
    // Note: In a real scenario, we'd use SqliteAdapter or similar to actually execute SQL.
    // For this example, we just want to verify the Table trait is implemented and SQL can be generated.

    let db = PrkDb::builder()
        .with_storage(prkdb::InMemoryAdapter::new())
        .register_collection::<User>()
        .build()?;

    // 2. Verify SQL Generation
    let create_sql = User::create_table_sql(prkdb_orm::dialect::SqlDialect::Sqlite);
    println!("Generated Create SQL: {}", create_sql);

    assert!(create_sql.contains("CREATE TABLE"));
    // Sqlite dialect quotes identifiers
    assert!(create_sql.contains("\"active\" BOOLEAN"));

    let insert_sql = User::insert_sql(prkdb_orm::dialect::SqlDialect::Sqlite);
    println!("Generated Insert SQL: {}", insert_sql);
    assert!(insert_sql.contains("INSERT INTO"));

    // 3. Verify Collection Operations still work
    let users = db.collection::<User>();
    let user = User {
        id: 1,
        username: "alice".to_string(),
        email: "alice@example.com".to_string(),
        active: true,
    };

    users.put(user.clone()).await?;
    let fetched = users.get(&1).await?;
    assert_eq!(fetched, Some(user));
    println!("Successfully put and fetched user via Collection API");

    // 4. Verify Fluent Query Builder
    // Note: This just generates SQL, it doesn't execute against the InMemoryAdapter (which is KV-store based).
    // But it proves the ORM layer is generating correct SQL for the type.
    let select_query = User::select()
        .where_username_eq("alice")
        .where_active_eq(true) // Chained where clauses usually imply AND
        .order_by_id(false) // DESC
        .to_sql();

    println!("Generated Select Query: {}", select_query.0);
    assert!(select_query.0.contains("SELECT"));
    assert!(select_query.0.contains("WHERE"));
    assert!(select_query.0.contains("username = ?"));

    println!("ORM Integration Verified Successfully!");

    Ok(())
}
