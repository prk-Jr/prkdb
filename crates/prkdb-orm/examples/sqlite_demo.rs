//! SQLite demo with normalized tables (persons, addresses, profiles) using the built-in sqlx executor.
//! - Shows generated DDL per table
//! - Inserts parent rows, child rows with FK, and a 1:1 profile
//! - Fetches data back as strongly typed structs

use prkdb_orm::dialect::SqlDialect;
use prkdb_orm::executor::SqlxSqliteExecutor;
use prkdb_orm::schema::{Table, TableHelpers};
use prkdb_orm_macros::Table;
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize, Table, PartialEq, Clone)]
#[table(name = "people", primary_key = "id")]
struct Person {
    #[auto_increment]
    id: i64,
    name: String,
    age: Option<u8>,
}

#[derive(Debug, Serialize, Deserialize, Table, PartialEq, Clone)]
#[table(name = "addresses", primary_key = "id")]
struct Address {
    #[auto_increment]
    id: i64,
    #[foreign(Person::id)]
    person_id: i64,
    street: String,
    city: String,
    zip_code: String,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Clone)]
enum Preference {
    Weekly,
    Monthly,
}

#[derive(Debug, Serialize, Deserialize, Table, PartialEq, Clone)]
#[table(name = "profiles", primary_key = "id")]
struct Profile {
    #[auto_increment]
    id: i64,
    #[foreign(Person::id)]
    person_id: i64,
    bio: String,
    #[column(json)]
    newsletter: Preference,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_target(false)
        .with_level(true)
        .init();

    let db_url = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "sqlite:///tmp/prkdb_orm_sqlite_demo.db".to_string());

    let pool = sqlx::sqlite::SqlitePoolOptions::new()
        .max_connections(1)
        .connect(&db_url)
        .await?;
    let exec = SqlxSqliteExecutor::new(&pool);

    // Recreate tables for a clean run
    for ddl in [
        Profile::drop_table_sql(SqlDialect::Sqlite),
        Address::drop_table_sql(SqlDialect::Sqlite),
        Person::drop_table_sql(SqlDialect::Sqlite),
    ] {
        let _ = sqlx::query(&ddl).execute(&pool).await;
    }
    for ddl in [
        Person::create_table_sql(SqlDialect::Sqlite),
        Address::create_table_sql(SqlDialect::Sqlite),
        Profile::create_table_sql(SqlDialect::Sqlite),
    ] {
        sqlx::query(&ddl).execute(&pool).await?;
    }

    // Insert parent
    let person = Person {
        id: 0,
        name: "Ada".into(),
        age: Some(8),
    };
    let person_id = exec
        .insert_and_return_id(
            &Person::insert_sql(SqlDialect::Sqlite),
            &person.insert_params(),
        )
        .await?;
    println!("Inserted person id={}", person_id);

    // Insert addresses
    let addr1 = Address {
        id: 0,
        person_id,
        street: "123 Hex Rd".into(),
        city: "Rustville".into(),
        zip_code: "99999".into(),
    };
    let addr2 = Address {
        id: 0,
        person_id,
        street: "456 Binary Ave".into(),
        city: "Enum City".into(),
        zip_code: "11111".into(),
    };
    for addr in [addr1, addr2] {
        exec.insert_and_return_id(
            &Address::insert_sql(SqlDialect::Sqlite),
            &addr.insert_params(),
        )
        .await?;
    }

    // Insert profile (1:1)
    let profile = Profile {
        id: 0,
        person_id,
        bio: "Loves normalized schemas".into(),
        newsletter: Preference::Weekly,
    };
    exec.insert_and_return_id(
        &Profile::insert_sql(SqlDialect::Sqlite),
        &profile.insert_params(),
    )
    .await?;

    // Fetch person
    let fetched_person = Person::select()
        .where_id_eq(person_id)
        .fetch_one(&exec, SqlDialect::Sqlite)
        .await?;

    // Fetch children
    let fetched_addresses = Address::select()
        .where_person_id_eq(person_id)
        .fetch_all(&exec, SqlDialect::Sqlite)
        .await?;
    let fetched_profile = Profile::select()
        .where_person_id_eq(person_id)
        .fetch_one(&exec, SqlDialect::Sqlite)
        .await?;

    println!("Person: {:?}", fetched_person);
    println!("Addresses: {:?}", fetched_addresses);
    println!("Profile: {:?}", fetched_profile);

    Ok(())
}
