use prkdb_orm::dialect::SqlDialect;
use prkdb_orm::schema::{select_with_joins_sql, Relations};
use prkdb_orm_macros::Table;
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize, Table)]
#[table(name = "address", primary_key = "id")]
struct Address {
    id: i64,
    #[foreign(Person::id)]
    person_id: i64,
    street: String,
    city: String,
    zip_code: String,
}

#[derive(Debug, Serialize, Deserialize, Table)]
#[table(name = "people", primary_key = "id")]
struct Person {
    id: i64,
    name: String,
    // Automatically inferred join to Address by Vec<Address>
    addresses: Vec<Address>,
}

fn main() {
    println!("FKs: {:?}", Address::foreign_keys());
    println!("Joins: {:?}", Person::joins());
    println!(
        "Join SQL (Postgres): {}",
        select_with_joins_sql::<Person>(SqlDialect::Postgres)
    );
}
