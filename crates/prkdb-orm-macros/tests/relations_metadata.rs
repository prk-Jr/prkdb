use prkdb_orm::dialect::SqlDialect;
use prkdb_orm::schema::{select_with_joins_sql, Relations};
use prkdb_orm_macros::Table;
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Table, Clone)]
#[table(name = "address", primary_key = "id")]
struct Address {
    id: i64,
    #[foreign(Person::id)]
    person_id: i64,
    street: String,
    city: String,
    zip_code: String,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Table, Clone)]
#[table(name = "people", primary_key = "id")]
struct Person {
    id: i64,
    name: String,
    age: Option<u8>,
    // Automatically inferred as a join because this is a Vec of another Table type.
    addresses: Vec<Address>,
}

#[test]
fn relations_metadata_exposed() {
    let fks = Address::foreign_keys();
    assert_eq!(fks, &[("person_id", "Person::id")]);

    let joins = Person::joins();
    assert_eq!(
        joins,
        &[("addresses", "address", "people.id = address.person_id")]
    );

    let pg_sql = select_with_joins_sql::<Person>(SqlDialect::Postgres);
    let expected_fragment = "INNER JOIN \"address\" ON people.id = address.person_id";
    assert!(
        pg_sql.contains(expected_fragment),
        "generated join SQL should stitch tables together, got: {}",
        pg_sql
    );
}
