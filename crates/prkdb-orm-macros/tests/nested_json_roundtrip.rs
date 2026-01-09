use prkdb_orm::{schema::FromRow, schema::Row, schema::Table, types::SqlValue};
use prkdb_orm_macros::Table;
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize, PartialEq, Clone)]
struct Geo {
    lat: f64,
    lng: f64,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Clone)]
struct Address {
    street: String,
    city: String,
    zip_code: String,
    coords: Option<Geo>,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Clone)]
enum Preference {
    Weekly,
    Monthly,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Clone)]
struct Profile {
    bio: String,
    interests: Vec<String>,
    newsletter: Preference,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Table, Clone)]
#[table(name = "people", primary_key = "id")]
struct Person {
    id: i64,
    name: String,
    age: Option<u8>,
    #[column(json)]
    addresses: Vec<Address>,
    #[column(json)]
    profile: Profile,
}

#[test]
fn deep_nested_json_round_trip() {
    let primary = Address {
        street: "123 Hex Rd".to_string(),
        city: "Rustville".to_string(),
        zip_code: "99999".to_string(),
        coords: Some(Geo {
            lat: 42.0,
            lng: -71.0,
        }),
    };
    let secondary = Address {
        street: "456 Binary Ave".to_string(),
        city: "Enum City".to_string(),
        zip_code: "11111".to_string(),
        coords: None,
    };
    let profile = Profile {
        bio: "Loves nested data".to_string(),
        interests: vec!["databases".into(), "rust".into()],
        newsletter: Preference::Weekly,
    };
    let person = Person {
        id: 1,
        name: "Ada".to_string(),
        age: Some(8),
        addresses: vec![primary.clone(), secondary.clone()],
        profile: profile.clone(),
    };

    let params = person.insert_params();
    assert_eq!(params.len(), 5);

    let mut row: Row = Row::new();
    row.insert("id".to_string(), SqlValue::Int(1));
    row.insert("name".to_string(), SqlValue::Text("Ada".into()));
    row.insert("age".to_string(), SqlValue::Int(8));
    row.insert(
        "addresses".to_string(),
        SqlValue::Json(serde_json::to_string(&person.addresses).unwrap()),
    );
    row.insert(
        "profile".to_string(),
        SqlValue::Json(serde_json::to_string(&person.profile).unwrap()),
    );

    let decoded = Person::from_row(&row).expect("decode should succeed");
    assert_eq!(decoded, person);
}

#[test]
fn handles_absent_nested_values() {
    let profile = Profile {
        bio: "".to_string(),
        interests: vec![],
        newsletter: Preference::Monthly,
    };
    let person = Person {
        id: 2,
        name: "Grace".to_string(),
        age: None,
        addresses: vec![],
        profile: profile.clone(),
    };
    let mut row: Row = Row::new();
    row.insert("id".to_string(), SqlValue::Int(2));
    row.insert("name".to_string(), SqlValue::Text("Grace".into()));
    row.insert("age".to_string(), SqlValue::Null);
    row.insert(
        "addresses".to_string(),
        SqlValue::Json(serde_json::to_string(&person.addresses).unwrap()),
    );
    row.insert(
        "profile".to_string(),
        SqlValue::Json(serde_json::to_string(&person.profile).unwrap()),
    );

    let decoded =
        Person::from_row(&row).expect("decode should succeed with null age and empty vectors");
    assert_eq!(decoded, person);
}
