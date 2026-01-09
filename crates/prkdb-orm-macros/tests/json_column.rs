use prkdb_orm::schema::Table;
use prkdb_orm::types::SqlValue;
use prkdb_orm_macros::Table;
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize, PartialEq, Clone)]
struct Address {
    street: String,
    city: String,
}

#[derive(Debug, Serialize, Deserialize, PartialEq)]
enum Status {
    Active,
    Disabled,
}

#[derive(Debug, Serialize, Deserialize, Table, PartialEq)]
#[table(name = "users", primary_key = "id")]
struct User {
    id: i64,
    name: String,
    #[column(json)]
    address: Address,
    #[column(json)]
    status: Status,
}

#[test]
fn json_columns_are_serialized() {
    let user = User {
        id: 1,
        name: "Ada".to_string(),
        address: Address {
            street: "42 Rust Ave".to_string(),
            city: "Ferris".to_string(),
        },
        status: Status::Active,
    };

    let params = user.insert_params();
    assert_eq!(
        params,
        vec![
            SqlValue::Int(1),
            SqlValue::Text("Ada".to_string()),
            SqlValue::Json(
                serde_json::to_string(&user.address).expect("address should serialize to JSON")
            ),
            SqlValue::Json(
                serde_json::to_string(&user.status).expect("status should serialize to JSON")
            )
        ]
    );
}
