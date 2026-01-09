use prkdb_orm::{dialect::DefaultDialect, types::SqlValue, Table, TableHelpers};
use prkdb_orm_macros::Table;

#[derive(Debug, Clone, PartialEq, Table)]
#[table(name = "customers", primary_key = "id")]
pub struct Customer {
    pub id: i64,
    pub first_name: String,
    pub last_name: String,
    pub mobile_number: String,
    pub age: i32,
    pub active: bool,
}

#[test]
fn ddl_create_table_has_columns_and_pk() {
    let sql = Customer::create_table_sql(DefaultDialect::current());
    assert!(sql.to_uppercase().contains("CREATE TABLE IF NOT EXISTS"));
    assert!(sql.to_lowercase().contains("customers"));
    assert!(sql.contains("id"));
    assert!(sql.contains("first_name"));
    assert!(sql.contains("PRIMARY KEY"));
}

#[test]
fn insert_sql_and_params_order() {
    let ins_sql = Customer::insert_sql(DefaultDialect::current());
    assert!(ins_sql.to_uppercase().starts_with("INSERT INTO"));
    let c = Customer {
        id: 1,
        first_name: "John".to_string(),
        last_name: "Doe".to_string(),
        mobile_number: "999".to_string(),
        age: 30,
        active: true,
    };
    let params = c.insert_params();
    assert_eq!(params.len(), 6);
    assert_eq!(params[0], SqlValue::Int(1));
    assert_eq!(params[1], SqlValue::Text("John".to_string()));
}

#[test]
fn update_builder_generates_sql_and_params_ordered() {
    let (sql, params) = Customer::update()
        .update_first_name_with_value("JANE".to_string())
        .update_last_name_with_value("DOE".to_string())
        .where_mobile_number_eq("999".to_string())
        .or_where_id_eq(1)
        .where_age_eq(18)
        .to_sql();

    dbg!(&sql);

    assert!(sql.to_uppercase().contains("UPDATE"));
    assert_eq!(params.len(), 5);
    assert_eq!(params[0], SqlValue::Text("JANE".to_string()));
    assert_eq!(params[1], SqlValue::Text("DOE".to_string()));
    assert_eq!(params[2], SqlValue::Text("999".to_string()));
    assert_eq!(params[3], SqlValue::Int(1));
}

#[test]
fn insert_builder_field_helpers_exist() {
    let (sql, params) = Customer::insert()
        .insert_first_name("Jane".to_string())
        .insert_last_name("Doe".to_string())
        .insert_mobile_number("123".to_string())
        .insert_age(25)
        .insert_active(true)
        .to_sql();

    assert!(sql.to_uppercase().starts_with("INSERT INTO"));
    assert!(sql.contains("first_name") && sql.contains("last_name"));
    assert_eq!(params.len(), 5);
    assert_eq!(params[0], SqlValue::Text("Jane".into()));
    assert_eq!(params[3], SqlValue::Int(25));
}

#[test]
fn select_builder_where_order_limit_offset_with_type_safe_ops() {
    let (sql1, params1) = Customer::select()
        .where_first_name_eq("John".to_string())
        .where_age_eq(30)
        .order_by("age", true)
        .limit(10)
        .offset(5)
        .to_sql();

    assert!(sql1.to_uppercase().starts_with("SELECT"));
    assert!(sql1.contains("WHERE"));
    assert!(sql1.contains("LIMIT 10"));
    assert_eq!(params1.len(), 2);
    assert_eq!(params1[0], SqlValue::Text("John".to_string()));
    assert_eq!(params1[1], SqlValue::Int(30));

    // String-specific helper usage:
    let (sql2, params2) = Customer::select()
        .where_first_name_eq("Alice".to_string())
        .to_sql();

    assert!(sql2.to_uppercase().starts_with("SELECT"));
    assert!(!params2.is_empty());
    assert!(sql2.contains("WHERE"));
    assert!(params2
        .iter()
        .any(|p| matches!(p, SqlValue::Text(s) if s.contains("Al"))));
}

#[test]
fn numeric_ops_exist_for_numeric_fields() {
    // test add_where_custom directly with numeric op
    let (sql, params) = Customer::select()
        .add_where_custom("age", ">", SqlValue::Int(21))
        .to_sql();
    assert!(sql.contains(">") || params.iter().any(|p| matches!(p, SqlValue::Int(_))));
}

#[test]
fn delete_builder_where() {
    let (sql, params) = Customer::delete()
        .where_mobile_number_eq("999".to_string())
        .to_sql();
    assert!(sql.to_uppercase().starts_with("DELETE"));
    assert_eq!(params.len(), 1);
    assert_eq!(params[0], SqlValue::Text("999".to_string()));
}
