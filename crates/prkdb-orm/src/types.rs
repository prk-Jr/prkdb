use crate::dialect::SqlDialect;

/// Represents a SQL value (simplified)
#[derive(Clone, Debug, PartialEq)]
pub enum SqlValue {
    Null,
    Int(i64),
    Float(f64),
    Bool(bool),
    Text(String),
    /// Datetime stored as string (format chosen by app; driver will map accordingly)
    DateTime(String),
    /// JSON stored as text (serialized JSON string)
    Json(String),
}

impl From<&str> for SqlValue {
    fn from(s: &str) -> Self {
        SqlValue::Text(s.to_owned())
    }
}
impl From<String> for SqlValue {
    fn from(s: String) -> Self {
        SqlValue::Text(s)
    }
}
impl From<&String> for SqlValue {
    fn from(s: &String) -> Self {
        SqlValue::Text(s.clone())
    }
}
impl From<i64> for SqlValue {
    fn from(v: i64) -> Self {
        SqlValue::Int(v)
    }
}
impl From<&i64> for SqlValue {
    fn from(v: &i64) -> Self {
        SqlValue::Int(*v)
    }
}
impl From<i32> for SqlValue {
    fn from(v: i32) -> Self {
        SqlValue::Int(v as i64)
    }
}

impl From<u8> for SqlValue {
    fn from(v: u8) -> Self {
        SqlValue::Int(v as i64)
    }
}
impl From<&u8> for SqlValue {
    fn from(v: &u8) -> Self {
        SqlValue::Int(*v as i64)
    }
}
impl From<u16> for SqlValue {
    fn from(v: u16) -> Self {
        SqlValue::Int(v as i64)
    }
}
impl From<&u16> for SqlValue {
    fn from(v: &u16) -> Self {
        SqlValue::Int(*v as i64)
    }
}
impl From<u32> for SqlValue {
    fn from(v: u32) -> Self {
        SqlValue::Int(v as i64)
    }
}
impl From<&u32> for SqlValue {
    fn from(v: &u32) -> Self {
        SqlValue::Int(*v as i64)
    }
}
impl From<u64> for SqlValue {
    fn from(v: u64) -> Self {
        SqlValue::Int(v as i64)
    }
}
impl From<&u64> for SqlValue {
    fn from(v: &u64) -> Self {
        SqlValue::Int(*v as i64)
    }
}
impl From<&i32> for SqlValue {
    fn from(v: &i32) -> Self {
        SqlValue::Int(*v as i64)
    }
}
impl From<f64> for SqlValue {
    fn from(v: f64) -> Self {
        SqlValue::Float(v)
    }
}
impl From<&f64> for SqlValue {
    fn from(v: &f64) -> Self {
        SqlValue::Float(*v)
    }
}
impl From<bool> for SqlValue {
    fn from(v: bool) -> Self {
        SqlValue::Bool(v)
    }
}
impl From<&bool> for SqlValue {
    fn from(v: &bool) -> Self {
        SqlValue::Bool(*v)
    }
}

// Chrono NaiveDateTime -> stored as "YYYY-MM-DD HH:MM:SS" string by default
impl From<chrono::NaiveDateTime> for SqlValue {
    fn from(dt: chrono::NaiveDateTime) -> Self {
        SqlValue::DateTime(dt.format("%Y-%m-%d %H:%M:%S").to_string())
    }
}
impl From<&chrono::NaiveDateTime> for SqlValue {
    fn from(dt: &chrono::NaiveDateTime) -> Self {
        SqlValue::DateTime(dt.format("%Y-%m-%d %H:%M:%S").to_string())
    }
}

// Chrono DateTime<Utc> -> RFC3339 string
impl From<chrono::DateTime<chrono::Utc>> for SqlValue {
    fn from(dt: chrono::DateTime<chrono::Utc>) -> Self {
        SqlValue::DateTime(dt.to_rfc3339())
    }
}
impl From<&chrono::DateTime<chrono::Utc>> for SqlValue {
    fn from(dt: &chrono::DateTime<chrono::Utc>) -> Self {
        SqlValue::DateTime(dt.to_rfc3339())
    }
}

// serde_json::Value -> JSON string
impl From<serde_json::Value> for SqlValue {
    fn from(j: serde_json::Value) -> Self {
        SqlValue::Json(serde_json::to_string(&j).unwrap_or_default())
    }
}
impl From<&serde_json::Value> for SqlValue {
    fn from(j: &serde_json::Value) -> Self {
        SqlValue::Json(serde_json::to_string(j).unwrap_or_default())
    }
}

/// Conversion from `SqlValue` into a concrete Rust type without relying on serde JSON.
pub trait FromSqlValue: Sized {
    fn from_sql_value(v: &SqlValue) -> Result<Self, String>;
}

impl FromSqlValue for i64 {
    fn from_sql_value(v: &SqlValue) -> Result<Self, String> {
        match v {
            SqlValue::Int(x) => Ok(*x),
            other => Err(format!("expected Int, got {:?}", other)),
        }
    }
}

impl FromSqlValue for i32 {
    fn from_sql_value(v: &SqlValue) -> Result<Self, String> {
        match v {
            SqlValue::Int(x) => Ok(*x as i32),
            other => Err(format!("expected Int, got {:?}", other)),
        }
    }
}

impl FromSqlValue for u8 {
    fn from_sql_value(v: &SqlValue) -> Result<Self, String> {
        match v {
            SqlValue::Int(x) if *x >= 0 && *x <= u8::MAX as i64 => Ok(*x as u8),
            other => Err(format!("expected unsigned Int (u8 range), got {:?}", other)),
        }
    }
}

impl FromSqlValue for u16 {
    fn from_sql_value(v: &SqlValue) -> Result<Self, String> {
        match v {
            SqlValue::Int(x) if *x >= 0 && *x <= u16::MAX as i64 => Ok(*x as u16),
            other => Err(format!(
                "expected unsigned Int (u16 range), got {:?}",
                other
            )),
        }
    }
}

impl FromSqlValue for u32 {
    fn from_sql_value(v: &SqlValue) -> Result<Self, String> {
        match v {
            SqlValue::Int(x) if *x >= 0 && *x <= u32::MAX as i64 => Ok(*x as u32),
            other => Err(format!(
                "expected unsigned Int (u32 range), got {:?}",
                other
            )),
        }
    }
}

impl FromSqlValue for u64 {
    fn from_sql_value(v: &SqlValue) -> Result<Self, String> {
        match v {
            SqlValue::Int(x) if *x >= 0 => Ok(*x as u64),
            other => Err(format!("expected unsigned Int, got {:?}", other)),
        }
    }
}

impl FromSqlValue for f64 {
    fn from_sql_value(v: &SqlValue) -> Result<Self, String> {
        match v {
            SqlValue::Float(x) => Ok(*x),
            other => Err(format!("expected Float, got {:?}", other)),
        }
    }
}

impl FromSqlValue for f32 {
    fn from_sql_value(v: &SqlValue) -> Result<Self, String> {
        match v {
            SqlValue::Float(x) => Ok(*x as f32),
            other => Err(format!("expected Float, got {:?}", other)),
        }
    }
}

impl FromSqlValue for bool {
    fn from_sql_value(v: &SqlValue) -> Result<Self, String> {
        match v {
            SqlValue::Bool(x) => Ok(*x),
            other => Err(format!("expected Bool, got {:?}", other)),
        }
    }
}

impl FromSqlValue for String {
    fn from_sql_value(v: &SqlValue) -> Result<Self, String> {
        match v {
            SqlValue::Text(s) => Ok(s.clone()),
            SqlValue::Json(s) => Ok(s.clone()),
            other => Err(format!("expected Text/Json, got {:?}", other)),
        }
    }
}

impl FromSqlValue for serde_json::Value {
    fn from_sql_value(v: &SqlValue) -> Result<Self, String> {
        match v {
            SqlValue::Json(s) | SqlValue::Text(s) => {
                serde_json::from_str(s).map_err(|e| format!("json decode error: {e}"))
            }
            other => Err(format!("expected Json/Text, got {:?}", other)),
        }
    }
}

impl FromSqlValue for chrono::NaiveDateTime {
    fn from_sql_value(v: &SqlValue) -> Result<Self, String> {
        match v {
            SqlValue::DateTime(s) | SqlValue::Text(s) => {
                chrono::NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S")
                    .map_err(|e| format!("naive datetime parse error: {e}"))
            }
            other => Err(format!("expected DateTime/Text, got {:?}", other)),
        }
    }
}

impl FromSqlValue for chrono::DateTime<chrono::Utc> {
    fn from_sql_value(v: &SqlValue) -> Result<Self, String> {
        match v {
            SqlValue::DateTime(s) | SqlValue::Text(s) => chrono::DateTime::parse_from_rfc3339(s)
                .map(|dt| dt.with_timezone(&chrono::Utc))
                .map_err(|e| format!("datetime parse error: {e}")),
            other => Err(format!("expected DateTime/Text, got {:?}", other)),
        }
    }
}

impl<T> FromSqlValue for Option<T>
where
    T: FromSqlValue,
{
    fn from_sql_value(v: &SqlValue) -> Result<Self, String> {
        match v {
            SqlValue::Null => Ok(None),
            other => Ok(Some(T::from_sql_value(other)?)),
        }
    }
}

/// Trait for mapping Rust types to SQL column types (dialect-aware)
pub trait OrmType {
    fn sql_type_name(dialect: crate::dialect::SqlDialect) -> &'static str;
}

impl OrmType for String {
    fn sql_type_name(_: SqlDialect) -> &'static str {
        "TEXT"
    }
}
impl OrmType for &str {
    fn sql_type_name(_: SqlDialect) -> &'static str {
        "TEXT"
    }
}
impl OrmType for i64 {
    fn sql_type_name(_: SqlDialect) -> &'static str {
        "BIGINT"
    }
}
impl OrmType for i32 {
    fn sql_type_name(_: SqlDialect) -> &'static str {
        "INTEGER"
    }
}
impl OrmType for u8 {
    fn sql_type_name(_: SqlDialect) -> &'static str {
        "INTEGER"
    }
}
impl OrmType for u16 {
    fn sql_type_name(_: SqlDialect) -> &'static str {
        "INTEGER"
    }
}
impl OrmType for u32 {
    fn sql_type_name(_: SqlDialect) -> &'static str {
        "BIGINT"
    }
}
impl OrmType for u64 {
    fn sql_type_name(_: SqlDialect) -> &'static str {
        "BIGINT"
    }
}
impl OrmType for f64 {
    fn sql_type_name(_: SqlDialect) -> &'static str {
        "REAL"
    }
}
impl OrmType for bool {
    fn sql_type_name(_: SqlDialect) -> &'static str {
        "BOOLEAN"
    }
}

impl<T> OrmType for Option<T>
where
    T: OrmType,
{
    fn sql_type_name(dialect: SqlDialect) -> &'static str {
        T::sql_type_name(dialect)
    }
}

// NaiveDateTime mapping
impl OrmType for chrono::NaiveDateTime {
    fn sql_type_name(dialect: SqlDialect) -> &'static str {
        match dialect {
            SqlDialect::Postgres => "TIMESTAMP",
            SqlDialect::MySql => "DATETIME",
            SqlDialect::Sqlite => "DATETIME",
        }
    }
}

// DateTime<Utc> mapping
impl OrmType for chrono::DateTime<chrono::Utc> {
    fn sql_type_name(dialect: SqlDialect) -> &'static str {
        match dialect {
            SqlDialect::Postgres => "TIMESTAMPTZ",
            SqlDialect::MySql => "DATETIME",
            SqlDialect::Sqlite => "DATETIME",
        }
    }
}

// serde_json::Value mapping (dialect-aware)
impl OrmType for serde_json::Value {
    fn sql_type_name(dialect: SqlDialect) -> &'static str {
        match dialect {
            SqlDialect::Postgres => "JSONB",
            SqlDialect::MySql => "JSON",
            SqlDialect::Sqlite => "TEXT", // sqlite doesn't have a native JSON column type (store as TEXT)
        }
    }
}
