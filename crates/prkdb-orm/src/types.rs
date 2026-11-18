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

// Preserve the legacy SQL text, including truncation to whole seconds for naive values.
const NAIVE_DATETIME_FORMAT: &[time::format_description::FormatItem<'static>] =
    time::macros::format_description!("[year]-[month]-[day] [hour]:[minute]:[second]");

/// Keep the former UTC representation: +00:00 and 0/3/6/9 fractional digits.
/// The default time RFC3339 formatter uses Z and trims trailing fractional zeros.
fn format_utc_datetime(dt: time::OffsetDateTime) -> String {
    let dt = dt.to_offset(time::UtcOffset::UTC);
    let base = dt
        .format(time::macros::format_description!(
            "[year]-[month]-[day]T[hour]:[minute]:[second]"
        ))
        .expect("datetime contains every required component");
    let nanos = dt.nanosecond();
    let fraction = if nanos == 0 {
        String::new()
    } else if nanos.is_multiple_of(1_000_000) {
        format!(".{:03}", nanos / 1_000_000)
    } else if nanos.is_multiple_of(1_000) {
        format!(".{:06}", nanos / 1_000)
    } else {
        format!(".{nanos:09}")
    };
    format!("{base}{fraction}+00:00")
}
impl From<time::PrimitiveDateTime> for SqlValue {
    fn from(dt: time::PrimitiveDateTime) -> Self {
        SqlValue::DateTime(
            dt.format(NAIVE_DATETIME_FORMAT)
                .expect("datetime contains every required component"),
        )
    }
}
impl From<&time::PrimitiveDateTime> for SqlValue {
    fn from(dt: &time::PrimitiveDateTime) -> Self {
        SqlValue::DateTime(
            dt.format(NAIVE_DATETIME_FORMAT)
                .expect("datetime contains every required component"),
        )
    }
}

// OffsetDateTime -> UTC RFC3339 string, matching the former DateTime<Utc> mapping
impl From<time::OffsetDateTime> for SqlValue {
    fn from(dt: time::OffsetDateTime) -> Self {
        SqlValue::DateTime(format_utc_datetime(dt))
    }
}
impl From<&time::OffsetDateTime> for SqlValue {
    fn from(dt: &time::OffsetDateTime) -> Self {
        SqlValue::DateTime(format_utc_datetime(*dt))
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

impl FromSqlValue for time::PrimitiveDateTime {
    fn from_sql_value(v: &SqlValue) -> Result<Self, String> {
        match v {
            SqlValue::DateTime(s) | SqlValue::Text(s) => {
                time::PrimitiveDateTime::parse(s, NAIVE_DATETIME_FORMAT)
                    .map_err(|e| format!("naive datetime parse error: {e}"))
            }
            other => Err(format!("expected DateTime/Text, got {:?}", other)),
        }
    }
}

impl FromSqlValue for time::OffsetDateTime {
    fn from_sql_value(v: &SqlValue) -> Result<Self, String> {
        match v {
            SqlValue::DateTime(s) | SqlValue::Text(s) => {
                time::OffsetDateTime::parse(s, &time::format_description::well_known::Rfc3339)
                    .map(|dt| dt.to_offset(time::UtcOffset::UTC))
                    .map_err(|e| format!("datetime parse error: {e}"))
            }
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

// PrimitiveDateTime mapping
impl OrmType for time::PrimitiveDateTime {
    fn sql_type_name(dialect: SqlDialect) -> &'static str {
        match dialect {
            SqlDialect::Postgres => "TIMESTAMP",
            SqlDialect::MySql => "DATETIME",
            SqlDialect::Sqlite => "DATETIME",
        }
    }
}

// OffsetDateTime mapping
impl OrmType for time::OffsetDateTime {
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

#[cfg(test)]
mod datetime_tests {
    use super::*;

    #[test]
    fn naive_datetime_preserves_legacy_sql_text() {
        let stored = SqlValue::DateTime("2022-04-15 05:20:00".into());
        let value = time::PrimitiveDateTime::from_sql_value(&stored).unwrap();
        assert_eq!(SqlValue::from(value), stored);
        assert_eq!(SqlValue::from(&value), stored);
        assert_eq!(
            SqlValue::from(value.replace_nanosecond(123_456_789).unwrap()),
            stored
        );
        assert_eq!(
            time::PrimitiveDateTime::sql_type_name(SqlDialect::Postgres),
            "TIMESTAMP"
        );
        assert_eq!(
            time::PrimitiveDateTime::sql_type_name(SqlDialect::MySql),
            "DATETIME"
        );
        assert_eq!(
            time::PrimitiveDateTime::sql_type_name(SqlDialect::Sqlite),
            "DATETIME"
        );
    }

    #[test]
    fn utc_datetime_preserves_legacy_rfc3339_text_and_precision() {
        for text in [
            "2022-04-15T05:20:00+00:00",
            "2022-04-15T05:20:00.120+00:00",
            "2022-04-15T05:20:00.123400+00:00",
            "2022-04-15T05:20:00.123456789+00:00",
        ] {
            let stored = SqlValue::DateTime(text.into());
            let value = time::OffsetDateTime::from_sql_value(&stored).unwrap();
            assert_eq!(SqlValue::from(value), stored);
            assert_eq!(SqlValue::from(&value), stored);
        }
        assert_eq!(
            time::OffsetDateTime::sql_type_name(SqlDialect::Postgres),
            "TIMESTAMPTZ"
        );
        assert_eq!(
            time::OffsetDateTime::sql_type_name(SqlDialect::MySql),
            "DATETIME"
        );
        assert_eq!(
            time::OffsetDateTime::sql_type_name(SqlDialect::Sqlite),
            "DATETIME"
        );
    }

    #[test]
    fn offset_datetime_inputs_normalize_to_utc() {
        for text in ["2022-04-15T10:50:00+05:30", "2022-04-15T05:20:00Z"] {
            let value = time::OffsetDateTime::from_sql_value(&SqlValue::Text(text.into())).unwrap();
            assert_eq!(value.offset(), time::UtcOffset::UTC);
            let original =
                time::OffsetDateTime::parse(text, &time::format_description::well_known::Rfc3339)
                    .unwrap();
            assert_eq!(SqlValue::from(original), SqlValue::from(value));
            assert_eq!(SqlValue::from(&original), SqlValue::from(value));
            assert_eq!(
                SqlValue::from(value),
                SqlValue::DateTime("2022-04-15T05:20:00+00:00".into())
            );
        }
    }

    #[test]
    fn invalid_datetime_values_are_rejected() {
        for stored in [
            SqlValue::Text("not-a-date".into()),
            SqlValue::Text("2022-02-30 05:20:00".into()),
            SqlValue::Int(0),
            SqlValue::Null,
        ] {
            assert!(time::PrimitiveDateTime::from_sql_value(&stored).is_err());
            assert!(time::OffsetDateTime::from_sql_value(&stored).is_err());
        }
    }
}
