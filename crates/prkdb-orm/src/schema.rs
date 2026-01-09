use crate::dialect::SqlDialect;
use crate::types::SqlValue;
use std::collections::HashMap;

/// Table trait implemented by derive(Table)
pub trait Table: Sized + Send + Sync + 'static {
    type Id: Send + Sync + Clone + 'static;

    fn table_name() -> &'static str;
    fn columns() -> &'static [&'static str];
    fn columns_meta(dialect: SqlDialect) -> Vec<(&'static str, &'static str)>;
    fn pk_column() -> &'static str;

    fn create_table_sql(dialect: SqlDialect) -> String;
    fn drop_table_sql(dialect: SqlDialect) -> String;
    fn select_all_sql(dialect: SqlDialect) -> String;
    fn select_by_pk_sql(dialect: SqlDialect) -> String;
    fn insert_sql(dialect: SqlDialect) -> String;
    fn update_sql(dialect: SqlDialect) -> String;
    fn delete_sql(dialect: SqlDialect) -> String;

    fn insert_params(&self) -> Vec<SqlValue>;
    fn pk_to_params(pk: &Self::Id) -> Vec<SqlValue>;
}

/// Trait for getting new builder instances for a table type
pub trait TableHelpers: Table {
    fn select() -> crate::builders::SelectBuilder<Self>;
    fn insert() -> crate::builders::InsertBuilder<Self>;
    fn update() -> crate::builders::UpdateBuilder<Self>;
    fn delete() -> crate::builders::DeleteBuilder<Self>;
}

/// Row representation used for decoding from query results.
pub type Row = HashMap<String, SqlValue>;

/// Decode a row into the concrete type.
pub trait FromRow: Sized {
    fn from_row(row: &Row) -> Result<Self, String>;
}

/// Relation metadata produced by derive macros.
pub trait Relations {
    /// Foreign keys declared on this table: (field_name, target_descriptor).
    /// The target descriptor is intentionally a string (e.g. "Person::id") to avoid type coupling.
    fn foreign_keys() -> &'static [(&'static str, &'static str)] {
        &[]
    }

    /// Declared joins: (field_name, target_table, on_expression).
    /// `on_expression` may be empty if not provided; consumers can infer defaults.
    fn joins() -> &'static [(&'static str, &'static str, &'static str)] {
        &[]
    }
}

/// Build a SELECT statement that eagerly stitches together all declared joins.
/// Joins are added as INNER JOINs using the `on` expression from `Relations::joins`.
pub fn select_with_joins_sql<T>(dialect: SqlDialect) -> String
where
    T: Table + Relations,
{
    let cols = T::columns().join(", ");
    let mut sql = format!(
        "SELECT {} FROM {}",
        cols,
        dialect.quote_ident(T::table_name())
    );
    for (_, table, on) in T::joins() {
        sql.push_str(&format!(
            " INNER JOIN {} ON {}",
            dialect.quote_ident(table),
            on
        ));
    }
    sql
}
