use crate::dialect::SqlDialect;

/// Quote identifier using dialect-specific quoting.
pub fn quote_ident(name: &str, dialect: SqlDialect) -> String {
    dialect.quote_ident(name)
}

/// Join columns with comma
pub fn join_cols(cols: &[&str]) -> String {
    cols.join(", ")
}

/// Create table DDL helper (generic)
pub fn create_table_sql(table_name: &str, cols_defs: &[(String, String)], dialect: SqlDialect) -> String {
    let mut defs = Vec::with_capacity(cols_defs.len());
    for (col, typ) in cols_defs {
        defs.push(format!("{} {}", dialect.quote_ident(col), typ));
    }
    format!("CREATE TABLE IF NOT EXISTS {} (\n    {}\n);", dialect.quote_ident(table_name), defs.join(",\n    "))
}

/// Convenience default dialect DDL
pub fn create_table_sql_default(table_name: &str, cols_defs: &[(String, String)]) -> String {
    create_table_sql(table_name, cols_defs, crate::dialect::DefaultDialect::current())
}
