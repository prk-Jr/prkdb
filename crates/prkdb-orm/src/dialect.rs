use std::fmt;

/// Supported SQL dialects at runtime.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SqlDialect {
    Sqlite,
    Postgres,
    MySql,
}

impl SqlDialect {
    pub fn quote_ident(&self, ident: &str) -> String {
        match self {
            SqlDialect::Sqlite => format!("\"{}\"", ident),
            SqlDialect::Postgres => format!("\"{}\"", ident),
            SqlDialect::MySql => format!("`{}`", ident),
        }
    }

    /// Placeholder at 1-based index (Postgres: $1, sqlite/mysql: ?)
    pub fn placeholder_at(&self, idx: usize) -> String {
        match self {
            SqlDialect::Postgres => format!("${}", idx),
            SqlDialect::Sqlite | SqlDialect::MySql => "?".to_string(),
        }
    }
}

impl fmt::Display for SqlDialect {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            SqlDialect::Sqlite => write!(f, "sqlite"),
            SqlDialect::Postgres => write!(f, "postgres"),
            SqlDialect::MySql => write!(f, "mysql"),
        }
    }
}

/// Default dialect chosen at compile time using Cargo features.
/// If none selected, fall back to sqlite.
/// If multiple selected, this will produce a compile-time error.
pub struct DefaultDialect;

impl DefaultDialect {
    pub fn current() -> SqlDialect {
        // If multiple dialect features are enabled, prefer Postgres > MySQL > SQLite.
        if cfg!(feature = "postgres") {
            SqlDialect::Postgres
        } else if cfg!(feature = "mysql") {
            SqlDialect::MySql
        } else {
            // Default to sqlite when enabled or when no SQL features are selected.
            SqlDialect::Sqlite
        }
    }
}
