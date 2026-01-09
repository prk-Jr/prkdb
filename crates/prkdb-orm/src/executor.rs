//! Optional executors backed by `sqlx`. Feature-gated per supported database.
//! Users can also implement `QueryExecutor` on their own types if they prefer
//! a different driver.

use crate::builders::QueryExecutor;
use crate::dialect::SqlDialect;
use crate::schema::Row;
use crate::types::SqlValue;
use async_trait::async_trait;
#[cfg(any(feature = "sqlite", feature = "postgres", feature = "mysql"))]
use sqlx::{Arguments, Column, Row as SqlxRow};
use std::collections::HashMap;

/// Rewrite dialect-agnostic `?` placeholders to Postgres-style `$N`.
#[cfg(feature = "postgres")]
fn rewrite_pg_placeholders(sql: &str) -> String {
    let mut out = String::with_capacity(sql.len());
    let mut idx = 1;
    for ch in sql.chars() {
        if ch == '?' {
            out.push('$');
            out.push_str(&idx.to_string());
            idx += 1;
        } else {
            out.push(ch);
        }
    }
    out
}

#[cfg(feature = "sqlite")]
fn sqlite_row_to_map(row: &sqlx::sqlite::SqliteRow) -> Row {
    let mut map = HashMap::new();
    for (idx, col) in row.columns().iter().enumerate() {
        let name = col.name().to_string();
        // Try null first
        if let Ok(opt) = row.try_get::<Option<i64>, _>(idx) {
            match opt {
                Some(v) => map.insert(name, SqlValue::Int(v)),
                None => map.insert(name, SqlValue::Null),
            };
            continue;
        }
        if let Ok(Some(v)) = row.try_get::<Option<f64>, _>(idx) {
            map.insert(name, SqlValue::Float(v));
            continue;
        }
        if let Ok(Some(v)) = row.try_get::<Option<bool>, _>(idx) {
            map.insert(name, SqlValue::Bool(v));
            continue;
        }
        if let Ok(opt) = row.try_get::<Option<String>, _>(idx) {
            match opt {
                Some(v) => map.insert(name, SqlValue::Text(v)),
                None => map.insert(name, SqlValue::Null),
            };
            continue;
        }
        map.insert(name, SqlValue::Null);
    }
    map
}

#[cfg(feature = "postgres")]
fn pg_row_to_map(row: &sqlx::postgres::PgRow) -> Row {
    let mut map = HashMap::new();
    for (idx, col) in row.columns().iter().enumerate() {
        let name = col.name().to_string();
        if let Ok(opt) = row.try_get::<Option<i64>, _>(idx) {
            if let Some(v) = opt {
                map.insert(name, SqlValue::Int(v));
                continue;
            } else {
                map.insert(name, SqlValue::Null);
                continue;
            }
        }
        if let Ok(opt) = row.try_get::<Option<f64>, _>(idx) {
            if let Some(v) = opt {
                map.insert(name, SqlValue::Float(v));
                continue;
            } else {
                map.insert(name, SqlValue::Null);
                continue;
            }
        }
        if let Ok(opt) = row.try_get::<Option<bool>, _>(idx) {
            if let Some(v) = opt {
                map.insert(name, SqlValue::Bool(v));
                continue;
            } else {
                map.insert(name, SqlValue::Null);
                continue;
            }
        }
        if let Ok(opt) = row.try_get::<Option<String>, _>(idx) {
            match opt {
                Some(v) => {
                    map.insert(name, SqlValue::Text(v));
                    continue;
                }
                None => {
                    map.insert(name, SqlValue::Null);
                    continue;
                }
            }
        }
        map.insert(name, SqlValue::Null);
    }
    map
}

#[cfg(feature = "mysql")]
fn mysql_row_to_map(row: &sqlx::mysql::MySqlRow) -> Row {
    let mut map = HashMap::new();
    for (idx, col) in row.columns().iter().enumerate() {
        let name = col.name().to_string();
        if let Ok(opt) = row.try_get::<Option<i64>, _>(idx) {
            if let Some(v) = opt {
                map.insert(name, SqlValue::Int(v));
                continue;
            } else {
                map.insert(name, SqlValue::Null);
                continue;
            }
        }
        if let Ok(opt) = row.try_get::<Option<f64>, _>(idx) {
            if let Some(v) = opt {
                map.insert(name, SqlValue::Float(v));
                continue;
            } else {
                map.insert(name, SqlValue::Null);
                continue;
            }
        }
        if let Ok(opt) = row.try_get::<Option<bool>, _>(idx) {
            if let Some(v) = opt {
                map.insert(name, SqlValue::Bool(v));
                continue;
            } else {
                map.insert(name, SqlValue::Null);
                continue;
            }
        }
        if let Ok(opt) = row.try_get::<Option<String>, _>(idx) {
            match opt {
                Some(v) => {
                    map.insert(name, SqlValue::Text(v));
                    continue;
                }
                None => {
                    map.insert(name, SqlValue::Null);
                    continue;
                }
            }
        }
        map.insert(name, SqlValue::Null);
    }
    map
}

#[cfg(feature = "sqlite")]
fn bind_sqlite_args(params: &[SqlValue]) -> Result<sqlx::sqlite::SqliteArguments<'_>, sqlx::Error> {
    let mut args = sqlx::sqlite::SqliteArguments::default();
    for v in params {
        match v {
            SqlValue::Null => args
                .add::<Option<i64>>(None)
                .map_err(|e| sqlx::Error::Protocol(e.to_string()))?,
            SqlValue::Int(x) => args
                .add(*x)
                .map_err(|e| sqlx::Error::Protocol(e.to_string()))?,
            SqlValue::Float(f) => args
                .add(*f)
                .map_err(|e| sqlx::Error::Protocol(e.to_string()))?,
            SqlValue::Bool(b) => args
                .add(*b)
                .map_err(|e| sqlx::Error::Protocol(e.to_string()))?,
            SqlValue::Text(s) | SqlValue::DateTime(s) | SqlValue::Json(s) => args
                .add(s.clone())
                .map_err(|e| sqlx::Error::Protocol(e.to_string()))?,
        };
    }
    Ok(args)
}

#[cfg(feature = "postgres")]
fn bind_pg_args(params: &[SqlValue]) -> Result<sqlx::postgres::PgArguments, sqlx::Error> {
    let mut args = sqlx::postgres::PgArguments::default();
    for v in params {
        match v {
            SqlValue::Null => args
                .add::<Option<i64>>(None)
                .map_err(|e| sqlx::Error::Protocol(e.to_string()))?,
            SqlValue::Int(x) => args
                .add(*x)
                .map_err(|e| sqlx::Error::Protocol(e.to_string()))?,
            SqlValue::Float(f) => args
                .add(*f)
                .map_err(|e| sqlx::Error::Protocol(e.to_string()))?,
            SqlValue::Bool(b) => args
                .add(*b)
                .map_err(|e| sqlx::Error::Protocol(e.to_string()))?,
            SqlValue::Text(s) | SqlValue::DateTime(s) | SqlValue::Json(s) => args
                .add(s.clone())
                .map_err(|e| sqlx::Error::Protocol(e.to_string()))?,
        };
    }
    Ok(args)
}

#[cfg(feature = "mysql")]
fn bind_mysql_args(params: &[SqlValue]) -> Result<sqlx::mysql::MySqlArguments, sqlx::Error> {
    let mut args = sqlx::mysql::MySqlArguments::default();
    for v in params {
        match v {
            SqlValue::Null => args
                .add::<Option<i64>>(None)
                .map_err(|e| sqlx::Error::Protocol(e.to_string()))?,
            SqlValue::Int(x) => args
                .add(*x)
                .map_err(|e| sqlx::Error::Protocol(e.to_string()))?,
            SqlValue::Float(f) => args
                .add(*f)
                .map_err(|e| sqlx::Error::Protocol(e.to_string()))?,
            SqlValue::Bool(b) => args
                .add(*b)
                .map_err(|e| sqlx::Error::Protocol(e.to_string()))?,
            SqlValue::Text(s) | SqlValue::DateTime(s) | SqlValue::Json(s) => args
                .add(s.clone())
                .map_err(|e| sqlx::Error::Protocol(e.to_string()))?,
        };
    }
    Ok(args)
}

#[cfg(feature = "sqlite")]
pub struct SqlxSqliteExecutor<'a> {
    pool: &'a sqlx::SqlitePool,
}

#[cfg(feature = "sqlite")]
impl<'a> SqlxSqliteExecutor<'a> {
    pub fn new(pool: &'a sqlx::SqlitePool) -> Self {
        Self { pool }
    }
}

#[cfg(feature = "sqlite")]
#[async_trait]
impl<'a> QueryExecutor for SqlxSqliteExecutor<'a> {
    type Error = sqlx::Error;

    async fn query(
        &self,
        sql: &str,
        params: &[SqlValue],
        _dialect: SqlDialect,
    ) -> Result<Vec<Row>, Self::Error> {
        let args = bind_sqlite_args(params)?;
        let rows = sqlx::query_with(sql, args).fetch_all(self.pool).await?;
        Ok(rows.into_iter().map(|r| sqlite_row_to_map(&r)).collect())
    }

    async fn execute(
        &self,
        sql: &str,
        params: &[SqlValue],
        _dialect: SqlDialect,
    ) -> Result<u64, Self::Error> {
        let args = bind_sqlite_args(params)?;
        let res = sqlx::query_with(sql, args).execute(self.pool).await?;
        Ok(res.rows_affected())
    }
}

#[cfg(feature = "sqlite")]
impl<'a> SqlxSqliteExecutor<'a> {
    /// Execute an insert and return the last inserted rowid (i64).
    pub async fn insert_and_return_id(
        &self,
        sql: &str,
        params: &[SqlValue],
    ) -> Result<i64, sqlx::Error> {
        let args = bind_sqlite_args(params)?;
        let mut conn = self.pool.acquire().await?;
        sqlx::query_with(sql, args).execute(&mut *conn).await?;
        let id: i64 = sqlx::query_scalar("SELECT last_insert_rowid()")
            .fetch_one(&mut *conn)
            .await?;
        Ok(id)
    }
}

#[cfg(feature = "postgres")]
pub struct SqlxPostgresExecutor<'a> {
    pool: &'a sqlx::PgPool,
}

#[cfg(feature = "postgres")]
impl<'a> SqlxPostgresExecutor<'a> {
    pub fn new(pool: &'a sqlx::PgPool) -> Self {
        Self { pool }
    }

    /// Connect with `DATABASE_URL` env var (helper for examples).
    pub async fn connect_default_env() -> Result<sqlx::PgPool, sqlx::Error> {
        let db_url = std::env::var("DATABASE_URL")
            .unwrap_or_else(|_| "postgres://localhost/postgres".into());
        sqlx::PgPool::connect(&db_url).await
    }
}

#[cfg(feature = "postgres")]
#[async_trait]
impl<'a> QueryExecutor for SqlxPostgresExecutor<'a> {
    type Error = sqlx::Error;

    async fn query(
        &self,
        sql: &str,
        params: &[SqlValue],
        _dialect: SqlDialect,
    ) -> Result<Vec<Row>, Self::Error> {
        let sql = rewrite_pg_placeholders(sql);
        let args = bind_pg_args(params)?;
        let rows = sqlx::query_with(&sql, args).fetch_all(self.pool).await?;
        Ok(rows.into_iter().map(|r| pg_row_to_map(&r)).collect())
    }

    async fn execute(
        &self,
        sql: &str,
        params: &[SqlValue],
        _dialect: SqlDialect,
    ) -> Result<u64, Self::Error> {
        let sql = rewrite_pg_placeholders(sql);
        let args = bind_pg_args(params)?;
        let res = sqlx::query_with(&sql, args).execute(self.pool).await?;
        Ok(res.rows_affected())
    }
}

#[cfg(feature = "postgres")]
impl<'a> SqlxPostgresExecutor<'a> {
    /// Execute an insert and return the generated id using `RETURNING <pk>`.
    pub async fn insert_and_return_id(
        &self,
        sql: &str,
        params: &[SqlValue],
        pk_column: &str,
    ) -> Result<i64, sqlx::Error> {
        let sql = format!("{} RETURNING {}", rewrite_pg_placeholders(sql), pk_column);
        let args = bind_pg_args(params)?;
        let id: i64 = sqlx::query_scalar_with(&sql, args)
            .fetch_one(self.pool)
            .await?;
        Ok(id)
    }
}

#[cfg(feature = "mysql")]
pub struct SqlxMySqlExecutor<'a> {
    pool: &'a sqlx::MySqlPool,
}

#[cfg(feature = "mysql")]
impl<'a> SqlxMySqlExecutor<'a> {
    pub fn new(pool: &'a sqlx::MySqlPool) -> Self {
        Self { pool }
    }
}

#[cfg(feature = "mysql")]
#[async_trait]
impl<'a> QueryExecutor for SqlxMySqlExecutor<'a> {
    type Error = sqlx::Error;

    async fn query(
        &self,
        sql: &str,
        params: &[SqlValue],
        _dialect: SqlDialect,
    ) -> Result<Vec<Row>, Self::Error> {
        let args = bind_mysql_args(params)?;
        let rows = sqlx::query_with(sql, args).fetch_all(self.pool).await?;
        Ok(rows.into_iter().map(|r| mysql_row_to_map(&r)).collect())
    }

    async fn execute(
        &self,
        sql: &str,
        params: &[SqlValue],
        _dialect: SqlDialect,
    ) -> Result<u64, Self::Error> {
        let args = bind_mysql_args(params)?;
        let res = sqlx::query_with(sql, args).execute(self.pool).await?;
        Ok(res.rows_affected())
    }
}

#[cfg(feature = "mysql")]
impl<'a> SqlxMySqlExecutor<'a> {
    /// Execute an insert and return LAST_INSERT_ID().
    pub async fn insert_and_return_id(
        &self,
        sql: &str,
        params: &[SqlValue],
    ) -> Result<i64, sqlx::Error> {
        let args = bind_mysql_args(params)?;
        let mut conn = self.pool.acquire().await?;
        sqlx::query_with(sql, args).execute(&mut *conn).await?;
        let id: i64 = sqlx::query_scalar("SELECT LAST_INSERT_ID()")
            .fetch_one(&mut *conn)
            .await?;
        Ok(id)
    }
}
