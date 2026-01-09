use crate::schema::{Row, Table};
use crate::types::SqlValue;
use async_trait::async_trait;

/// Glue enum stored as string for simplicity ("AND" or "OR").
#[derive(Debug, Clone)]
pub enum Glue {
    And,
    Or,
}

impl Glue {
    fn as_str(&self) -> &'static str {
        match self {
            Glue::And => "AND",
            Glue::Or => "OR",
        }
    }
}

/// (col, op, glue, value)
pub type WhereEntry = (String, String, Glue, SqlValue);
/// (col, values, glue)
pub type WhereInEntry = (String, Vec<SqlValue>, Glue);
/// (col, op, glue, value) for having
pub type HavingEntry = (String, String, Glue, SqlValue);
/// (type, table, on)
pub type JoinEntry = (String, String, String);

#[derive(Debug, Clone)]
pub struct SelectBuilder<T> {
    pub _marker: std::marker::PhantomData<T>,
    pub wheres: Vec<WhereEntry>,
    pub wheres_in: Vec<WhereInEntry>,
    pub group_bys: Vec<String>,
    pub order_bys: Vec<(String, bool)>,
    pub havings: Vec<HavingEntry>,
    pub joins: Vec<JoinEntry>,
    pub limit: Option<usize>,
    pub offset: Option<usize>,
}

impl<T> Default for SelectBuilder<T> {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Debug, Clone)]
pub struct UpdateBuilder<T> {
    pub _marker: std::marker::PhantomData<T>,
    pub wheres: Vec<WhereEntry>,
    pub wheres_in: Vec<WhereInEntry>,
    pub updates: Vec<(String, SqlValue)>,
}

impl<T> Default for UpdateBuilder<T> {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Debug, Clone)]
pub struct DeleteBuilder<T> {
    pub _marker: std::marker::PhantomData<T>,
    pub wheres: Vec<WhereEntry>,
    pub wheres_in: Vec<WhereInEntry>,
    pub limit: Option<usize>,
    pub offset: Option<usize>,
}

impl<T> Default for DeleteBuilder<T> {
    fn default() -> Self {
        Self::new()
    }
}

/// Generic InsertBuilder<T>
/// - collects column names and SqlValue parameters
/// - can produce an INSERT SQL string with `?` placeholders and the flattened params
#[derive(Debug, Clone)]
pub struct InsertBuilder<T> {
    pub _marker: std::marker::PhantomData<T>,
    cols: Vec<String>,
    vals: Vec<SqlValue>,
}

impl<T> Default for InsertBuilder<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T> InsertBuilder<T> {
    /// Create a new empty InsertBuilder
    pub fn new() -> Self {
        Self {
            _marker: std::marker::PhantomData,
            cols: Vec::new(),
            vals: Vec::new(),
        }
    }

    /// Add a single column/value pair
    pub fn add(mut self, col: &str, v: SqlValue) -> Self {
        self.cols.push(col.to_string());
        self.vals.push(v);
        self
    }

    /// Add multiple pairs from an iterator of (&str, SqlValue)
    pub fn add_pairs<I>(mut self, pairs: I) -> Self
    where
        I: IntoIterator<Item = (&'static str, SqlValue)>,
    {
        for (c, v) in pairs {
            self.cols.push(c.to_string());
            self.vals.push(v);
        }
        self
    }

    /// Build SQL and params. Uses `?` placeholders (SQLite/MySQL style).
    /// The SQL will be `INSERT INTO <table> (col1, col2, ...) VALUES (?, ?, ...)`.
    pub fn to_sql(self) -> (String, Vec<SqlValue>)
    where
        T: Table,
    {
        let col_list = self.cols.join(", ");
        let placeholders = if self.vals.is_empty() {
            // no values: produce VALUES ()
            String::new()
        } else {
            std::iter::repeat_n("?", self.vals.len())
                .collect::<Vec<_>>()
                .join(", ")
        };
        let sql = if self.vals.is_empty() {
            // Edge case: no values - produce a minimal insert (may be invalid for some DBs)
            format!(
                "INSERT INTO {} ({}) VALUES ()",
                <T as Table>::table_name(),
                col_list
            )
        } else {
            format!(
                "INSERT INTO {} ({}) VALUES ({})",
                <T as Table>::table_name(),
                col_list,
                placeholders
            )
        };
        (sql, self.vals)
    }

    /// Execute the insert and return rows affected (driver-specific; default counts returned rows).
    pub async fn exec<E>(
        self,
        executor: &E,
        dialect: crate::dialect::SqlDialect,
    ) -> Result<u64, String>
    where
        T: Table,
        E: QueryExecutor + Sync,
    {
        let (sql, params) = self.to_sql();
        executor
            .execute(&sql, &params, dialect)
            .await
            .map_err(|e| e.to_string())
    }
}

impl<T> SelectBuilder<T> {
    pub fn new() -> Self {
        Self {
            _marker: std::marker::PhantomData,
            wheres: Vec::new(),
            wheres_in: Vec::new(),
            group_bys: Vec::new(),
            order_bys: Vec::new(),
            havings: Vec::new(),
            joins: Vec::new(),
            limit: None,
            offset: None,
        }
    }

    // where helpers
    pub fn add_where_eq(mut self, col: &str, val: SqlValue) -> Self {
        self.wheres
            .push((col.to_string(), "=".to_string(), Glue::And, val));
        self
    }
    pub fn add_or_where_eq(mut self, col: &str, val: SqlValue) -> Self {
        self.wheres
            .push((col.to_string(), "=".to_string(), Glue::Or, val));
        self
    }
    pub fn add_where_like(mut self, col: &str, val: SqlValue) -> Self {
        self.wheres
            .push((col.to_string(), "LIKE".to_string(), Glue::And, val));
        self
    }
    pub fn add_where_custom(mut self, col: &str, op: &str, val: SqlValue) -> Self {
        self.wheres
            .push((col.to_string(), op.to_string(), Glue::And, val));
        self
    }

    pub fn add_where_in(mut self, col: &str, vals: Vec<SqlValue>) -> Self {
        self.wheres_in.push((col.to_string(), vals, Glue::And));
        self
    }

    pub fn add_or_where_in(mut self, col: &str, vals: Vec<SqlValue>) -> Self {
        self.wheres_in.push((col.to_string(), vals, Glue::Or));
        self
    }

    // group/order/having/join
    pub fn add_group_by(mut self, col: &str) -> Self {
        self.group_bys.push(col.to_string());
        self
    }
    pub fn add_order_by(mut self, col: &str, asc: bool) -> Self {
        self.order_bys.push((col.to_string(), asc));
        self
    }
    pub fn add_having_eq(mut self, col: &str, val: SqlValue) -> Self {
        self.havings
            .push((col.to_string(), "=".to_string(), Glue::And, val));
        self
    }
    pub fn add_or_having_eq(mut self, col: &str, val: SqlValue) -> Self {
        self.havings
            .push((col.to_string(), "=".to_string(), Glue::Or, val));
        self
    }
    pub fn add_or_having_custom(mut self, col: &str, op: &str, val: SqlValue) -> Self {
        self.havings
            .push((col.to_string(), op.to_string(), Glue::Or, val));
        self
    }
    pub fn add_join(mut self, typ: &str, table: &str, on: &str) -> Self {
        self.joins
            .push((typ.to_string(), table.to_string(), on.to_string()));
        self
    }

    pub fn limit(mut self, n: usize) -> Self {
        self.limit = Some(n);
        self
    }
    pub fn offset(mut self, n: usize) -> Self {
        self.offset = Some(n);
        self
    }

    /// Build SQL: produces `?` placeholders and flattened params.
    pub fn to_sql(self) -> (String, Vec<SqlValue>)
    where
        T: Table,
    {
        let cols = <T as Table>::columns().join(", ");
        let mut sql = format!("SELECT {} FROM {}", cols, <T as Table>::table_name());
        let mut params: Vec<SqlValue> = Vec::new();

        // joins
        for (typ, table, on) in &self.joins {
            sql.push_str(&format!(" {} {} ON {}", typ, table, on));
        }

        // WHERE with glue logic
        if !self.wheres.is_empty() || !self.wheres_in.is_empty() {
            let mut parts: Vec<String> = Vec::new();
            let mut first = true;
            for (col, op, glue, _) in &self.wheres {
                let cond = format!("{} {} ?", col, op);
                if first {
                    parts.push(cond);
                    first = false;
                } else {
                    parts.push(format!("{} {}", glue.as_str(), cond));
                }
            }
            for (col, vals, glue) in &self.wheres_in {
                let placeholders = std::iter::repeat_n("?", vals.len())
                    .collect::<Vec<_>>()
                    .join(", ");
                let cond = format!("{} IN ({})", col, placeholders);
                if first {
                    parts.push(cond);
                    first = false;
                } else {
                    parts.push(format!("{} {}", glue.as_str(), cond));
                }
            }
            if !parts.is_empty() {
                sql.push_str(&format!(" WHERE {}", parts.join(" ")));
            }

            // push params in the same order
            for (_c, _op, _g, v) in self.wheres.into_iter() {
                params.push(v);
            }
            for (_c, vals, _g) in self.wheres_in.into_iter() {
                for v in vals {
                    params.push(v);
                }
            }
        }

        // GROUP BY
        if !self.group_bys.is_empty() {
            sql.push_str(&format!(" GROUP BY {}", self.group_bys.join(", ")));
        }

        // HAVING
        if !self.havings.is_empty() {
            let mut parts: Vec<String> = Vec::new();
            let mut first = true;
            for (col, op, glue, _) in &self.havings {
                let cond = format!("{} {} ?", col, op);
                if first {
                    parts.push(cond);
                    first = false;
                } else {
                    parts.push(format!("{} {}", glue.as_str(), cond));
                }
            }
            if !parts.is_empty() {
                sql.push_str(&format!(" HAVING {}", parts.join(" ")));
            }
            for (_c, _op, _g, v) in self.havings.into_iter() {
                params.push(v);
            }
        }

        // ORDER BY
        if !self.order_bys.is_empty() {
            let parts: Vec<String> = self
                .order_bys
                .into_iter()
                .map(|(c, asc)| format!("{} {}", c, if asc { "ASC" } else { "DESC" }))
                .collect();
            sql.push_str(&format!(" ORDER BY {}", parts.join(", ")));
        }

        if let Some(l) = self.limit {
            sql.push_str(&format!(" LIMIT {}", l));
        }
        if let Some(o) = self.offset {
            sql.push_str(&format!(" OFFSET {}", o));
        }

        (sql, params)
    }

    /// Execute this select using a provided executor and decode a single row.
    /// Returns an error if no rows are returned.
    pub async fn fetch_one<E>(
        self,
        executor: &E,
        dialect: crate::dialect::SqlDialect,
    ) -> Result<T, String>
    where
        T: Table + crate::schema::FromRow,
        E: QueryExecutor + Sync,
    {
        let (sql, params) = self.to_sql();
        let rows = executor
            .query(&sql, &params, dialect)
            .await
            .map_err(|e| e.to_string())?;
        let row = rows
            .into_iter()
            .next()
            .ok_or_else(|| "no rows returned".to_string())?;
        <T as crate::schema::FromRow>::from_row(&row)
    }

    /// Execute and decode all rows.
    pub async fn fetch_all<E>(
        self,
        executor: &E,
        dialect: crate::dialect::SqlDialect,
    ) -> Result<Vec<T>, String>
    where
        T: Table + crate::schema::FromRow,
        E: QueryExecutor + Sync,
    {
        let (sql, params) = self.to_sql();
        let rows = executor
            .query(&sql, &params, dialect)
            .await
            .map_err(|e| e.to_string())?;
        rows.into_iter()
            .map(|row| <T as crate::schema::FromRow>::from_row(&row))
            .collect()
    }

    /// Execute and decode at most one row.
    pub async fn fetch_optional<E>(
        self,
        executor: &E,
        dialect: crate::dialect::SqlDialect,
    ) -> Result<Option<T>, String>
    where
        T: Table + crate::schema::FromRow,
        E: QueryExecutor + Sync,
    {
        let (sql, params) = self.to_sql();
        let rows = executor
            .query(&sql, &params, dialect)
            .await
            .map_err(|e| e.to_string())?;
        if let Some(row) = rows.into_iter().next() {
            Ok(Some(<T as crate::schema::FromRow>::from_row(&row)?))
        } else {
            Ok(None)
        }
    }
}

/// Minimal async executor abstraction so users can plug in any driver (sqlx, rusqlite via tokio, etc.).
#[async_trait]
pub trait QueryExecutor {
    type Error: std::fmt::Display + Send + Sync + 'static;

    /// Execute a SQL statement and return rows.
    async fn query(
        &self,
        sql: &str,
        params: &[SqlValue],
        dialect: crate::dialect::SqlDialect,
    ) -> Result<Vec<Row>, Self::Error>;

    /// Execute a statement where only rows-affected is relevant. Defaults to using `query`
    /// and returning the number of rows produced (drivers can override with a real count).
    async fn execute(
        &self,
        sql: &str,
        params: &[SqlValue],
        dialect: crate::dialect::SqlDialect,
    ) -> Result<u64, Self::Error> {
        let rows = self.query(sql, params, dialect).await?;
        Ok(rows.len() as u64)
    }
}

impl<T> UpdateBuilder<T> {
    pub fn new() -> Self {
        Self {
            _marker: std::marker::PhantomData,
            wheres: Vec::new(),
            wheres_in: Vec::new(),
            updates: Vec::new(),
        }
    }

    pub fn add_update_value(mut self, col: &str, val: SqlValue) -> Self {
        self.updates.push((col.to_string(), val));
        self
    }

    pub fn add_where_eq(mut self, col: &str, val: SqlValue) -> Self {
        self.wheres
            .push((col.to_string(), "=".to_string(), Glue::And, val));
        self
    }
    pub fn add_or_where_eq(mut self, col: &str, val: SqlValue) -> Self {
        self.wheres
            .push((col.to_string(), "=".to_string(), Glue::Or, val));
        self
    }
    pub fn add_where_custom(mut self, col: &str, op: &str, val: SqlValue) -> Self {
        self.wheres
            .push((col.to_string(), op.to_string(), Glue::And, val));
        self
    }
    pub fn add_where_in(mut self, col: &str, vals: Vec<SqlValue>) -> Self {
        self.wheres_in.push((col.to_string(), vals, Glue::And));
        self
    }
    pub fn add_or_where_in(mut self, col: &str, vals: Vec<SqlValue>) -> Self {
        self.wheres_in.push((col.to_string(), vals, Glue::Or));
        self
    }

    pub fn to_sql(self) -> (String, Vec<SqlValue>)
    where
        T: Table,
    {
        let mut sql = format!("UPDATE {} SET ", <T as Table>::table_name());
        let mut params: Vec<SqlValue> = Vec::new();

        let set_parts: Vec<String> = self
            .updates
            .iter()
            .map(|(c, _)| format!("{} = ?", c))
            .collect();
        sql.push_str(&set_parts.join(", "));
        for (_c, v) in self.updates.into_iter() {
            params.push(v);
        }

        if !self.wheres.is_empty() || !self.wheres_in.is_empty() {
            let mut parts: Vec<String> = Vec::new();
            let mut first = true;
            for (col, op, glue, _) in &self.wheres {
                let cond = format!("{} {} ?", col, op);
                if first {
                    parts.push(cond);
                    first = false;
                } else {
                    parts.push(format!("{} {}", glue.as_str(), cond));
                }
            }
            for (col, vals, glue) in &self.wheres_in {
                let placeholders = std::iter::repeat_n("?", vals.len())
                    .collect::<Vec<_>>()
                    .join(", ");
                let cond = format!("{} IN ({})", col, placeholders);
                if first {
                    parts.push(cond);
                    first = false;
                } else {
                    parts.push(format!("{} {}", glue.as_str(), cond));
                }
            }
            if !parts.is_empty() {
                sql.push_str(&format!(" WHERE {}", parts.join(" ")));
            }

            for (_c, _op, _g, v) in self.wheres.into_iter() {
                params.push(v);
            }
            for (_c, vals, _g) in self.wheres_in.into_iter() {
                for v in vals {
                    params.push(v);
                }
            }
        }

        (sql, params)
    }

    /// Execute the update and return rows affected.
    pub async fn exec<E>(
        self,
        executor: &E,
        dialect: crate::dialect::SqlDialect,
    ) -> Result<u64, String>
    where
        T: Table,
        E: QueryExecutor + Sync,
    {
        let (sql, params) = self.to_sql();
        executor
            .execute(&sql, &params, dialect)
            .await
            .map_err(|e| e.to_string())
    }
}

impl<T> DeleteBuilder<T> {
    pub fn new() -> Self {
        Self {
            _marker: std::marker::PhantomData,
            wheres: Vec::new(),
            wheres_in: Vec::new(),
            limit: None,
            offset: None,
        }
    }

    pub fn add_where_eq(mut self, col: &str, val: SqlValue) -> Self {
        self.wheres
            .push((col.to_string(), "=".to_string(), Glue::And, val));
        self
    }
    pub fn add_or_where_eq(mut self, col: &str, val: SqlValue) -> Self {
        self.wheres
            .push((col.to_string(), "=".to_string(), Glue::Or, val));
        self
    }
    pub fn add_where_in(mut self, col: &str, vals: Vec<SqlValue>) -> Self {
        self.wheres_in.push((col.to_string(), vals, Glue::And));
        self
    }
    pub fn add_or_where_in(mut self, col: &str, vals: Vec<SqlValue>) -> Self {
        self.wheres_in.push((col.to_string(), vals, Glue::Or));
        self
    }

    pub fn limit(mut self, n: usize) -> Self {
        self.limit = Some(n);
        self
    }
    pub fn offset(mut self, n: usize) -> Self {
        self.offset = Some(n);
        self
    }

    pub fn to_sql(self) -> (String, Vec<SqlValue>)
    where
        T: Table,
    {
        let mut sql = format!("DELETE FROM {}", <T as Table>::table_name());
        let mut params: Vec<SqlValue> = Vec::new();

        if !self.wheres.is_empty() || !self.wheres_in.is_empty() {
            let mut parts: Vec<String> = Vec::new();
            let mut first = true;
            for (col, op, glue, _) in &self.wheres {
                let cond = format!("{} {} ?", col, op);
                if first {
                    parts.push(cond);
                    first = false;
                } else {
                    parts.push(format!("{} {}", glue.as_str(), cond));
                }
            }
            for (col, vals, glue) in &self.wheres_in {
                let placeholders = std::iter::repeat_n("?", vals.len())
                    .collect::<Vec<_>>()
                    .join(", ");
                let cond = format!("{} IN ({})", col, placeholders);
                if first {
                    parts.push(cond);
                    first = false;
                } else {
                    parts.push(format!("{} {}", glue.as_str(), cond));
                }
            }
            if !parts.is_empty() {
                sql.push_str(&format!(" WHERE {}", parts.join(" ")));
            }
            for (_c, _op, _g, v) in self.wheres.into_iter() {
                params.push(v);
            }
            for (_c, vals, _g) in self.wheres_in.into_iter() {
                for v in vals {
                    params.push(v);
                }
            }
        }

        if let Some(l) = self.limit {
            sql.push_str(&format!(" LIMIT {}", l));
        }
        if let Some(o) = self.offset {
            sql.push_str(&format!(" OFFSET {}", o));
        }

        (sql, params)
    }

    /// Execute the delete and return rows affected.
    pub async fn exec<E>(
        self,
        executor: &E,
        dialect: crate::dialect::SqlDialect,
    ) -> Result<u64, String>
    where
        T: Table,
        E: QueryExecutor + Sync,
    {
        let (sql, params) = self.to_sql();
        executor
            .execute(&sql, &params, dialect)
            .await
            .map_err(|e| e.to_string())
    }
}
