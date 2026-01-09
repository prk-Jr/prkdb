use async_trait::async_trait;
use prkdb_core::error::StorageError;
use prkdb_core::storage::StorageAdapter;
use sqlx::sqlite::{SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions, SqliteSynchronous};
use sqlx::{Row, SqlitePool};
use std::str::FromStr;

#[derive(Clone)]
pub struct SqliteAdapter {
    pool: SqlitePool,
}

impl SqliteAdapter {
    pub async fn connect(url: &str) -> Result<Self, StorageError> {
        let opts = SqliteConnectOptions::from_str(url)
            .map_err(|e| StorageError::BackendError(e.to_string()))?
            .create_if_missing(true)
            .journal_mode(SqliteJournalMode::Wal)
            .foreign_keys(true)
            .synchronous(SqliteSynchronous::Normal)
            .busy_timeout(std::time::Duration::from_secs(5));

        let pool = SqlitePoolOptions::new()
            .max_connections(8)
            .connect_with(opts)
            .await
            .map_err(|e| StorageError::BackendError(e.to_string()))?;

        // Initialize schema for KV and outbox
        sqlx::query("CREATE TABLE IF NOT EXISTS kv (k BLOB PRIMARY KEY, v BLOB)")
            .execute(&pool)
            .await
            .map_err(|e| StorageError::BackendError(e.to_string()))?;
        sqlx::query("CREATE TABLE IF NOT EXISTS outbox (id TEXT PRIMARY KEY, payload BLOB)")
            .execute(&pool)
            .await
            .map_err(|e| StorageError::BackendError(e.to_string()))?;
        sqlx::query("CREATE INDEX IF NOT EXISTS idx_kv_k ON kv(k)")
            .execute(&pool)
            .await
            .ok();
        sqlx::query("CREATE INDEX IF NOT EXISTS idx_outbox_id ON outbox(id)")
            .execute(&pool)
            .await
            .ok();
        Ok(Self { pool })
    }
}

#[async_trait]
impl StorageAdapter for SqliteAdapter {
    async fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, StorageError> {
        let row = sqlx::query("SELECT v FROM kv WHERE k = ?")
            .bind(key)
            .fetch_optional(&self.pool)
            .await
            .map_err(|e| StorageError::BackendError(e.to_string()))?;
        Ok(row.map(|r| r.get::<Vec<u8>, _>(0)))
    }

    async fn put(&self, key: &[u8], value: &[u8]) -> Result<(), StorageError> {
        sqlx::query(
            "INSERT INTO kv(k, v) VALUES(?, ?) ON CONFLICT(k) DO UPDATE SET v = excluded.v",
        )
        .bind(key)
        .bind(value)
        .execute(&self.pool)
        .await
        .map_err(|e| StorageError::BackendError(e.to_string()))?;
        Ok(())
    }

    async fn delete(&self, key: &[u8]) -> Result<(), StorageError> {
        sqlx::query("DELETE FROM kv WHERE k = ?")
            .bind(key)
            .execute(&self.pool)
            .await
            .map_err(|e| StorageError::BackendError(e.to_string()))?;
        Ok(())
    }

    async fn outbox_save(&self, id: &str, payload: &[u8]) -> Result<(), StorageError> {
        sqlx::query("INSERT INTO outbox(id, payload) VALUES(?, ?) ON CONFLICT(id) DO UPDATE SET payload = excluded.payload")
            .bind(id)
            .bind(payload)
            .execute(&self.pool)
            .await
            .map_err(|e| StorageError::BackendError(e.to_string()))?;
        Ok(())
    }

    async fn outbox_list(&self) -> Result<Vec<(String, Vec<u8>)>, StorageError> {
        let rows = sqlx::query("SELECT id, payload FROM outbox ORDER BY id")
            .fetch_all(&self.pool)
            .await
            .map_err(|e| StorageError::BackendError(e.to_string()))?;
        Ok(rows
            .into_iter()
            .map(|r| (r.get::<String, _>(0), r.get::<Vec<u8>, _>(1)))
            .collect())
    }

    async fn outbox_remove(&self, id: &str) -> Result<(), StorageError> {
        sqlx::query("DELETE FROM outbox WHERE id = ?")
            .bind(id)
            .execute(&self.pool)
            .await
            .map_err(|e| StorageError::BackendError(e.to_string()))?;
        Ok(())
    }

    async fn put_with_outbox(
        &self,
        key: &[u8],
        value: &[u8],
        outbox_id: &str,
        outbox_payload: &[u8],
    ) -> Result<(), StorageError> {
        let mut tx = self
            .pool
            .begin()
            .await
            .map_err(|e| StorageError::BackendError(e.to_string()))?;

        sqlx::query(
            "INSERT INTO kv(k, v) VALUES(?, ?) ON CONFLICT(k) DO UPDATE SET v = excluded.v",
        )
        .bind(key)
        .bind(value)
        .execute(&mut *tx)
        .await
        .map_err(|e| StorageError::BackendError(e.to_string()))?;

        sqlx::query("INSERT INTO outbox(id, payload) VALUES(?, ?) ON CONFLICT(id) DO UPDATE SET payload = excluded.payload")
            .bind(outbox_id)
            .bind(outbox_payload)
            .execute(&mut *tx)
            .await
            .map_err(|e| StorageError::BackendError(e.to_string()))?;

        tx.commit()
            .await
            .map_err(|e| StorageError::BackendError(e.to_string()))?;
        Ok(())
    }

    async fn delete_with_outbox(
        &self,
        key: &[u8],
        outbox_id: &str,
        outbox_payload: &[u8],
    ) -> Result<(), StorageError> {
        let mut tx = self
            .pool
            .begin()
            .await
            .map_err(|e| StorageError::BackendError(e.to_string()))?;

        sqlx::query("DELETE FROM kv WHERE k = ?")
            .bind(key)
            .execute(&mut *tx)
            .await
            .map_err(|e| StorageError::BackendError(e.to_string()))?;

        sqlx::query("INSERT INTO outbox(id, payload) VALUES(?, ?) ON CONFLICT(id) DO UPDATE SET payload = excluded.payload")
            .bind(outbox_id)
            .bind(outbox_payload)
            .execute(&mut *tx)
            .await
            .map_err(|e| StorageError::BackendError(e.to_string()))?;

        tx.commit()
            .await
            .map_err(|e| StorageError::BackendError(e.to_string()))?;
        Ok(())
    }

    async fn scan_prefix(&self, prefix: &[u8]) -> Result<Vec<(Vec<u8>, Vec<u8>)>, StorageError> {
        let rows = sqlx::query("SELECT k, v FROM kv ORDER BY k")
            .fetch_all(&self.pool)
            .await
            .map_err(|e| StorageError::BackendError(e.to_string()))?;
        let mut out = Vec::with_capacity(rows.len());
        for row in rows {
            let k: Vec<u8> = row.get(0);
            let v: Vec<u8> = row.get(1);
            if prefix.is_empty() || k.starts_with(prefix) {
                out.push((k, v));
            }
        }
        Ok(out)
    }

    async fn scan_range(
        &self,
        start: &[u8],
        end: &[u8],
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>, StorageError> {
        let rows = sqlx::query("SELECT k, v FROM kv ORDER BY k")
            .fetch_all(&self.pool)
            .await
            .map_err(|e| StorageError::BackendError(e.to_string()))?;
        let mut out = Vec::with_capacity(rows.len());
        for row in rows {
            let k: Vec<u8> = row.get(0);
            let v: Vec<u8> = row.get(1);
            if k.as_slice() >= start && k.as_slice() < end {
                out.push((k, v));
            }
        }
        Ok(out)
    }

    async fn migrate_table(&self, ddl: &str) -> Result<(), StorageError> {
        sqlx::query(ddl)
            .execute(&self.pool)
            .await
            .map_err(|e| StorageError::BackendError(e.to_string()))?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::SqliteAdapter;
    use prkdb_core::storage::StorageAdapter;
    use sqlx::Row;

    #[tokio::test]
    async fn sqlite_put_get_outbox() {
        let adapter = SqliteAdapter::connect("sqlite::memory:").await.unwrap();
        adapter.put(b"k", b"v").await.unwrap();
        assert_eq!(adapter.get(b"k").await.unwrap().unwrap(), b"v");
        adapter.outbox_save("e1", b"p").await.unwrap();
        let items = adapter.outbox_list().await.unwrap();
        assert_eq!(items.len(), 1);
        adapter.outbox_remove("e1").await.unwrap();
        assert!(adapter.outbox_list().await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn sqlite_migrate_table_executes_ddl() {
        let adapter = SqliteAdapter::connect("sqlite::memory:").await.unwrap();
        adapter
            .migrate_table("CREATE TABLE IF NOT EXISTS items (id INTEGER PRIMARY KEY, name TEXT)")
            .await
            .unwrap();

        let row = sqlx::query("SELECT name FROM sqlite_master WHERE type='table' AND name='items'")
            .fetch_one(&adapter.pool)
            .await
            .unwrap();
        assert_eq!(row.get::<String, _>(0), "items");
    }
}
