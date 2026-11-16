use crate::api::{Cursor, Story};
use anyhow::{Result, anyhow};
use rusqlite::{Connection, OptionalExtension, params};
use serde::{Serialize, de::DeserializeOwned};
use std::{path::Path, sync::Mutex};

pub struct Store(Mutex<Connection>);

impl Store {
    pub fn open(path: &Path) -> Result<Self> {
        let db = Connection::open(path)?;
        db.busy_timeout(std::time::Duration::from_secs(10))?;
        db.execute_batch(
            "PRAGMA journal_mode=WAL;
            CREATE TABLE IF NOT EXISTS state (key TEXT PRIMARY KEY, value TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS outbox (
                id INTEGER PRIMARY KEY, published INTEGER NOT NULL, payload TEXT NOT NULL,
                sent_at INTEGER, telegram_id INTEGER
            );",
        )?;
        Ok(Self(Mutex::new(db)))
    }

    pub fn get<T: DeserializeOwned>(&self, key: &str) -> Result<Option<T>> {
        let db = self.0.lock().map_err(|_| anyhow!("Database lock failed"))?;
        let value: Option<String> = db
            .query_row("SELECT value FROM state WHERE key=?1", [key], |r| r.get(0))
            .optional()?;
        value
            .map(|v| serde_json::from_str(&v).map_err(Into::into))
            .transpose()
    }

    pub fn set(&self, key: &str, value: &impl Serialize) -> Result<()> {
        let db = self.0.lock().map_err(|_| anyhow!("Database lock failed"))?;
        db.execute(
            "INSERT OR REPLACE INTO state VALUES (?1,?2)",
            params![key, serde_json::to_string(value)?],
        )?;
        Ok(())
    }

    pub fn commit(&self, items: &[Story], cursor: Option<Cursor>) -> Result<()> {
        let mut db = self.0.lock().map_err(|_| anyhow!("Database lock failed"))?;
        let tx = db.transaction()?;
        for item in items {
            tx.execute(
                "INSERT OR IGNORE INTO outbox (id,published,payload) VALUES (?1,?2,?3)",
                params![item.no, item.cursor()?.0, serde_json::to_string(item)?],
            )?;
        }
        if let Some(cursor) = cursor {
            tx.execute(
                "INSERT OR REPLACE INTO state VALUES ('cursor',?1)",
                [serde_json::to_string(&cursor)?],
            )?;
        }
        tx.execute(
            "DELETE FROM outbox WHERE sent_at < unixepoch() - 2592000",
            [],
        )?;
        tx.commit()?;
        Ok(())
    }

    pub fn pending(&self) -> Result<Option<Story>> {
        let db = self.0.lock().map_err(|_| anyhow!("Database lock failed"))?;
        let value: Option<String> = db
            .query_row(
                "SELECT payload FROM outbox WHERE sent_at IS NULL ORDER BY published,id LIMIT 1",
                [],
                |r| r.get(0),
            )
            .optional()?;
        value
            .map(|v| serde_json::from_str(&v).map_err(Into::into))
            .transpose()
    }

    pub fn acknowledge(&self, id: i64, telegram_id: i64) -> Result<()> {
        let db = self.0.lock().map_err(|_| anyhow!("Database lock failed"))?;
        db.execute(
            "UPDATE outbox SET sent_at=unixepoch(),telegram_id=?2 WHERE id=?1",
            params![id, telegram_id],
        )?;
        Ok(())
    }

    pub fn counts(&self) -> Result<(i64, i64)> {
        let db = self.0.lock().map_err(|_| anyhow!("Database lock failed"))?;
        Ok(db.query_row("SELECT count(*) FILTER(WHERE sent_at IS NULL), count(*) FILTER(WHERE sent_at IS NOT NULL) FROM outbox",
            [], |r| Ok((r.get(0)?, r.get(1)?)))?)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    pub fn story(id: i64) -> Story {
        serde_json::from_value(json!({"no": id, "time": "2026-11-17T00:00:00Z",
            "channels": ["main"], "zh": "test <tag>", "source": "test", "url": "https://eqn.news/main/1"})).unwrap()
    }

    #[test]
    fn deduplication_and_delivery_persist_across_restarts() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("state.db");
        let store = Store::open(&path).unwrap();
        store
            .commit(&[story(2), story(1), story(1)], Some(Cursor(0, 2)))
            .unwrap();
        assert_eq!(store.counts().unwrap(), (2, 0));
        assert_eq!(store.pending().unwrap().unwrap().no, 1);
        store.acknowledge(1, 101).unwrap();
        store.set("paused", &true).unwrap();
        drop(store);
        let store = Store::open(&path).unwrap();
        assert_eq!(store.counts().unwrap(), (1, 1));
        assert_eq!(store.pending().unwrap().unwrap().no, 2);
        assert_eq!(store.get::<bool>("paused").unwrap(), Some(true));
    }

    #[test]
    fn failed_transaction_does_not_advance_cursor() {
        let store = Store::open(Path::new(":memory:")).unwrap();
        store.set("cursor", &Cursor(0, 1)).unwrap();
        let mut invalid = story(3);
        invalid.time = "invalid".into();
        assert!(
            store
                .commit(&[story(2), invalid], Some(Cursor(0, 3)))
                .is_err()
        );
        assert_eq!(store.counts().unwrap(), (0, 0));
        assert_eq!(store.get::<Cursor>("cursor").unwrap(), Some(Cursor(0, 1)));
    }

    #[test]
    fn formatting_escapes_untrusted_html() {
        let formatted = story(1).formatted().unwrap();
        assert!(formatted.contains("test &lt;tag&gt;"));
        assert!(formatted.contains("07:00 UTC+7"));
    }
}
