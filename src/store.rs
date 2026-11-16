use crate::api::{Cursor, Story};
use anyhow::{Result, anyhow};
use rusqlite::{Connection, OptionalExtension, params};
use serde::{Serialize, de::DeserializeOwned};
use std::{path::Path, sync::Mutex};

pub struct Store(Mutex<Connection>);

impl Store {
    pub fn open(path: &Path) -> Result<Self> {
        let mut db = Connection::open(path)?;
        db.busy_timeout(std::time::Duration::from_secs(10))?;
        db.execute_batch(
            "PRAGMA journal_mode=WAL;
            CREATE TABLE IF NOT EXISTS state (key TEXT PRIMARY KEY, value TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS outbox (
                id INTEGER PRIMARY KEY, published INTEGER NOT NULL, payload TEXT NOT NULL,
                sent_at INTEGER, telegram_id INTEGER
            );
            CREATE TABLE IF NOT EXISTS subscribers (
                chat_id INTEGER PRIMARY KEY, paused INTEGER NOT NULL DEFAULT 0
            );
            CREATE TABLE IF NOT EXISTS invitations (
                username TEXT PRIMARY KEY, chat_id INTEGER UNIQUE
            );
            CREATE TABLE IF NOT EXISTS recipient_deliveries (
                chat_id INTEGER NOT NULL, story_id INTEGER NOT NULL,
                sent_at INTEGER, telegram_id INTEGER,
                next_attempt INTEGER NOT NULL DEFAULT 0,
                PRIMARY KEY (chat_id, story_id)
            );",
        )?;
        let tx = db.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
        if tx.pragma_query_value(None, "user_version", |r| r.get::<_, i64>(0))? == 0 {
            tx.execute_batch("ALTER TABLE recipient_deliveries ADD COLUMN queued_ms INTEGER;
                ALTER TABLE recipient_deliveries ADD COLUMN ack_ms INTEGER;
                ALTER TABLE recipient_deliveries ADD COLUMN attempts INTEGER NOT NULL DEFAULT 0;
                ALTER TABLE recipient_deliveries ADD COLUMN failed_at INTEGER;
                CREATE INDEX delivery_due ON recipient_deliveries(next_attempt,story_id,chat_id) WHERE sent_at IS NULL AND failed_at IS NULL;
                CREATE INDEX delivery_story ON recipient_deliveries(story_id);
                CREATE INDEX delivery_ack ON recipient_deliveries(chat_id,ack_ms DESC) WHERE ack_ms IS NOT NULL;
                PRAGMA user_version=1;")?;
        }
        tx.commit()?;
        Ok(Self(Mutex::new(db)))
    }

    pub fn configure_recipients(&self, owner: i64, usernames: &[String]) -> Result<()> {
        let mut db = self.0.lock().map_err(|_| anyhow!("Database lock failed"))?;
        let tx = db.transaction()?;
        let migrated: bool = tx.query_row(
            "SELECT EXISTS(SELECT 1 FROM state WHERE key='multiuser_v1')",
            [],
            |r| r.get(0),
        )?;
        if !migrated {
            let paused: Option<String> = tx
                .query_row("SELECT value FROM state WHERE key='paused'", [], |r| {
                    r.get(0)
                })
                .optional()?;
            let paused = paused.as_deref() == Some("true");
            tx.execute(
                "INSERT OR IGNORE INTO subscribers VALUES (?1,?2)",
                params![owner, paused],
            )?;
            tx.execute(
                "INSERT OR IGNORE INTO recipient_deliveries (chat_id,story_id,sent_at,telegram_id)
                SELECT ?1,id,sent_at,telegram_id FROM outbox",
                [owner],
            )?;
            tx.execute("INSERT INTO state VALUES ('multiuser_v1','true')", [])?;
        }
        for name in usernames {
            let name = name.trim_start_matches('@').to_ascii_lowercase();
            tx.execute(
                "INSERT OR IGNORE INTO invitations (username) VALUES (?1)",
                [name],
            )?;
        }
        tx.commit()?;
        Ok(())
    }

    pub fn authorize(&self, chat_id: i64, username: Option<&str>) -> Result<bool> {
        let mut db = self.0.lock().map_err(|_| anyhow!("Database lock failed"))?;
        let tx = db.transaction()?;
        let known: bool = tx.query_row(
            "SELECT EXISTS(SELECT 1 FROM subscribers WHERE chat_id=?1)",
            [chat_id],
            |r| r.get(0),
        )?;
        if known {
            return Ok(true);
        }
        let Some(username) = username else {
            return Ok(false);
        };
        let claimed = tx.execute(
            "UPDATE invitations SET chat_id=?1 WHERE username=?2 AND chat_id IS NULL",
            params![chat_id, username.to_ascii_lowercase()],
        )?;
        if claimed == 0 {
            return Ok(false);
        }
        tx.execute("INSERT INTO subscribers (chat_id) VALUES (?1)", [chat_id])?;
        tx.commit()?;
        Ok(true)
    }

    pub fn paused(&self, chat_id: i64) -> Result<bool> {
        let db = self.0.lock().map_err(|_| anyhow!("Database lock failed"))?;
        Ok(db.query_row(
            "SELECT paused FROM subscribers WHERE chat_id=?1",
            [chat_id],
            |r| r.get(0),
        )?)
    }

    pub fn set_paused(&self, chat_id: i64, paused: bool) -> Result<()> {
        let db = self.0.lock().map_err(|_| anyhow!("Database lock failed"))?;
        db.execute(
            "UPDATE subscribers SET paused=?2 WHERE chat_id=?1",
            params![chat_id, paused],
        )?;
        Ok(())
    }

    pub fn invitations(&self) -> Result<Vec<(String, bool)>> {
        let db = self.0.lock().map_err(|_| anyhow!("Database lock failed"))?;
        let mut query =
            db.prepare("SELECT username,chat_id IS NOT NULL FROM invitations ORDER BY username")?;
        Ok(query
            .query_map([], |r| Ok((r.get(0)?, r.get(1)?)))?
            .collect::<std::result::Result<_, _>>()?)
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

    pub fn commit(&self, items: &[Story], cursor: Option<Cursor>) -> Result<usize> {
        let mut db = self.0.lock().map_err(|_| anyhow!("Database lock failed"))?;
        let tx = db.transaction()?;
        let mut rows = 0;
        for item in items {
            let inserted = tx.execute(
                "INSERT OR IGNORE INTO outbox (id,published,payload) VALUES (?1,?2,?3)",
                params![item.no, item.cursor()?.0, serde_json::to_string(item)?],
            )?;
            if inserted > 0 {
                rows += 1;
                tx.execute("INSERT INTO recipient_deliveries (chat_id,story_id,queued_ms) SELECT chat_id,?1,?2 FROM subscribers", params![item.no,chrono::Utc::now().timestamp_millis()])?;
            }
        }
        if let Some(cursor) = cursor {
            tx.execute(
                "INSERT OR REPLACE INTO state VALUES ('cursor',?1)",
                [serde_json::to_string(&cursor)?],
            )?;
        }
        tx.commit()?;
        Ok(rows)
    }

    pub fn prune(&self) -> Result<()> {
        let mut db = self.0.lock().map_err(|_| anyhow!("Database lock failed"))?;
        let tx = db.transaction()?;
        tx.execute(
            "DELETE FROM recipient_deliveries WHERE sent_at < unixepoch() - 2592000",
            [],
        )?;
        tx.execute("DELETE FROM outbox WHERE NOT EXISTS (SELECT 1 FROM recipient_deliveries WHERE story_id=outbox.id)", [])?;
        tx.commit()?;
        Ok(())
    }

    pub fn pending(&self, busy: &[i64]) -> Result<Option<(i64, Story)>> {
        let db = self.0.lock().map_err(|_| anyhow!("Database lock failed"))?;
        let value: Option<(i64, String)> = db
            .query_row(
                "SELECT d.chat_id,o.payload FROM recipient_deliveries d
                 JOIN outbox o ON o.id=d.story_id JOIN subscribers s ON s.chat_id=d.chat_id
                 WHERE d.sent_at IS NULL AND d.failed_at IS NULL AND s.paused=0 AND d.next_attempt<=unixepoch()
                 AND d.chat_id NOT IN (SELECT value FROM json_each(?1))
                 ORDER BY d.next_attempt,o.published,o.id,d.chat_id LIMIT 1",
                [serde_json::to_string(busy)?],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .optional()?;
        value
            .map(|(id, v)| Ok((id, serde_json::from_str(&v)?)))
            .transpose()
    }

    pub fn acknowledge(&self, chat_id: i64, id: i64, telegram_id: i64) -> Result<()> {
        let db = self.0.lock().map_err(|_| anyhow!("Database lock failed"))?;
        db.execute(
            "UPDATE recipient_deliveries SET sent_at=unixepoch(),telegram_id=?3,ack_ms=?4 WHERE story_id=?2 AND chat_id=?1",
            params![chat_id, id, telegram_id,chrono::Utc::now().timestamp_millis()],
        )?;
        Ok(())
    }

    pub fn defer(&self, chat_id: i64, id: i64, retry_after: Option<u64>) -> Result<()> {
        let db = self.0.lock().map_err(|_| anyhow!("Database lock failed"))?;
        db.execute("UPDATE recipient_deliveries SET attempts=attempts+1,next_attempt=unixepoch()+coalesce(?3,min(300,2 << min(attempts,8))) WHERE chat_id=?1 AND story_id=?2", params![chat_id,id,retry_after.map(|v| v.clamp(1,86400))])?;
        Ok(())
    }

    pub fn quarantine(&self, chat_id: i64, story: i64) -> Result<()> {
        let db = self.0.lock().map_err(|_| anyhow!("Database lock failed"))?;
        db.execute("UPDATE recipient_deliveries SET failed_at=unixepoch() WHERE chat_id=?1 AND story_id=?2",params![chat_id,story])?;
        Ok(())
    }

    pub fn retry_failed(&self, chat_id: i64) -> Result<usize> {
        let db = self.0.lock().map_err(|_| anyhow!("Database lock failed"))?;
        Ok(db.execute("UPDATE recipient_deliveries SET failed_at=NULL,next_attempt=0,attempts=0 WHERE chat_id=?1 AND failed_at IS NOT NULL",[chat_id])?)
    }

    pub fn recent_latency(&self, chat_id: i64) -> Result<Option<(i64, i64)>> {
        let db = self.0.lock().map_err(|_| anyhow!("Database lock failed"))?;
        Ok(db
            .query_row(
                "SELECT max(0,d.ack_ms-d.queued_ms),max(0,d.ack_ms-o.published)
            FROM recipient_deliveries d JOIN outbox o ON o.id=d.story_id
            WHERE d.chat_id=?1 AND d.ack_ms IS NOT NULL AND d.queued_ms IS NOT NULL
            ORDER BY d.ack_ms DESC LIMIT 1",
                [chat_id],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .optional()?)
    }

    pub fn counts(&self, chat_id: i64) -> Result<(i64, i64, i64)> {
        let db = self.0.lock().map_err(|_| anyhow!("Database lock failed"))?;
        Ok(db.query_row("SELECT count(*) FILTER(WHERE sent_at IS NULL AND failed_at IS NULL), count(*) FILTER(WHERE sent_at >= unixepoch()-2592000), count(*) FILTER(WHERE failed_at IS NOT NULL) FROM recipient_deliveries WHERE chat_id=?1",
            [chat_id], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)))?)
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
        store.configure_recipients(100, &[]).unwrap();
        store
            .commit(&[story(2), story(1), story(1)], Some(Cursor(0, 2)))
            .unwrap();
        assert_eq!(store.counts(100).unwrap(), (2, 0, 0));
        assert_eq!(store.pending(&[]).unwrap().unwrap().1.no, 1);
        store.acknowledge(100, 1, 101).unwrap();
        store.set_paused(100, true).unwrap();
        drop(store);
        let store = Store::open(&path).unwrap();
        store.configure_recipients(100, &[]).unwrap();
        assert_eq!(store.counts(100).unwrap(), (1, 1, 0));
        assert!(store.pending(&[]).unwrap().is_none());
        store.set_paused(100, false).unwrap();
        assert_eq!(store.pending(&[]).unwrap().unwrap().1.no, 2);
        assert!(!store.paused(100).unwrap());
    }

    #[test]
    fn failed_transaction_does_not_advance_cursor() {
        let store = Store::open(Path::new(":memory:")).unwrap();
        store.configure_recipients(100, &[]).unwrap();
        store.set("cursor", &Cursor(0, 1)).unwrap();
        let mut invalid = story(3);
        invalid.time = "invalid".into();
        assert!(
            store
                .commit(&[story(2), invalid], Some(Cursor(0, 3)))
                .is_err()
        );
        assert_eq!(store.counts(100).unwrap(), (0, 0, 0));
        assert_eq!(store.get::<Cursor>("cursor").unwrap(), Some(Cursor(0, 1)));
    }

    #[test]
    fn formatting_escapes_untrusted_html() {
        let formatted = story(1).formatted().unwrap();
        assert!(formatted.contains("test &lt;tag&gt;"));
        assert!(formatted.contains("07:00:00 UTC+7"));
    }

    #[test]
    fn username_invitation_binds_once_to_the_authenticated_id() {
        let store = Store::open(Path::new(":memory:")).unwrap();
        store
            .configure_recipients(100, &["Guest_User".into()])
            .unwrap();
        assert!(!store.authorize(200, None).unwrap());
        assert!(!store.authorize(200, Some("someone_else")).unwrap());
        assert!(store.authorize(200, Some("guest_USER")).unwrap());
        assert!(store.authorize(200, Some("renamed_account")).unwrap());
        assert!(!store.authorize(300, Some("guest_user")).unwrap());
        assert_eq!(
            store.invitations().unwrap(),
            vec![("guest_user".into(), true)]
        );
    }

    #[test]
    fn recipients_have_independent_delivery_pause_and_retry_state() {
        let store = Store::open(Path::new(":memory:")).unwrap();
        store
            .configure_recipients(100, &["guest_user".into()])
            .unwrap();
        store.commit(&[story(1)], None).unwrap();
        assert!(store.authorize(200, Some("guest_user")).unwrap());
        assert_eq!(store.counts(200).unwrap(), (0, 0, 0));
        store.commit(&[story(1), story(2)], None).unwrap();
        assert_eq!(store.counts(100).unwrap(), (2, 0, 0));
        assert_eq!(store.counts(200).unwrap(), (1, 0, 0));
        store.set_paused(100, true).unwrap();
        assert!(!store.paused(200).unwrap());
        assert_eq!(store.pending(&[]).unwrap().unwrap().0, 200);
        store.defer(200, 2, None).unwrap();
        store.set_paused(100, false).unwrap();
        assert_eq!(store.pending(&[]).unwrap().unwrap().0, 100);
        store.acknowledge(100, 2, 123).unwrap();
        assert_eq!(store.counts(100).unwrap(), (1, 1, 0));
        assert_eq!(store.counts(200).unwrap(), (1, 0, 0));
    }

    #[test]
    fn concurrent_lanes_and_retry_state_survive_restart() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("delivery.db");
        let store = Store::open(&path).unwrap();
        store.configure_recipients(100, &["reader".into()]).unwrap();
        store.authorize(200, Some("reader")).unwrap();
        store.commit(&[story(1), story(2)], None).unwrap();
        assert_eq!(store.pending(&[100]).unwrap().unwrap().0, 200);
        assert!(store.pending(&[100, 200]).unwrap().is_none());
        store.defer(200, 1, Some(120)).unwrap();
        store.quarantine(100, 1).unwrap();
        drop(store);
        let store = Store::open(&path).unwrap();
        assert_eq!(store.counts(100).unwrap(), (1, 0, 1));
        assert_eq!(store.pending(&[100]).unwrap().unwrap().1.no, 2);
        assert_eq!(store.pending(&[200]).unwrap().unwrap().1.no, 2);
        assert_eq!(store.retry_failed(200).unwrap(), 0);
        assert_eq!(store.retry_failed(100).unwrap(), 1);
        store.acknowledge(100, 1, 99).unwrap();
        assert!(store.recent_latency(100).unwrap().is_some());
        assert!(store.recent_latency(200).unwrap().is_none());
    }

    #[test]
    fn late_indexed_story_is_delivered_once_without_rewinding_cursor() {
        let store = Store::open(Path::new(":memory:")).unwrap();
        store.configure_recipients(100, &[]).unwrap();
        let high = story(3);
        let cursor = high.cursor().unwrap();
        store
            .commit(std::slice::from_ref(&high), Some(cursor))
            .unwrap();
        assert_eq!(store.commit(&[story(2), high.clone()], None).unwrap(), 1);
        assert_eq!(store.commit(&[story(2), high], None).unwrap(), 0);
        assert_eq!(store.get::<Cursor>("cursor").unwrap(), Some(cursor));
        assert_eq!(store.counts(100).unwrap(), (2, 0, 0));
    }

    #[test]
    fn legacy_owner_history_and_pause_are_migrated_once() {
        let store = Store::open(Path::new(":memory:")).unwrap();
        {
            let db = store.0.lock().unwrap();
            db.execute(
                "INSERT INTO outbox VALUES (1,0,?1,unixepoch(),123)",
                [serde_json::to_string(&story(1)).unwrap()],
            )
            .unwrap();
            db.execute(
                "INSERT INTO outbox VALUES (2,0,?1,NULL,NULL)",
                [serde_json::to_string(&story(2)).unwrap()],
            )
            .unwrap();
        }
        store.set("paused", &true).unwrap();
        store.configure_recipients(100, &[]).unwrap();
        assert!(store.paused(100).unwrap());
        assert_eq!(store.counts(100).unwrap(), (1, 1, 0));
        store.acknowledge(100, 2, 124).unwrap();
        store.set_paused(100, false).unwrap();
        store.configure_recipients(100, &[]).unwrap();
        assert!(!store.paused(100).unwrap());
        assert_eq!(store.counts(100).unwrap(), (0, 2, 0));
    }
}
