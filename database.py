import sqlite3
import uuid
from datetime import datetime, timedelta

DB_PATH = "cougconnect.db"


def init_db():
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS member_links (
                discord_id   TEXT PRIMARY KEY,
                mp_member_id INTEGER,
                mp_email     TEXT,
                tier         TEXT,
                linked_at    TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                last_synced  TIMESTAMP
            )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS verify_tokens (
                token       TEXT PRIMARY KEY,
                discord_id  TEXT,
                expires_at  TIMESTAMP,
                used        INTEGER DEFAULT 0
            )
        """)
        conn.commit()


# ── Tokens ────────────────────────────────────────────────────────────────────

def create_token(discord_id: str) -> str:
    token = str(uuid.uuid4())
    expires_at = datetime.utcnow() + timedelta(minutes=15)
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute(
            "INSERT INTO verify_tokens (token, discord_id, expires_at) VALUES (?, ?, ?)",
            (token, discord_id, expires_at.isoformat()),
        )
        conn.commit()
    return token


def consume_token(token: str) -> str | None:
    """Validate and consume a token. Returns discord_id if valid, else None."""
    with sqlite3.connect(DB_PATH) as conn:
        row = conn.execute(
            "SELECT discord_id, expires_at, used FROM verify_tokens WHERE token = ?",
            (token,),
        ).fetchone()
        if not row:
            return None
        discord_id, expires_at, used = row
        if used:
            return None
        if datetime.utcnow() > datetime.fromisoformat(expires_at):
            return None
        conn.execute("UPDATE verify_tokens SET used = 1 WHERE token = ?", (token,))
        conn.commit()
    return discord_id


def cleanup_expired_tokens():
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute(
            "DELETE FROM verify_tokens WHERE expires_at < ? OR used = 1",
            (datetime.utcnow().isoformat(),),
        )
        conn.commit()


# ── Member links ──────────────────────────────────────────────────────────────

def upsert_member(discord_id: str, mp_member_id: int, mp_email: str, tier: str):
    now = datetime.utcnow().isoformat()
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute("""
            INSERT INTO member_links (discord_id, mp_member_id, mp_email, tier, linked_at, last_synced)
            VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT(discord_id) DO UPDATE SET
                mp_member_id = excluded.mp_member_id,
                mp_email     = excluded.mp_email,
                tier         = excluded.tier,
                last_synced  = excluded.last_synced
        """, (discord_id, mp_member_id, mp_email, tier, now, now))
        conn.commit()


def get_member_by_discord(discord_id: str) -> dict | None:
    with sqlite3.connect(DB_PATH) as conn:
        row = conn.execute(
            "SELECT discord_id, mp_member_id, mp_email, tier, linked_at, last_synced "
            "FROM member_links WHERE discord_id = ?",
            (discord_id,),
        ).fetchone()
    if not row:
        return None
    return dict(zip(["discord_id", "mp_member_id", "mp_email", "tier", "linked_at", "last_synced"], row))


def get_member_by_email(email: str) -> dict | None:
    with sqlite3.connect(DB_PATH) as conn:
        row = conn.execute(
            "SELECT discord_id, mp_member_id, mp_email, tier, linked_at, last_synced "
            "FROM member_links WHERE LOWER(mp_email) = LOWER(?)",
            (email,),
        ).fetchone()
    if not row:
        return None
    return dict(zip(["discord_id", "mp_member_id", "mp_email", "tier", "linked_at", "last_synced"], row))


def get_member_by_mp_id(mp_member_id: int) -> dict | None:
    with sqlite3.connect(DB_PATH) as conn:
        row = conn.execute(
            "SELECT discord_id, mp_member_id, mp_email, tier, linked_at, last_synced "
            "FROM member_links WHERE mp_member_id = ?",
            (mp_member_id,),
        ).fetchone()
    if not row:
        return None
    return dict(zip(["discord_id", "mp_member_id", "mp_email", "tier", "linked_at", "last_synced"], row))


def remove_member(discord_id: str):
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute("DELETE FROM member_links WHERE discord_id = ?", (discord_id,))
        conn.commit()


def get_stats() -> dict:
    with sqlite3.connect(DB_PATH) as conn:
        rows = conn.execute(
            "SELECT tier, COUNT(*) FROM member_links GROUP BY tier"
        ).fetchall()
    counts = {tier: count for tier, count in rows}
    total = sum(counts.values())
    return {
        "total": total,
        "gold": counts.get("gold", 0),
        "silver": counts.get("silver", 0),
        "insider": counts.get("insider", 0),
        "unsubscribed": counts.get("unsubscribed", 0),
    }
