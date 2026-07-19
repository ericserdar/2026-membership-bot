import os
import sqlite3
import uuid
from datetime import datetime, timedelta

DB_PATH = os.environ.get("DB_PATH", "/data/cougconnect.db")


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
        conn.execute("""
            CREATE TABLE IF NOT EXISTS tier_changes (
                id           INTEGER PRIMARY KEY AUTOINCREMENT,
                discord_id   TEXT,
                mp_email     TEXT,
                old_tier     TEXT,
                new_tier     TEXT,
                changed_at   TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                reason       TEXT
            )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS unlinked_members (
                mp_member_id INTEGER PRIMARY KEY,
                first_seen   TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                last_seen    TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS expiry_notices (
                discord_id  TEXT,
                expires_at  TEXT,
                notified_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (discord_id, expires_at)
            )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS winback_notices (
                discord_id  TEXT,
                changed_at  TEXT,
                notified_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (discord_id, changed_at)
            )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS milestone_notices (
                discord_id  TEXT,
                years       INTEGER,
                notified_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (discord_id, years)
            )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS stats_snapshots (
                snapshot_date TEXT PRIMARY KEY,
                gold          INTEGER,
                silver        INTEGER,
                insider       INTEGER,
                unsubscribed  INTEGER,
                total         INTEGER
            )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS upgrade_nudges (
                discord_id  TEXT PRIMARY KEY,
                notified_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS flagged_messages (
                id             INTEGER PRIMARY KEY AUTOINCREMENT,
                message_id     TEXT,
                channel_id     TEXT,
                channel_name   TEXT,
                author_id      TEXT,
                author_name    TEXT,
                content        TEXT,
                flagger_id     TEXT,
                flagger_name   TEXT,
                reason         TEXT,
                flagged_at     TIMESTAMP DEFAULT CURRENT_TIMESTAMP
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

def log_tier_change(discord_id: str, mp_email: str, old_tier: str, new_tier: str, reason: str = "sync"):
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute(
            "INSERT INTO tier_changes (discord_id, mp_email, old_tier, new_tier, reason) VALUES (?, ?, ?, ?, ?)",
            (discord_id, mp_email, old_tier, new_tier, reason),
        )
        conn.commit()


def get_tier_changes_since(hours: int = 24) -> list[dict]:
    cutoff = (datetime.utcnow() - timedelta(hours=hours)).isoformat()
    with sqlite3.connect(DB_PATH) as conn:
        rows = conn.execute(
            "SELECT discord_id, mp_email, old_tier, new_tier, changed_at, reason "
            "FROM tier_changes WHERE changed_at >= ? ORDER BY changed_at DESC",
            (cutoff,),
        ).fetchall()
    return [dict(zip(["discord_id", "mp_email", "old_tier", "new_tier", "changed_at", "reason"], row)) for row in rows]


def get_tier_changes(limit: int = 50) -> list[dict]:
    with sqlite3.connect(DB_PATH) as conn:
        rows = conn.execute(
            "SELECT discord_id, mp_email, old_tier, new_tier, changed_at, reason "
            "FROM tier_changes ORDER BY changed_at DESC LIMIT ?",
            (limit,),
        ).fetchall()
    return [dict(zip(["discord_id", "mp_email", "old_tier", "new_tier", "changed_at", "reason"], row)) for row in rows]


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


def get_all_members() -> list[dict]:
    with sqlite3.connect(DB_PATH) as conn:
        rows = conn.execute(
            "SELECT discord_id, mp_member_id, mp_email, tier, linked_at, last_synced "
            "FROM member_links"
        ).fetchall()
    return [dict(zip(["discord_id", "mp_member_id", "mp_email", "tier", "linked_at", "last_synced"], row)) for row in rows]


# ── Unlinked paying members (webhooks from MemberPress accounts with no Discord link) ──

def record_unlinked(mp_member_id: int):
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute("""
            INSERT INTO unlinked_members (mp_member_id) VALUES (?)
            ON CONFLICT(mp_member_id) DO UPDATE SET last_seen = CURRENT_TIMESTAMP
        """, (mp_member_id,))
        conn.commit()


def get_unlinked_ids() -> list[int]:
    with sqlite3.connect(DB_PATH) as conn:
        rows = conn.execute("SELECT mp_member_id FROM unlinked_members ORDER BY last_seen DESC").fetchall()
    return [r[0] for r in rows]


def remove_unlinked(mp_member_id: int):
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute("DELETE FROM unlinked_members WHERE mp_member_id = ?", (mp_member_id,))
        conn.commit()


# ── Expiry notices (one DM per discord_id + expiry date) ─────────────────────

def expiry_notice_sent(discord_id: str, expires_at: str) -> bool:
    with sqlite3.connect(DB_PATH) as conn:
        row = conn.execute(
            "SELECT 1 FROM expiry_notices WHERE discord_id = ? AND expires_at = ?",
            (discord_id, expires_at),
        ).fetchone()
    return row is not None


def record_expiry_notice(discord_id: str, expires_at: str):
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute(
            "INSERT OR IGNORE INTO expiry_notices (discord_id, expires_at) VALUES (?, ?)",
            (discord_id, expires_at),
        )
        conn.commit()


# ── Win-back / milestone / upgrade-nudge tracking ────────────────────────────

def get_downgrades_days_ago(days: int) -> list[dict]:
    """Tier changes to unsubscribed that happened `days` to `days+1` days ago."""
    upper = (datetime.utcnow() - timedelta(days=days)).isoformat()
    lower = (datetime.utcnow() - timedelta(days=days + 1)).isoformat()
    with sqlite3.connect(DB_PATH) as conn:
        rows = conn.execute(
            "SELECT discord_id, mp_email, old_tier, changed_at FROM tier_changes "
            "WHERE new_tier = 'unsubscribed' AND changed_at >= ? AND changed_at < ?",
            (lower, upper),
        ).fetchall()
    return [dict(zip(["discord_id", "mp_email", "old_tier", "changed_at"], row)) for row in rows]


def notice_sent(table: str, discord_id: str, key) -> bool:
    assert table in ("winback_notices", "milestone_notices")
    col = "changed_at" if table == "winback_notices" else "years"
    with sqlite3.connect(DB_PATH) as conn:
        row = conn.execute(
            f"SELECT 1 FROM {table} WHERE discord_id = ? AND {col} = ?",
            (discord_id, key),
        ).fetchone()
    return row is not None


def record_notice(table: str, discord_id: str, key):
    assert table in ("winback_notices", "milestone_notices")
    col = "changed_at" if table == "winback_notices" else "years"
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute(
            f"INSERT OR IGNORE INTO {table} (discord_id, {col}) VALUES (?, ?)",
            (discord_id, key),
        )
        conn.commit()


def upgrade_nudge_sent(discord_id: str) -> bool:
    with sqlite3.connect(DB_PATH) as conn:
        row = conn.execute("SELECT 1 FROM upgrade_nudges WHERE discord_id = ?", (discord_id,)).fetchone()
    return row is not None


def record_upgrade_nudge(discord_id: str):
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute("INSERT OR IGNORE INTO upgrade_nudges (discord_id) VALUES (?)", (discord_id,))
        conn.commit()


# ── Weekly stats snapshots ────────────────────────────────────────────────────

def save_stats_snapshot(stats: dict):
    today = datetime.utcnow().strftime("%Y-%m-%d")
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute("""
            INSERT OR REPLACE INTO stats_snapshots (snapshot_date, gold, silver, insider, unsubscribed, total)
            VALUES (?, ?, ?, ?, ?, ?)
        """, (today, stats["gold"], stats["silver"], stats["insider"], stats["unsubscribed"], stats["total"]))
        conn.commit()


def get_previous_snapshot() -> dict | None:
    """Most recent snapshot before today."""
    today = datetime.utcnow().strftime("%Y-%m-%d")
    with sqlite3.connect(DB_PATH) as conn:
        row = conn.execute(
            "SELECT snapshot_date, gold, silver, insider, unsubscribed, total "
            "FROM stats_snapshots WHERE snapshot_date < ? ORDER BY snapshot_date DESC LIMIT 1",
            (today,),
        ).fetchone()
    if not row:
        return None
    return dict(zip(["snapshot_date", "gold", "silver", "insider", "unsubscribed", "total"], row))


# ── Churn analysis ────────────────────────────────────────────────────────────

def get_churn_data(months: int = 6) -> dict:
    """Monthly new-link and cancellation counts, plus membership length for churned members."""
    cutoff = (datetime.utcnow() - timedelta(days=months * 31)).isoformat()
    with sqlite3.connect(DB_PATH) as conn:
        monthly = conn.execute("""
            SELECT substr(changed_at, 1, 7) AS month,
                   SUM(CASE WHEN new_tier = 'unsubscribed' THEN 1 ELSE 0 END) AS cancels,
                   SUM(CASE WHEN old_tier IN ('none', '') OR old_tier IS NULL THEN 1 ELSE 0 END) AS new_links
            FROM tier_changes WHERE changed_at >= ?
            GROUP BY month ORDER BY month
        """, (cutoff,)).fetchall()
        by_tier = conn.execute("""
            SELECT old_tier, COUNT(*) FROM tier_changes
            WHERE new_tier = 'unsubscribed' AND changed_at >= ? AND old_tier IN ('gold', 'silver', 'insider')
            GROUP BY old_tier
        """, (cutoff,)).fetchall()
        lengths = conn.execute("""
            SELECT tc.changed_at, ml.linked_at FROM tier_changes tc
            JOIN member_links ml ON ml.discord_id = tc.discord_id
            WHERE tc.new_tier = 'unsubscribed' AND tc.changed_at >= ? AND ml.linked_at IS NOT NULL
        """, (cutoff,)).fetchall()

    days = []
    for changed_at, linked_at in lengths:
        try:
            delta = datetime.fromisoformat(changed_at) - datetime.fromisoformat(linked_at)
            if delta.days >= 0:
                days.append(delta.days)
        except ValueError:
            continue

    return {
        "monthly": [dict(zip(["month", "cancels", "new_links"], row)) for row in monthly],
        "cancels_by_tier": dict(by_tier),
        "avg_days_before_cancel": (sum(days) / len(days)) if days else None,
        "churn_sample_size": len(days),
    }


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


# ── Flagged messages (mod moderation log) ─────────────────────────────────────

def log_flagged_message(message_id: str, channel_id: str, channel_name: str,
                        author_id: str, author_name: str, content: str,
                        flagger_id: str, flagger_name: str) -> int:
    with sqlite3.connect(DB_PATH) as conn:
        cur = conn.execute(
            "INSERT INTO flagged_messages (message_id, channel_id, channel_name, author_id, author_name, content, flagger_id, flagger_name) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            (message_id, channel_id, channel_name, author_id, author_name, content, flagger_id, flagger_name),
        )
        conn.commit()
        return cur.lastrowid


def set_flag_reason(flag_id: int, reason: str):
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute("UPDATE flagged_messages SET reason = ? WHERE id = ?", (reason, flag_id))
        conn.commit()


def get_flagged_messages(limit: int = 20) -> list[dict]:
    with sqlite3.connect(DB_PATH) as conn:
        rows = conn.execute(
            "SELECT id, channel_name, author_name, content, flagger_name, reason, flagged_at "
            "FROM flagged_messages ORDER BY flagged_at DESC LIMIT ?",
            (limit,),
        ).fetchall()
    return [
        {"id": r[0], "channel_name": r[1], "author_name": r[2], "content": r[3],
         "flagger_name": r[4], "reason": r[5], "flagged_at": r[6]}
        for r in rows
    ]


def count_flags_for_author(author_id: str) -> int:
    with sqlite3.connect(DB_PATH) as conn:
        return conn.execute(
            "SELECT COUNT(*) FROM flagged_messages WHERE author_id = ?", (author_id,)
        ).fetchone()[0]


def get_flag_totals(top: int = 15) -> dict:
    with sqlite3.connect(DB_PATH) as conn:
        total = conn.execute("SELECT COUNT(*) FROM flagged_messages").fetchone()[0]
        last30 = conn.execute(
            "SELECT COUNT(*) FROM flagged_messages WHERE flagged_at >= ?",
            ((datetime.utcnow() - timedelta(days=30)).isoformat(sep=" "),),
        ).fetchone()[0]
        rows = conn.execute(
            "SELECT author_id, author_name, COUNT(*) as n FROM flagged_messages "
            "GROUP BY author_id ORDER BY n DESC, MAX(flagged_at) DESC LIMIT ?",
            (top,),
        ).fetchall()
    return {
        "total": total,
        "last30": last30,
        "by_author": [{"author_id": r[0], "author_name": r[1], "count": r[2]} for r in rows],
    }
