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
        conn.execute("""
            CREATE TABLE IF NOT EXISTS news_items (
                id             INTEGER PRIMARY KEY AUTOINCREMENT,
                source         TEXT NOT NULL,
                guid           TEXT NOT NULL,
                title          TEXT NOT NULL,
                url            TEXT NOT NULL,
                kind           TEXT NOT NULL,
                thumbnail      TEXT,
                published_at   TEXT,
                first_seen_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                discord_posted INTEGER DEFAULT 0,
                UNIQUE (source, guid)
            )
        """)
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_news_published ON news_items (published_at DESC)"
        )
        # Columns added after launch; ALTER is a no-op error once they exist.
        for col_decl in ("summary TEXT", "suppressed INTEGER DEFAULT 0"):
            try:
                conn.execute(f"ALTER TABLE news_items ADD COLUMN {col_decl}")
            except sqlite3.OperationalError:
                pass
        # ── Onboarding (new-member journey) ──────────────────────────────────
        # One row per guild join; each step is stamped with a timestamp so the
        # daily report can count what happened in a window.
        conn.execute("""
            CREATE TABLE IF NOT EXISTS discord_joins (
                discord_id            TEXT PRIMARY KEY,
                joined_at             TEXT,
                welcome_dm_sent_at    TEXT,
                welcome_dm_failed_at  TEXT,
                nudged_24h_at         TEXT,
                nudged_72h_at         TEXT
            )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS verify_failures (
                id           INTEGER PRIMARY KEY AUTOINCREMENT,
                discord_id   TEXT,
                email        TEXT,
                reason       TEXT,
                attempted_at TEXT
            )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS onboarding_notices (
                discord_id  TEXT,
                step        TEXT,
                sent_at     TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (discord_id, step)
            )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS mailchimp_sync (
                mp_member_id INTEGER PRIMARY KEY,
                email        TEXT,
                tags         TEXT,
                synced_at    TEXT
            )
        """)
        # OAuth "Connect Discord" from the account page: one row per started
        # login, consumed exactly once by the callback.
        conn.execute("""
            CREATE TABLE IF NOT EXISTS connect_states (
                state       TEXT PRIMARY KEY,
                email       TEXT,
                wp_user_id  INTEGER,
                created_at  TEXT,
                used        INTEGER DEFAULT 0
            )
        """)
        # One row per game-week channel the bot opened. Close only ever acts on
        # channels listed here, so a hand-made channel can never be deleted by
        # the scheduler no matter what it's named.
        conn.execute("""
            CREATE TABLE IF NOT EXISTS gameday_channels (
                channel_id  TEXT PRIMARY KEY,
                game_date   TEXT,
                opponent    TEXT,
                sport       TEXT,
                close_on    TEXT,
                opened_at   TEXT,
                closed_at   TEXT
            )
        """)
        # unlinked_members grew columns so the funnel can tell "never verified"
        # from "verified later" without deleting the signup history.
        for col_decl in ("email TEXT", "registered_at TEXT", "verified_at TEXT"):
            try:
                conn.execute(f"ALTER TABLE unlinked_members ADD COLUMN {col_decl}")
            except sqlite3.OperationalError:
                pass
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


def peek_token(token: str) -> str | None:
    """Validate a token WITHOUT consuming it.

    The verify page checks the token up front but only burns it on success, so
    a member who mistypes their email can correct it on the same 15-minute link
    instead of going back to Discord for a new one.
    """
    with sqlite3.connect(DB_PATH) as conn:
        row = conn.execute(
            "SELECT discord_id, expires_at, used FROM verify_tokens WHERE token = ?",
            (token,),
        ).fetchone()
    if not row:
        return None
    discord_id, expires_at, used = row
    if used or datetime.utcnow() > datetime.fromisoformat(expires_at):
        return None
    return discord_id


def cleanup_expired_tokens():
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute(
            "DELETE FROM verify_tokens WHERE expires_at < ? OR used = 1",
            (datetime.utcnow().isoformat(),),
        )
        conn.execute(
            "DELETE FROM connect_states WHERE created_at < ? OR used = 1",
            ((datetime.utcnow() - timedelta(minutes=30)).isoformat(),),
        )
        conn.commit()


# ── OAuth connect states (account-page "Connect Discord") ────────────────────

def create_connect_state(state: str, email: str, wp_user_id: int):
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute(
            "INSERT INTO connect_states (state, email, wp_user_id, created_at) VALUES (?, ?, ?, ?)",
            (state, email, wp_user_id, datetime.utcnow().isoformat()),
        )
        conn.commit()


def consume_connect_state(state: str, max_age_minutes: int = 15) -> dict | None:
    """Return {email, wp_user_id} once for a fresh, unused state; None otherwise."""
    with sqlite3.connect(DB_PATH) as conn:
        row = conn.execute(
            "SELECT email, wp_user_id, created_at, used FROM connect_states WHERE state = ?", (state,)
        ).fetchone()
        if not row:
            return None
        email, wp_user_id, created_at, used = row
        if used or datetime.utcnow() - datetime.fromisoformat(created_at) > timedelta(minutes=max_age_minutes):
            return None
        conn.execute("UPDATE connect_states SET used = 1 WHERE state = ?", (state,))
        conn.commit()
    return {"email": email, "wp_user_id": wp_user_id}


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

def record_unlinked(mp_member_id: int, email: str | None = None, registered_at: str | None = None):
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute("""
            INSERT INTO unlinked_members (mp_member_id, email, registered_at) VALUES (?, ?, ?)
            ON CONFLICT(mp_member_id) DO UPDATE SET
                last_seen     = CURRENT_TIMESTAMP,
                email         = COALESCE(excluded.email, unlinked_members.email),
                registered_at = COALESCE(excluded.registered_at, unlinked_members.registered_at)
        """, (mp_member_id, email, registered_at))
        conn.commit()


def get_unlinked_ids() -> list[int]:
    """Webhook-seen MemberPress accounts that still have no Discord link."""
    with sqlite3.connect(DB_PATH) as conn:
        rows = conn.execute(
            "SELECT mp_member_id FROM unlinked_members WHERE verified_at IS NULL ORDER BY last_seen DESC"
        ).fetchall()
    return [r[0] for r in rows]


def mark_unlinked_verified(mp_member_id: int):
    """Stop reporting a member who has since verified, but keep the row — it
    dates their signup, which the funnel needs to measure time-to-verify."""
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute(
            "UPDATE unlinked_members SET verified_at = ? WHERE mp_member_id = ? AND verified_at IS NULL",
            (datetime.utcnow().isoformat(), mp_member_id),
        )
        conn.commit()


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


def clear_notices_above(table: str, discord_id: str, years: int) -> int:
    """Drop milestone notices for years the member has not actually reached.

    Needed when tenure is recalculated downward — an over-granted notice would
    silently suppress the real milestone when they genuinely get there.
    """
    assert table in ("winback_notices", "milestone_notices")
    with sqlite3.connect(DB_PATH) as conn:
        cur = conn.execute(
            f"DELETE FROM {table} WHERE discord_id = ? AND years > ?",
            (str(discord_id), int(years)),
        )
        return cur.rowcount or 0


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
            "SELECT id, channel_name, author_id, author_name, content, flagger_name, reason, flagged_at "
            "FROM flagged_messages ORDER BY flagged_at DESC LIMIT ?",
            (limit,),
        ).fetchall()
    return [
        {"id": r[0], "channel_name": r[1], "author_id": r[2], "author_name": r[3], "content": r[4],
         "flagger_name": r[5], "reason": r[6], "flagged_at": r[7]}
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


# ── News aggregator ───────────────────────────────────────────────────────────

def news_source_seeded(source: str) -> bool:
    """True once a source has any rows — its first poll seeds silently."""
    with sqlite3.connect(DB_PATH) as conn:
        row = conn.execute(
            "SELECT 1 FROM news_items WHERE source = ? LIMIT 1", (source,)
        ).fetchone()
    return row is not None


def insert_news_item(source: str, guid: str, title: str, url: str, kind: str,
                     thumbnail: str | None, published_at: str,
                     discord_posted: int = 0, summary: str | None = None) -> bool:
    """Insert an item if unseen. Returns True only when the row is new.
    Known rows missing a summary get one backfilled while the feed still
    carries the entry."""
    with sqlite3.connect(DB_PATH) as conn:
        cur = conn.execute(
            "INSERT OR IGNORE INTO news_items "
            "(source, guid, title, url, kind, thumbnail, published_at, discord_posted, summary) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (source, guid, title, url, kind, thumbnail, published_at, discord_posted, summary),
        )
        if cur.rowcount == 0 and summary:
            conn.execute(
                "UPDATE news_items SET summary = ? "
                "WHERE source = ? AND guid = ? AND (summary IS NULL OR summary = '')",
                (summary, source, guid),
            )
        conn.commit()
    return cur.rowcount == 1


def mark_news_suppressed(source: str, guid: str):
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute(
            "UPDATE news_items SET suppressed = 1 WHERE source = ? AND guid = ?",
            (source, guid),
        )
        conn.commit()


def get_recent_titles(days: int = 2) -> list[dict]:
    """Titles seen in the last N days (suppressed ones included) — the
    corpus the cross-source duplicate check compares against."""
    cutoff = (datetime.utcnow() - timedelta(days=days)).isoformat(sep=" ")
    with sqlite3.connect(DB_PATH) as conn:
        rows = conn.execute(
            "SELECT source, title FROM news_items WHERE first_seen_at >= ?",
            (cutoff,),
        ).fetchall()
    return [{"source": r[0], "title": r[1]} for r in rows]


def mark_news_posted(source: str, guid: str):
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute(
            "UPDATE news_items SET discord_posted = 1 WHERE source = ? AND guid = ?",
            (source, guid),
        )
        conn.commit()


def get_recent_news(limit: int = 30, include_cougconnect: bool = False) -> list[dict]:
    query = ("SELECT source, title, url, kind, thumbnail, published_at, summary FROM news_items "
             "WHERE suppressed = 0 "
             + ("" if include_cougconnect else "AND kind != 'cougconnect' ")
             + "ORDER BY published_at DESC LIMIT ?")
    with sqlite3.connect(DB_PATH) as conn:
        rows = conn.execute(query, (limit,)).fetchall()
    return [
        dict(zip(["source", "title", "url", "kind", "thumbnail", "published_at", "summary"], row))
        for row in rows
    ]


# ── Onboarding (new-member journey) ───────────────────────────────────────────

_JOIN_FLAGS = ("welcome_dm_sent", "welcome_dm_failed", "nudged_24h", "nudged_72h")


def record_join(discord_id: str):
    """A (re)join resets the clock — the nudges are about THIS visit."""
    now = datetime.utcnow().isoformat()
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute("""
            INSERT INTO discord_joins (discord_id, joined_at) VALUES (?, ?)
            ON CONFLICT(discord_id) DO UPDATE SET
                joined_at = excluded.joined_at,
                welcome_dm_sent_at = NULL, welcome_dm_failed_at = NULL,
                nudged_24h_at = NULL, nudged_72h_at = NULL
        """, (discord_id, now))
        conn.commit()


def set_join_flag(discord_id: str, flag: str):
    """Stamp a step as done. Creates the row for members who joined before tracking existed."""
    assert flag in _JOIN_FLAGS
    now = datetime.utcnow().isoformat()
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute("INSERT OR IGNORE INTO discord_joins (discord_id, joined_at) VALUES (?, ?)", (discord_id, now))
        conn.execute(f"UPDATE discord_joins SET {flag}_at = ? WHERE discord_id = ?", (now, discord_id))
        conn.commit()


def get_joins_due(hours: int, flag: str, window_hours: int = 72) -> list[dict]:
    """Joins between `hours` and `hours + window_hours` old that haven't had `flag` yet.

    The window keeps a long-off flag from waking up and nudging months-old rows.
    """
    assert flag in _JOIN_FLAGS
    newest = (datetime.utcnow() - timedelta(hours=hours)).isoformat()
    oldest = (datetime.utcnow() - timedelta(hours=hours + window_hours)).isoformat()
    with sqlite3.connect(DB_PATH) as conn:
        rows = conn.execute(
            f"SELECT discord_id, joined_at FROM discord_joins "
            f"WHERE {flag}_at IS NULL AND joined_at <= ? AND joined_at >= ? ORDER BY joined_at",
            (newest, oldest),
        ).fetchall()
    return [{"discord_id": r[0], "joined_at": r[1]} for r in rows]


def count_joins_since(hours: int) -> int:
    cutoff = (datetime.utcnow() - timedelta(hours=hours)).isoformat()
    with sqlite3.connect(DB_PATH) as conn:
        return conn.execute("SELECT COUNT(*) FROM discord_joins WHERE joined_at >= ?", (cutoff,)).fetchone()[0]


def count_join_flag_since(flag: str, hours: int) -> int:
    assert flag in _JOIN_FLAGS
    cutoff = (datetime.utcnow() - timedelta(hours=hours)).isoformat()
    with sqlite3.connect(DB_PATH) as conn:
        return conn.execute(f"SELECT COUNT(*) FROM discord_joins WHERE {flag}_at >= ?", (cutoff,)).fetchone()[0]


def count_unverified_joins_older_than(hours: int) -> int:
    """Joined more than `hours` ago and never linked a membership."""
    cutoff = (datetime.utcnow() - timedelta(hours=hours)).isoformat()
    with sqlite3.connect(DB_PATH) as conn:
        return conn.execute(
            "SELECT COUNT(*) FROM discord_joins j LEFT JOIN member_links m ON m.discord_id = j.discord_id "
            "WHERE j.joined_at < ? AND m.discord_id IS NULL",
            (cutoff,),
        ).fetchone()[0]


def record_verify_failure(discord_id: str, email: str, reason: str):
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute(
            "INSERT INTO verify_failures (discord_id, email, reason, attempted_at) VALUES (?, ?, ?, ?)",
            (discord_id, email, reason, datetime.utcnow().isoformat()),
        )
        conn.commit()


def count_verify_failures(discord_id: str, hours: int = 24) -> int:
    cutoff = (datetime.utcnow() - timedelta(hours=hours)).isoformat()
    with sqlite3.connect(DB_PATH) as conn:
        return conn.execute(
            "SELECT COUNT(*) FROM verify_failures WHERE discord_id = ? AND attempted_at >= ?",
            (discord_id, cutoff),
        ).fetchone()[0]


def get_verify_failures_since(hours: int) -> list[dict]:
    cutoff = (datetime.utcnow() - timedelta(hours=hours)).isoformat()
    with sqlite3.connect(DB_PATH) as conn:
        rows = conn.execute(
            "SELECT discord_id, email, reason, attempted_at FROM verify_failures "
            "WHERE attempted_at >= ? ORDER BY attempted_at DESC",
            (cutoff,),
        ).fetchall()
    return [dict(zip(["discord_id", "email", "reason", "attempted_at"], r)) for r in rows]


def onboarding_step_sent(discord_id: str, step: str) -> bool:
    with sqlite3.connect(DB_PATH) as conn:
        row = conn.execute(
            "SELECT 1 FROM onboarding_notices WHERE discord_id = ? AND step = ?", (discord_id, step)
        ).fetchone()
    return row is not None


def record_onboarding_step(discord_id: str, step: str):
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute("INSERT OR IGNORE INTO onboarding_notices (discord_id, step) VALUES (?, ?)", (discord_id, step))
        conn.commit()


def get_members_linked_days_ago(days: int) -> list[dict]:
    """Members whose first verification was `days`..`days+1` days ago."""
    upper = (datetime.utcnow() - timedelta(days=days)).isoformat()
    lower = (datetime.utcnow() - timedelta(days=days + 1)).isoformat()
    with sqlite3.connect(DB_PATH) as conn:
        rows = conn.execute(
            "SELECT discord_id, mp_member_id, mp_email, tier, linked_at FROM member_links "
            "WHERE linked_at >= ? AND linked_at < ?",
            (lower, upper),
        ).fetchall()
    return [dict(zip(["discord_id", "mp_member_id", "mp_email", "tier", "linked_at"], r)) for r in rows]


def mailchimp_synced(mp_member_id: int, tags: list[str]) -> bool:
    """True when this member was already pushed with exactly these tags."""
    with sqlite3.connect(DB_PATH) as conn:
        row = conn.execute("SELECT tags FROM mailchimp_sync WHERE mp_member_id = ?", (mp_member_id,)).fetchone()
    return bool(row) and row[0] == ",".join(sorted(tags))


def record_mailchimp_sync(mp_member_id: int, email: str, tags: list[str]):
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute(
            "INSERT OR REPLACE INTO mailchimp_sync (mp_member_id, email, tags, synced_at) VALUES (?, ?, ?, ?)",
            (mp_member_id, email, ",".join(sorted(tags)), datetime.utcnow().isoformat()),
        )
        conn.commit()


# ── Game-week channels ────────────────────────────────────────────────────────

def record_gameday_channel(channel_id: str, game_date: str, opponent: str,
                           sport: str, close_on: str):
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute(
            "INSERT OR REPLACE INTO gameday_channels "
            "(channel_id, game_date, opponent, sport, close_on, opened_at) "
            "VALUES (?, ?, ?, ?, ?, ?)",
            (str(channel_id), game_date, opponent, sport, close_on,
             datetime.utcnow().isoformat()),
        )
        conn.commit()


def gameday_channel_open(game_date: str, opponent: str) -> str | None:
    """Channel id already open for this game, or None. Keeps opens idempotent."""
    with sqlite3.connect(DB_PATH) as conn:
        row = conn.execute(
            "SELECT channel_id FROM gameday_channels "
            "WHERE game_date = ? AND opponent = ? AND closed_at IS NULL",
            (game_date, opponent),
        ).fetchone()
    return row[0] if row else None


def get_gameday_channels_due(on_or_before: str) -> list[dict]:
    """Open channels whose close date has arrived.

    `<=` rather than `=` so a week the bot slept through still gets cleaned up
    on the next run instead of leaving the channel open forever.
    """
    with sqlite3.connect(DB_PATH) as conn:
        conn.row_factory = sqlite3.Row
        rows = conn.execute(
            "SELECT * FROM gameday_channels WHERE closed_at IS NULL AND close_on <= ? "
            "ORDER BY close_on",
            (on_or_before,),
        ).fetchall()
    return [dict(r) for r in rows]


def get_open_gameday_channels() -> list[dict]:
    with sqlite3.connect(DB_PATH) as conn:
        conn.row_factory = sqlite3.Row
        rows = conn.execute(
            "SELECT * FROM gameday_channels WHERE closed_at IS NULL ORDER BY game_date"
        ).fetchall()
    return [dict(r) for r in rows]


def mark_gameday_channel_closed(channel_id: str):
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute(
            "UPDATE gameday_channels SET closed_at = ? WHERE channel_id = ?",
            (datetime.utcnow().isoformat(), str(channel_id)),
        )
        conn.commit()
