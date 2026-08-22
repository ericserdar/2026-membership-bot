"""BYU content aggregator: polls the feeds.json sources, posts new items to
the #byu-news channel, and serves /news.json for the cougconnect.com widget.

All sources are plain RSS/Atom (YouTube via its free no-key Atom feeds), so
there are no API credentials here. Dedup lives in the news_items table; a
source's very first poll seeds every entry silently so a fresh deploy never
floods the channel.
"""
import asyncio
import difflib
import gzip
import html as html_mod
import json
import logging
import os
import re
from datetime import datetime, timedelta, timezone

import aiohttp
from aiohttp import web
import discord
import feedparser

import database as db

log = logging.getLogger("cougconnect")

FEEDS_PATH = os.path.join(os.path.dirname(__file__), "feeds.json")
MAX_PER_SOURCE = int(os.getenv("NEWS_MAX_PER_SOURCE", "5"))
# Feeds sometimes recycle old entries under new GUIDs; never post stale items.
MAX_AGE_DAYS = 3
FETCH_TIMEOUT = aiohttp.ClientTimeout(total=30)
USER_AGENT = "CougConnectBot/1.0 (+https://cougconnect.com)"
ALERT_AFTER_FAILURES = 3
ALERT_COOLDOWN = timedelta(hours=24)

KIND_DEFAULTS = {  # kind -> (emoji, embed color)
    "article":     ("📰", 0x1A3EF0),
    "video":       ("▶️", 0xE02424),
    "podcast":     ("🎙️", 0x7C3AED),
    "cougconnect": ("🔵", 0x0D1B2E),
}

# In-memory failure tracking — resetting on redeploy is fine, it only delays
# an alert by a couple of cycles.
_fail_counts: dict[str, int] = {}
_last_alert: dict[str, datetime] = {}
_warned_no_channel = False


def load_feeds() -> list[dict]:
    try:
        with open(FEEDS_PATH) as f:
            return [s for s in json.load(f) if s.get("enabled")]
    except (FileNotFoundError, json.JSONDecodeError):
        return []


async def fetch_feed(session: aiohttp.ClientSession, url: str):
    async with session.get(url, timeout=FETCH_TIMEOUT) as resp:
        resp.raise_for_status()
        raw = await resp.read()
    # aiohttp only decompresses when Content-Encoding is set; byucougars.com
    # gzips unconditionally without the header.
    if raw[:2] == b"\x1f\x8b":
        raw = gzip.decompress(raw)
    return await asyncio.to_thread(feedparser.parse, raw)


def _entry_datetime(entry) -> datetime | None:
    parsed = entry.get("published_parsed") or entry.get("updated_parsed")
    if not parsed:
        return None
    return datetime(*parsed[:6], tzinfo=timezone.utc)


def _entry_thumbnail(entry) -> str | None:
    for thumb in entry.get("media_thumbnail") or []:
        if thumb.get("url"):
            return thumb["url"]
    for media in entry.get("media_content") or []:
        if media.get("url") and media.get("medium", "image") == "image":
            return media["url"]
    image = entry.get("image")
    if image and image.get("href"):
        return image["href"]
    return None


def _entry_summary(entry) -> str | None:
    raw = entry.get("summary") or entry.get("description") or ""
    if not raw:
        for media in entry.get("media_content") or []:
            raw = media.get("description") or ""
            if raw:
                break
    text = html_mod.unescape(re.sub(r"<[^>]+>", " ", raw))
    text = re.sub(r"\s+", " ", text).strip()
    if len(text) < 15:
        return None
    if len(text) > 220:
        text = text[:220].rsplit(" ", 1)[0].rstrip(".,;:") + "…"
    return text


def entry_to_item(entry, source_cfg: dict) -> dict | None:
    guid = entry.get("id") or entry.get("link")
    link = entry.get("link")
    title = (entry.get("title") or "").strip()
    if not guid or not link or not title:
        return None
    published = _entry_datetime(entry) or datetime.now(timezone.utc)
    return {
        "guid": guid,
        "title": title[:256],
        "url": link,
        "kind": source_cfg["kind"],
        "thumbnail": _entry_thumbnail(entry),
        "published_at": published.isoformat(),
        "summary": _entry_summary(entry),
    }


def _normalize_title(title: str) -> str:
    return re.sub(r"\s+", " ", re.sub(r"[^a-z0-9 ]", " ", title.lower())).strip()


def is_duplicate_title(title: str, corpus: list[str]) -> bool:
    """True when a title is effectively the same story as one already seen.

    Conservative on purpose: the target is cross-posted mirrors (a podcast
    episode and its YouTube upload), not two outlets covering one event.
    Same-day pressers share long boilerplate ("… | Media Availability |
    Fall Camp | August 21") and sit around ratio 0.85, so the threshold
    stays above that.
    """
    norm = _normalize_title(title)
    if len(norm) < 20:
        return False
    for seen in corpus:
        if len(seen) < 20:
            continue
        shorter, longer = sorted((norm, seen), key=len)
        if len(shorter) >= 30 and shorter in longer:
            return True
        if difflib.SequenceMatcher(None, norm, seen).ratio() >= 0.92:
            return True
    return False


def build_embed(item: dict, source_cfg: dict) -> discord.Embed:
    emoji, color = KIND_DEFAULTS[source_cfg["kind"]]
    emoji = source_cfg.get("emoji", emoji)
    if source_cfg.get("color"):
        color = int(source_cfg["color"], 16)
    title = item["title"]
    if source_cfg["kind"] == "cougconnect":
        title = f"New on CougConnect: {title}"
    embed = discord.Embed(
        title=f"{emoji} {title}"[:256],
        url=item["url"],
        color=color,
        timestamp=datetime.fromisoformat(item["published_at"]),
    )
    embed.set_author(name=source_cfg["name"])
    if item.get("thumbnail"):
        if source_cfg["kind"] == "video":
            embed.set_image(url=item["thumbnail"])
        else:
            embed.set_thumbnail(url=item["thumbnail"])
    embed.set_footer(text="Discuss below 👇 · CougConnect")
    return embed


def thread_name(item: dict, source_cfg: dict) -> str:
    emoji, _ = KIND_DEFAULTS[source_cfg["kind"]]
    emoji = source_cfg.get("emoji", emoji)
    return f"{emoji} {item['title']}"[:100]


# SUPPRESS_NOTIFICATIONS — posts arrive without pinging anyone (like @silent).
SILENT_FLAG = 1 << 12

KIND_TAG_NAMES = {"video": "Video", "podcast": "Podcast",
                  "article": "Article", "cougconnect": "CougConnect"}
_tag_ids: dict[str, int] = {}   # kind -> forum tag id, resolved once per process
_tag_create_failed = False


async def _kind_tag_id(channel, kind: str) -> int | None:
    """Resolve (creating if needed) the forum tag for a kind.
    Creating needs Manage Channels; failure disables tagging quietly."""
    global _tag_create_failed
    name = KIND_TAG_NAMES.get(kind)
    if name is None:
        return None
    if kind in _tag_ids:
        return _tag_ids[kind]
    for tag in channel.available_tags:
        if tag.name == name:
            _tag_ids[kind] = tag.id
            return tag.id
    if _tag_create_failed:
        return None
    try:
        tag = await channel.create_tag(name=name)
        _tag_ids[kind] = tag.id
        return tag.id
    except Exception as e:
        _tag_create_failed = True
        log.warning(f"Forum tag creation failed (needs Manage Channels?): {e}")
        return None


async def send_item(session: aiohttp.ClientSession, channel, item: dict, source_cfg: dict):
    """Post one item silently: a forum post in a forum channel, else a message.

    discord.py can't set SUPPRESS_NOTIFICATIONS on a forum starter message,
    so the forum path hits the REST endpoint directly.
    """
    embed = build_embed(item, source_cfg)
    if isinstance(channel, discord.ForumChannel):
        payload = {
            "name": thread_name(item, source_cfg),
            "auto_archive_duration": 10080,
            "message": {"embeds": [embed.to_dict()], "flags": SILENT_FLAG},
        }
        tag_id = await _kind_tag_id(channel, source_cfg["kind"])
        if tag_id is not None:
            payload["applied_tags"] = [str(tag_id)]
        headers = {"Authorization": f"Bot {os.getenv('DISCORD_BOT_TOKEN', '')}"}
        for _ in range(3):
            async with session.post(
                f"https://discord.com/api/v10/channels/{channel.id}/threads",
                json=payload, headers=headers, timeout=FETCH_TIMEOUT,
            ) as resp:
                if resp.status == 429:
                    data = await resp.json()
                    await asyncio.sleep(float(data.get("retry_after", 2)) + 0.5)
                    continue
                resp.raise_for_status()
                return
    else:
        await channel.send(embed=embed, silent=True)


async def _handle_feed_failure(source_cfg: dict, error: Exception, post_admin_log):
    src_id = source_cfg["id"]
    _fail_counts[src_id] = _fail_counts.get(src_id, 0) + 1
    log.warning(f"News feed failed ({src_id}, attempt {_fail_counts[src_id]}): {error}")
    if _fail_counts[src_id] < ALERT_AFTER_FAILURES:
        return
    now = datetime.now(timezone.utc)
    last = _last_alert.get(src_id)
    if last and now - last < ALERT_COOLDOWN:
        return
    _last_alert[src_id] = now
    await post_admin_log(
        f"⚠️ News feed **{source_cfg['name']}** has failed "
        f"{_fail_counts[src_id]} times in a row: `{error}`"
    )


async def _process_source(session, channel, source_cfg: dict, post_admin_log,
                          title_corpus: list[str]) -> int:
    """Poll one source. Returns the number of items silently backfilled."""
    src_id = source_cfg["id"]
    parsed = await fetch_feed(session, source_cfg["url"])
    items = [i for e in parsed.entries[:50] if (i := entry_to_item(e, source_cfg))]

    if not db.news_source_seeded(src_id):
        for item in items:
            db.insert_news_item(src_id, item["guid"], item["title"], item["url"],
                                item["kind"], item["thumbnail"], item["published_at"],
                                summary=item["summary"])
        return len(items)

    cutoff = datetime.now(timezone.utc) - timedelta(days=MAX_AGE_DAYS)
    to_post = []
    for item in items:
        is_new = db.insert_news_item(src_id, item["guid"], item["title"], item["url"],
                                     item["kind"], item["thumbnail"], item["published_at"],
                                     summary=item["summary"])
        if not is_new:
            continue
        if is_duplicate_title(item["title"], title_corpus):
            db.mark_news_suppressed(src_id, item["guid"])
            log.info(f"Suppressed cross-source duplicate ({src_id}): {item['title'][:80]}")
            continue
        title_corpus.append(_normalize_title(item["title"]))
        if (len(to_post) < MAX_PER_SOURCE
                and datetime.fromisoformat(item["published_at"]) >= cutoff):
            to_post.append(item)

    if channel is not None:
        for item in reversed(to_post):  # oldest first, so the channel reads chronologically
            await send_item(session, channel, item, source_cfg)
            db.mark_news_posted(src_id, item["guid"])
            await asyncio.sleep(1)

    if _fail_counts.pop(src_id, 0) >= ALERT_AFTER_FAILURES:
        _last_alert.pop(src_id, None)
        await post_admin_log(f"✅ News feed recovered: **{source_cfg['name']}**")
    return 0


async def poll_feeds(bot, channel_id: int, post_admin_log):
    """One polling cycle over every enabled source. Called by bot.news_task."""
    global _warned_no_channel
    # No channel yet? Still fetch and store, so /news.json (the website feed)
    # works and sources get seeded — only the Discord posting waits.
    channel = bot.get_channel(channel_id) if channel_id else None
    if channel is None and not _warned_no_channel:
        _warned_no_channel = True
        await post_admin_log(
            "📰 News aggregator is collecting for the website, but Discord posting "
            "is off — set DISCORD_NEWS_CHANNEL_ID to the #byu-news channel ID."
        )

    backfilled = 0
    title_corpus = [_normalize_title(r["title"]) for r in db.get_recent_titles(days=2)]
    async with aiohttp.ClientSession(headers={"User-Agent": USER_AGENT}) as session:
        for source_cfg in load_feeds():
            try:
                backfilled += await _process_source(session, channel, source_cfg,
                                                    post_admin_log, title_corpus)
            except Exception as e:
                await _handle_feed_failure(source_cfg, e, post_admin_log)
    if backfilled:
        await post_admin_log(
            f"📰 News aggregator first run: seeded {backfilled} items silently — "
            "new items will post to the channel from the next cycle on."
        )


async def handle_news_json(request: web.Request) -> web.Response:
    try:
        limit = min(int(request.query.get("limit", "30")), 50)
    except ValueError:
        limit = 30
    items = await asyncio.to_thread(
        db.get_recent_news, limit, request.query.get("all") == "1"
    )
    names = {s["id"]: s["name"] for s in load_feeds()}
    for item in items:
        item["source_name"] = names.get(item["source"], item["source"])
        item["emoji"] = KIND_DEFAULTS.get(item["kind"], ("🔗", 0))[0]
    return web.json_response(
        {"updated": datetime.now(timezone.utc).isoformat(), "items": items},
        headers={
            "Access-Control-Allow-Origin": "*",
            "Cache-Control": "public, max-age=300",
        },
    )
