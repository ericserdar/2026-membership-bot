"""BYU content aggregator: polls the feeds.json sources, posts new items to
the #byu-news channel, and serves /news.json for the cougconnect.com widget.

All sources are plain RSS/Atom (YouTube via its free no-key Atom feeds), so
there are no API credentials here. Dedup lives in the news_items table; a
source's very first poll seeds every entry silently so a fresh deploy never
floods the channel.
"""
import asyncio
import gzip
import json
import logging
import os
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
    }


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


async def _process_source(session, channel, source_cfg: dict, post_admin_log) -> int:
    """Poll one source. Returns the number of items silently backfilled."""
    src_id = source_cfg["id"]
    parsed = await fetch_feed(session, source_cfg["url"])
    items = [i for e in parsed.entries[:50] if (i := entry_to_item(e, source_cfg))]

    if not db.news_source_seeded(src_id):
        for item in items:
            db.insert_news_item(src_id, item["guid"], item["title"], item["url"],
                                item["kind"], item["thumbnail"], item["published_at"])
        return len(items)

    cutoff = datetime.now(timezone.utc) - timedelta(days=MAX_AGE_DAYS)
    to_post = []
    for item in items:
        is_new = db.insert_news_item(src_id, item["guid"], item["title"], item["url"],
                                     item["kind"], item["thumbnail"], item["published_at"])
        if (is_new and len(to_post) < MAX_PER_SOURCE
                and datetime.fromisoformat(item["published_at"]) >= cutoff):
            to_post.append(item)

    for item in reversed(to_post):  # oldest first, so the channel reads chronologically
        await channel.send(embed=build_embed(item, source_cfg))
        db.mark_news_posted(src_id, item["guid"])
        await asyncio.sleep(1)

    if _fail_counts.pop(src_id, 0) >= ALERT_AFTER_FAILURES:
        _last_alert.pop(src_id, None)
        await post_admin_log(f"✅ News feed recovered: **{source_cfg['name']}**")
    return 0


async def poll_feeds(bot, channel_id: int, post_admin_log):
    """One polling cycle over every enabled source. Called by bot.news_task."""
    global _warned_no_channel
    channel = bot.get_channel(channel_id) if channel_id else None
    if channel is None:
        if not _warned_no_channel:
            _warned_no_channel = True
            await post_admin_log(
                "📰 News aggregator is idle — set DISCORD_NEWS_CHANNEL_ID to the "
                "#byu-news channel ID to activate it."
            )
        return

    backfilled = 0
    async with aiohttp.ClientSession(headers={"User-Agent": USER_AGENT}) as session:
        for source_cfg in load_feeds():
            try:
                backfilled += await _process_source(session, channel, source_cfg, post_admin_log)
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
