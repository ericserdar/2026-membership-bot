"""Backtest AutoMod against recent history. READ-ONLY: posts, deletes and DMs nothing.

Pulls the last N messages from each watched channel, runs every message that
live AutoMod would check through automod.classify(), and writes a CSV you can
read through to see whether the rubric matches your judgment.

Usage:
    python3 ops/automod_backtest.py                  # 500 per channel
    python3 ops/automod_backtest.py --limit 200
    python3 ops/automod_backtest.py --channels 1050091238730236055

Needs DISCORD_BOT_TOKEN and ANTHROPIC_API_KEY in .env.

Limits: Discord history = 100 messages/request (5 requests per channel at 500,
paced 1s apart and honouring 429 retry_after). Claude: one call per message,
4 concurrent; failures come back blank (fail-open), they never abort the run.
Output: ops/automod_backtest-<timestamp>.csv, written in full at the end and
also every 50 rows, so an interrupted run keeps what it already classified.
"""
import argparse
import asyncio
import csv
import os
import sys
import time
from datetime import datetime

import requests
from dotenv import load_dotenv

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(HERE, os.pardir))
load_dotenv(os.path.join(HERE, os.pardir, ".env"))

import automod  # noqa: E402  (after load_dotenv so it sees the env)

API = "https://discord.com/api/v10"
HEADERS = {
    "Authorization": f"Bot {os.getenv('DISCORD_BOT_TOKEN', '')}",
    # Discord 403s Python's default User-Agent, so send an explicit one.
    "User-Agent": "CougConnectBot automod_backtest/1.0 (+https://cougconnect.com)",
}
DEFAULT_CHANNELS = ["1050091238730236055", "1050104741692178432"]


def get(path, params=None):
    for _ in range(5):
        r = requests.get(API + path, headers=HEADERS, params=params, timeout=30)
        if r.status_code == 429:
            time.sleep(float(r.json().get("retry_after", 2)) + 0.5)
            continue
        r.raise_for_status()
        return r.json()
    raise RuntimeError(f"rate-limited 5x on {path}")


def fetch_history(channel_id, limit):
    """Newest-first pages, returned oldest-first."""
    out, before = [], None
    while len(out) < limit:
        params = {"limit": min(100, limit - len(out))}
        if before:
            params["before"] = before
        page = get(f"/channels/{channel_id}/messages", params)
        if not page:
            break
        out.extend(page)
        before = page[-1]["id"]
        time.sleep(1)
    return list(reversed(out))


async def run(channels, limit, out_path):
    rows, sem = [], asyncio.Semaphore(4)
    spend = {"cost": 0.0, "calls": 0}

    def tally(cost, usage):
        spend["cost"] += cost
        spend["calls"] += 1
    automod.on_usage = tally
    fields = ["channel", "message_id", "timestamp", "author", "verdict", "tone", "category", "reason", "dm_text", "content"]

    def flush():
        with open(out_path, "w", newline="") as f:
            w = csv.DictWriter(f, fieldnames=fields)
            w.writeheader()
            w.writerows(sorted(rows, key=lambda r: (r["channel"], r["timestamp"])))

    for cid in channels:
        name = get(f"/channels/{cid}").get("name", cid)
        msgs = fetch_history(cid, limit)
        print(f"#{name}: fetched {len(msgs)} messages")

        async def one(i, m):
            content = m.get("content") or ""
            author = m["author"].get("global_name") or m["author"]["username"]
            if m["author"].get("bot") or not automod.worth_checking(content):
                return
            ctx = [((p["author"].get("global_name") or p["author"]["username"]), p.get("content") or "")
                   for p in msgs[max(0, i - 5):i] if p.get("content")]
            async with sem:
                res = await automod.classify(content, author, name, ctx)
            rows.append({
                "channel": name, "message_id": m["id"], "timestamp": m["timestamp"][:19], "author": author,
                "verdict": (res or {}).get("verdict", "ERROR/allow"), "tone": (res or {}).get("tone", ""), "category": (res or {}).get("category", ""),
                "reason": (res or {}).get("reason", ""), "dm_text": (res or {}).get("dm_text", ""),
                "content": content,
            })
            if len(rows) % 50 == 0:
                flush()
                print(f"  ...{len(rows)} classified")

        await asyncio.gather(*(one(i, m) for i, m in enumerate(msgs)))
    flush()

    # Replay through the same MoodTracker the bot uses, in timestamp order.
    tracker, fires = automod.MoodTracker(), []
    for r in sorted(rows, key=lambda r: r["timestamp"]):
        ts = datetime.fromisoformat(r["timestamp"]).timestamp()
        hit = tracker.record(hash(r["channel"]), ts, r["tone"] or None)
        if hit:
            fires.append((r["channel"], r["timestamp"], hit))
    print(f"\nMood monitor (>= {automod.MOOD_NEG_PCT:.0f}% negative of >= {automod.MOOD_MIN_MSGS} msgs in "
          f"{automod.MOOD_WINDOW_MIN:.0f} min, {automod.MOOD_COOLDOWN_MIN:.0f} min cooldown) would have fired {len(fires)}x:")
    for ch, t, hit in fires:
        print(f"  #{ch} at {t} UTC: {hit['pct']}% of {hit['messages']} msgs negative ({hit['hostile']} hostile)")
    tones = {}
    for r in rows:
        tones[r["tone"] or "?"] = tones.get(r["tone"] or "?", 0) + 1
    print(f"Tones: {tones}")

    counts = {}
    for r in rows:
        counts[r["verdict"]] = counts.get(r["verdict"], 0) + 1
    print(f"\n{len(rows)} messages checked: {counts}")
    if spend["calls"]:
        per = spend["cost"] / spend["calls"]
        print(f"API cost: ${spend['cost']:.4f} for {spend['calls']} calls (${per:.5f}/msg, "
              f"~${per * 1000 * 30:.2f}/month at 1,000 checked msgs/day)")
    print(f"CSV: {out_path}")
    print("Worth reading first: every 'remove' and 'review' row, then skim 'allow' for misses.")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--limit", type=int, default=500)
    ap.add_argument("--channels", default=",".join(DEFAULT_CHANNELS))
    a = ap.parse_args()
    if not os.getenv("DISCORD_BOT_TOKEN") or not os.getenv("ANTHROPIC_API_KEY"):
        sys.exit("Need DISCORD_BOT_TOKEN and ANTHROPIC_API_KEY in .env")
    out = os.path.join(HERE, f"automod_backtest-{datetime.now():%Y%m%d-%H%M}.csv")
    asyncio.run(run([c.strip() for c in a.channels.split(",") if c.strip()], a.limit, out))


if __name__ == "__main__":
    main()
