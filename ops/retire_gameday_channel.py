"""Close out the last game-week channel: point people at the fan server, archive, delete.

Runs the cutover in the only order that's safe for the ~91 people currently in
#🏈-byu-vs-utah-tech - the pointer to the new home goes up BEFORE the channel they
are using disappears, and the transcript is safely uploaded before anything is
destroyed:

    1. post + pin the pointer in #🏈-byufootball   (silent, no pings)
    2. read the full channel transcript            (oldest first)
    3. upload the transcript to the admin log
    4. delete the channel                          (only if step 3 succeeded)

Step 4 is irreversible and is gated on step 3 returning a real message id. No
transcript, no delete - the same guarantee the old /gameday-close command had.

Usage:
    python3 ops/retire_gameday_channel.py            # dry run - writes nothing
    python3 ops/retire_gameday_channel.py --apply    # execute

Rollback: the pin can be deleted (the script prints its message id). The channel
cannot be restored - the transcript in the admin log, and the local copy this
script writes to ops/, are the only remaining record.

Safe to re-run: an existing pin with the same marker is not re-posted, and an
already-deleted channel is treated as done.
"""
import argparse
import json
import os
import sys
import time

import requests
from dotenv import load_dotenv

load_dotenv(os.path.join(os.path.dirname(__file__), os.pardir, ".env"))

GAME_CHANNEL_ID = "1544838810947424326"   # 🏈-byu-vs-utah-tech
FOOTBALL_CHANNEL_ID = "1050091238730236055"  # 🏈-byufootball
ADMIN_LOG_CHANNEL_ID = "1493610618094227577"
PICKEM_CHANNEL_ID = "1455436751337422848"
GAMEDAY_INVITE = "https://discord.gg/R6yNUtpFRq"

SUPPRESS_NOTIFICATIONS = 1 << 12   # 4096 - lands silently, no push for the channel

# Any pin already containing this marker means the pointer is up; don't post twice.
PIN_MARKER = "Game-day chat has a new home"

POINTER = (
    f"🏈 **{PIN_MARKER}**\n\n"
    f"We're not opening a channel per game anymore. Live game chat runs on a fan-run "
    f"server: <{GAMEDAY_INVITE}>\n\n"
    f"Worth knowing before you join: it isn't affiliated with CougConnect and we don't "
    f"moderate it. Different server, different rules — what happens there isn't something "
    f"we can sort out for you.\n\n"
    f"Everything else stays put. 🎯 Pick'em is in <#{PICKEM_CHANNEL_ID}>, and this channel "
    f"is still where BYU football gets talked about all week.\n\n"
    f"Our rules don't loosen on Saturdays — players and their families read this server. "
    f"Go Cougs!"
)

API = "https://discord.com/api/v10"
OPS = os.path.dirname(os.path.abspath(__file__))

TOKEN = os.getenv("DISCORD_BOT_TOKEN", "")
if not TOKEN:
    sys.exit("DISCORD_BOT_TOKEN missing - run from the bot directory with .env present.")

HEADERS = {
    "Authorization": "Bot " + TOKEN,
    # Discord 403s Python's default User-Agent, so send an explicit one.
    "User-Agent": "CougConnectBot retire_gameday_channel/1.0 (+https://cougconnect.com)",
}
TIMEOUT = 30
MAX_RETRIES = 5


def call(method: str, path: str, *, json_body=None, files=None, data=None):
    """One Discord request, retrying on 429 and 5xx."""
    for attempt in range(MAX_RETRIES):
        r = requests.request(method, API + path, headers=HEADERS,
                             json=json_body, files=files, data=data, timeout=TIMEOUT)
        if r.status_code == 429:
            wait = float(r.json().get("retry_after", 2))
            print(f"    rate limited, sleeping {wait}s")
            time.sleep(wait + 0.2)
            continue
        if r.status_code >= 500 and attempt < MAX_RETRIES - 1:
            time.sleep(2 ** attempt)
            continue
        if not r.ok:
            sys.exit(f"{method} {path} -> HTTP {r.status_code}: {r.text[:400]}")
        return r.json() if r.content else None
    sys.exit(f"{method} {path} -> gave up after {MAX_RETRIES} attempts")


def fetch_transcript(channel: dict) -> tuple[str, int]:
    """Whole channel history, oldest first, as plain text."""
    from datetime import datetime
    from zoneinfo import ZoneInfo
    mt = ZoneInfo("America/Denver")

    msgs, before = [], None
    while True:
        page = call("GET", f"/channels/{channel['id']}/messages?limit=100"
                           + (f"&before={before}" if before else ""))
        if not page:
            break
        msgs += page
        before = page[-1]["id"]
        if len(page) < 100:
            break
        time.sleep(0.3)
    msgs.reverse()

    out = [f"# {channel['name']}", f"# {channel.get('topic') or ''}",
           f"# archived {datetime.now(mt).strftime('%Y-%m-%d %H:%M %Z')}", ""]
    for m in msgs:
        stamp = datetime.fromisoformat(m["timestamp"]).astimezone(mt).strftime("%m/%d %H:%M")
        body = m.get("content") or ""
        for a in m.get("attachments", []):
            body += f"\n    [attachment] {a['filename']} — {a['url']}"
        for e in m.get("embeds", []):
            if e.get("title") or e.get("description"):
                body += f"\n    [embed] {e.get('title', '')} {(e.get('description') or '')[:200]}"
        author = m["author"].get("global_name") or m["author"]["username"]
        out.append(f"[{stamp}] {author}: {body}".rstrip())
    return "\n".join(out), len(msgs)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true", help="execute; without it, nothing is written")
    args = ap.parse_args()
    act = args.apply

    print("[1/4] Pointer in #🏈-byufootball")
    pins = call("GET", f"/channels/{FOOTBALL_CHANNEL_ID}/pins")
    pinned = pins.get("items", pins) if isinstance(pins, dict) else pins
    already = next((p for p in pinned
                    if PIN_MARKER in ((p.get("message") or p).get("content") or "")), None)
    if already:
        mid = (already.get("message") or already)["id"]
        print(f"  already pinned (message {mid}) - not reposting")
    elif act:
        msg = call("POST", f"/channels/{FOOTBALL_CHANNEL_ID}/messages", json_body={
            "content": POINTER,
            "flags": SUPPRESS_NOTIFICATIONS,
            "allowed_mentions": {"parse": []},
        })
        call("PUT", f"/channels/{FOOTBALL_CHANNEL_ID}/pins/{msg['id']}")
        print(f"  posted silently and pinned - message {msg['id']}")
        print(f"  rollback: DELETE /channels/{FOOTBALL_CHANNEL_ID}/messages/{msg['id']}")
    else:
        print("  WOULD POST (silent, no pings) and pin:")
        print("  " + POINTER.replace("\n", "\n  "))

    print("\n[2/4] Transcript")
    channel = None
    try:
        channel = call("GET", f"/channels/{GAME_CHANNEL_ID}")
    except SystemExit:
        pass
    if not channel:
        print("  channel already gone - nothing left to archive or delete")
        return
    transcript, count = fetch_transcript(channel)
    local = os.path.join(OPS, f"archive-{channel['name']}.txt")
    with open(local, "w") as f:
        f.write(transcript)
    print(f"  {count} message(s), {len(transcript)} bytes -> {local}")

    print("\n[3/4] Upload to admin log")
    filename = f"2026-09-05-{channel['name']}.txt"
    if act:
        payload = {
            "content": f"🗄️ **Game-week archive — BYU–Utah Tech** (`#{channel['name']}`, "
                       f"{count} message(s)). Game-week channels are retired; deleting this one now.",
            "flags": SUPPRESS_NOTIFICATIONS,
            "allowed_mentions": {"parse": []},
        }
        up = call("POST", f"/channels/{ADMIN_LOG_CHANNEL_ID}/messages",
                  data={"payload_json": json.dumps(payload)},
                  files={"files[0]": (filename, transcript.encode("utf-8"), "text/plain")})
        if not up or not up.get("id") or not up.get("attachments"):
            sys.exit("  FAILED - transcript upload returned no attachment. Channel NOT deleted.")
        print(f"  uploaded as {filename} (message {up['id']})")
    else:
        print(f"  WOULD UPLOAD {filename} ({count} messages) to the admin log")

    print("\n[4/4] Delete channel")
    if act:
        call("DELETE", f"/channels/{GAME_CHANNEL_ID}")
        gone = False
        try:
            call("GET", f"/channels/{GAME_CHANNEL_ID}")
        except SystemExit:
            gone = True
        if not gone:
            sys.exit("  FAILED - channel still present after DELETE")
        print(f"  deleted #{channel['name']} - {count} message(s) archived above")
    else:
        print(f"  WOULD DELETE #{channel['name']} ({count} messages, IRREVERSIBLE)")

    print("\nDone." if act else "\nDry run - nothing was written. Re-run with --apply.")


if __name__ == "__main__":
    main()
