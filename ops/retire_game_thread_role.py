"""Retire the "🏈 Game Thread Pings" role now that game-week channels are gone.

Nothing pings this role any more - the game-week feature that owned it was removed
- but it is still offered in Channels & Roles and still sits on everyone who opted
in. Deleting the role alone would leave the onboarding prompt pointing at a role
that no longer exists, so the prompt option comes out first, then the role.

Order matters and each step is verified before the next one runs:

    1. snapshot every member holding the role      -> ops/game-thread-holders.json
    2. back up the whole onboarding config         -> ops/onboarding-backup.json
    3. PUT the onboarding config minus the option  (re-read to confirm)
    4. DELETE the role                             (re-read to confirm)

Usage:
    python3 ops/retire_game_thread_role.py            # dry run - writes nothing
    python3 ops/retire_game_thread_role.py --apply    # execute

Rollback: PUT ops/onboarding-backup.json back to /guilds/{id}/onboarding to restore
the prompt. The role itself cannot be undeleted - recreating it by name gives a new
id and an empty membership, so the 31 opt-ins in the holder snapshot would have to
be re-granted from that file.

Safe to re-run: a missing option and a missing role are both treated as done.
"""
import argparse
import json
import os
import sys
import time
import urllib.error
import urllib.request

from dotenv import load_dotenv

load_dotenv(os.path.join(os.path.dirname(__file__), os.pardir, ".env"))

GUILD_ID = "1050090756649537616"
ROLE_ID = "1540506842554433556"          # 🏈 Game Thread Pings

API = "https://discord.com/api/v10"
OPS = os.path.dirname(os.path.abspath(__file__))
HOLDERS_PATH = os.path.join(OPS, "game-thread-holders.json")
BACKUP_PATH = os.path.join(OPS, "onboarding-backup.json")

TOKEN = os.getenv("DISCORD_BOT_TOKEN", "")
if not TOKEN:
    sys.exit("DISCORD_BOT_TOKEN missing - run from the bot directory with .env present.")

HEADERS = {
    "Authorization": "Bot " + TOKEN,
    "Content-Type": "application/json",
    # Discord 403s Python's default urllib User-Agent, so send an explicit one.
    "User-Agent": "CougConnectBot retire_game_thread_role/1.0 (+https://cougconnect.com)",
}

TIMEOUT = 15
MAX_RETRIES = 5


def call(method: str, path: str, body: dict | None = None):
    """One Discord request, retrying on 429 and 5xx. Returns None for 204."""
    data = json.dumps(body).encode() if body is not None else None
    for attempt in range(MAX_RETRIES):
        req = urllib.request.Request(API + path, data=data, headers=HEADERS, method=method)
        try:
            with urllib.request.urlopen(req, timeout=TIMEOUT) as r:
                raw = r.read()
                return json.loads(raw) if raw else None
        except urllib.error.HTTPError as e:
            detail = e.read().decode()[:400]
            if e.code == 429:
                wait = float(json.loads(detail).get("retry_after", 2))
                print(f"    rate limited, sleeping {wait}s")
                time.sleep(wait + 0.2)
                continue
            if e.code >= 500 and attempt < MAX_RETRIES - 1:
                time.sleep(2 ** attempt)
                continue
            sys.exit(f"{method} {path} -> HTTP {e.code}: {detail}")
    sys.exit(f"{method} {path} -> gave up after {MAX_RETRIES} attempts")


def snapshot_holders() -> list[dict]:
    """Every member carrying the role, paged 1000 at a time."""
    after, holders, scanned = "0", [], 0
    while True:
        page = call("GET", f"/guilds/{GUILD_ID}/members?limit=1000&after={after}")
        if not page:
            break
        scanned += len(page)
        for m in page:
            if ROLE_ID in m.get("roles", []):
                u = m["user"]
                holders.append({
                    "id": u["id"],
                    "username": u["username"],
                    "display": m.get("nick") or u.get("global_name") or u["username"],
                })
        after = page[-1]["user"]["id"]
        if len(page) < 1000:
            break
        time.sleep(0.4)
    print(f"  scanned {scanned} guild members, {len(holders)} hold the role")
    return holders


def strip_option(cfg: dict) -> tuple[dict, list[str]]:
    """The onboarding config minus every option granting this role.

    PUT replaces the whole config, so every prompt and every surviving option is
    rebuilt here verbatim - dropping a field would silently erase it. The emoji
    object comes back from GET nested, but PUT wants it flattened.
    """
    prompts, removed = [], []
    for p in cfg["prompts"]:
        options = []
        for o in p["options"]:
            if ROLE_ID in o.get("role_ids", []):
                removed.append(f"{p['title']} -> {o['title']}")
                continue
            emoji = o.get("emoji") or {}
            options.append({
                "id": o["id"],
                "title": o["title"],
                "description": o.get("description") or "",
                "role_ids": o.get("role_ids", []),
                "channel_ids": o.get("channel_ids", []),
                "emoji_name": emoji.get("name"),
                "emoji_id": emoji.get("id"),
                "emoji_animated": bool(emoji.get("animated")),
            })
        prompts.append({
            "id": p["id"],
            "title": p["title"],
            "options": options,
            "single_select": p["single_select"],
            "required": p["required"],
            "in_onboarding": p["in_onboarding"],
            "type": p["type"],
        })
    payload = {
        "prompts": prompts,
        "default_channel_ids": cfg["default_channel_ids"],
        "enabled": cfg["enabled"],
        "mode": cfg["mode"],
    }
    return payload, removed


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true", help="execute; without it, nothing is written")
    args = ap.parse_args()

    roles = call("GET", f"/guilds/{GUILD_ID}/roles")
    role = next((r for r in roles if r["id"] == ROLE_ID), None)
    print(f"Role {ROLE_ID}: " + (f"{role['name']!r}" if role else "already gone"))

    print("\n[1/4] Snapshotting holders")
    holders = snapshot_holders() if role else []
    if role:
        with open(HOLDERS_PATH, "w") as f:
            json.dump(holders, f, indent=1)
        print(f"  -> {HOLDERS_PATH}")

    print("\n[2/4] Backing up onboarding config")
    cfg = call("GET", f"/guilds/{GUILD_ID}/onboarding")
    with open(BACKUP_PATH, "w") as f:
        json.dump(cfg, f, indent=1)
    print(f"  -> {BACKUP_PATH}")

    payload, removed = strip_option(cfg)
    print("\n[3/4] Onboarding prompt option")
    if not removed:
        print("  no option grants this role - nothing to remove")
    else:
        for r in removed:
            print(f"  REMOVE: {r}")
        for p in payload["prompts"]:
            print(f"    after: {p['title']!r} keeps {len(p['options'])} option(s)")
        if args.apply:
            call("PUT", f"/guilds/{GUILD_ID}/onboarding", payload)
            check, still = strip_option(call("GET", f"/guilds/{GUILD_ID}/onboarding"))
            if still:
                sys.exit(f"  FAILED - option still present: {still}. Role NOT deleted.")
            print("  removed, confirmed by re-read")

    print("\n[4/4] Role delete")
    if not role:
        print("  already gone")
    elif args.apply:
        call("DELETE", f"/guilds/{GUILD_ID}/roles/{ROLE_ID}")
        after = call("GET", f"/guilds/{GUILD_ID}/roles")
        if any(r["id"] == ROLE_ID for r in after):
            sys.exit("  FAILED - role still present after DELETE")
        print(f"  deleted {role['name']!r} - removed from {len(holders)} member(s)")
    else:
        print(f"  WOULD DELETE {role['name']!r}, removing it from {len(holders)} member(s)")

    print("\nDone." if args.apply else "\nDry run - nothing was written. Re-run with --apply.")


if __name__ == "__main__":
    main()
