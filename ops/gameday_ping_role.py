"""
Create the opt-in "Game Day Pings" role and offer it in Channels & Roles.

    python3 ops/gameday_ping_role.py            # dry run: shows what would change
    python3 ops/gameday_ping_role.py --apply    # does it

Steps (--apply):
  1. create role "Game Day Pings" (no permissions, not hoisted, not mentionable by members)
  2. back up the whole onboarding config -> ops/onboarding-backup-gameday-pings.json
  3. PUT the config with one new option in the ping prompt (re-read to confirm)

PUT /guilds/{id}/onboarding REPLACES the whole config, so every prompt and option
is rebuilt verbatim, emoji flattened (GET nests it, PUT wants it flat).

Rollback: PUT the backup file back to /guilds/{id}/onboarding, then delete the role.
Nobody is given the role here, so nothing notifies anyone.
"""

import argparse
import json
import os
import sys
import urllib.error
import urllib.request

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)
GUILD_ID = "1050090756649537616"
ROLE_NAME = "Game Day Pings"
PING_PROMPT_TITLE = "What do you want to get pinged for?"
OPTION_TITLE = "Game day chats"
OPTION_DESC = "A ping when each football and basketball game-day channel opens"
BACKUP_PATH = os.path.join(HERE, "onboarding-backup-gameday-pings.json")


def _token() -> str:
    with open(os.path.join(ROOT, ".env")) as f:
        for line in f:
            if line.startswith("DISCORD_BOT_TOKEN="):
                return line.split("=", 1)[1].strip().strip('"')
    sys.exit("DISCORD_BOT_TOKEN missing from .env")


TOKEN = _token()


def call(method: str, path: str, body=None):
    req = urllib.request.Request(
        "https://discord.com/api/v10" + path, method=method,
        data=json.dumps(body).encode() if body is not None else None,
        headers={"Authorization": "Bot " + TOKEN, "User-Agent": "DiscordBot (cougconnect, 1.0)",
                 "Content-Type": "application/json", "X-Audit-Log-Reason": "Game Day Pings opt-in role"},
    )
    try:
        with urllib.request.urlopen(req) as r:
            raw = r.read()
            return json.loads(raw) if raw else None
    except urllib.error.HTTPError as e:
        sys.exit(f"{method} {path} -> {e.code}: {e.read().decode()[:500]}")


def rebuild(cfg: dict, role_id: str | None) -> tuple[dict, bool]:
    """Config verbatim, plus the new option in the ping prompt. Returns (payload, added)."""
    prompts, added = [], False
    for p in cfg["prompts"]:
        options = []
        for o in p["options"]:
            emoji = o.get("emoji") or {}
            options.append({
                "id": o["id"], "title": o["title"], "description": o.get("description") or "",
                "role_ids": o.get("role_ids", []), "channel_ids": o.get("channel_ids", []),
                "emoji_name": emoji.get("name"), "emoji_id": emoji.get("id"),
                "emoji_animated": bool(emoji.get("animated")),
            })
        if p["title"] == PING_PROMPT_TITLE and role_id and not any(role_id in o["role_ids"] for o in options):
            options.append({
                "title": OPTION_TITLE, "description": OPTION_DESC, "role_ids": [role_id], "channel_ids": [],
                "emoji_name": "🏟️", "emoji_id": None, "emoji_animated": False,
            })
            added = True
        prompts.append({
            "id": p["id"], "title": p["title"], "options": options, "single_select": p["single_select"],
            "required": p["required"], "in_onboarding": p["in_onboarding"], "type": p["type"],
        })
    return {"prompts": prompts, "default_channel_ids": cfg["default_channel_ids"],
            "enabled": cfg["enabled"], "mode": cfg["mode"]}, added


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true")
    args = ap.parse_args()

    roles = call("GET", f"/guilds/{GUILD_ID}/roles")
    role = next((r for r in roles if r["name"] == ROLE_NAME), None)
    cfg = call("GET", f"/guilds/{GUILD_ID}/onboarding")
    prompt = next((p for p in cfg["prompts"] if p["title"] == PING_PROMPT_TITLE), None)
    if not prompt:
        sys.exit(f"Prompt {PING_PROMPT_TITLE!r} not found; nothing changed.")
    print(f"Ping prompt now: {[o['title'] for o in prompt['options']]}")
    print(f"Role {ROLE_NAME!r}: {'exists ' + role['id'] if role else 'would be created'}")

    if not args.apply:
        _, added = rebuild(cfg, role["id"] if role else "NEW")
        print(f"Would add option {OPTION_TITLE!r} -> {ROLE_NAME}" if added else "Option already present.")
        print("Dry run: nothing written.")
        return

    if not role:
        role = call("POST", f"/guilds/{GUILD_ID}/roles",
                    {"name": ROLE_NAME, "permissions": "0", "hoist": False, "mentionable": False})
        print(f"[1/3] created role {role['id']}")
    else:
        print(f"[1/3] role exists {role['id']}")

    with open(BACKUP_PATH, "w") as f:
        json.dump(cfg, f, indent=1)
    print(f"[2/3] backed up onboarding -> {BACKUP_PATH}")

    payload, added = rebuild(cfg, role["id"])
    if added:
        call("PUT", f"/guilds/{GUILD_ID}/onboarding", payload)
    after = call("GET", f"/guilds/{GUILD_ID}/onboarding")
    p2 = next(p for p in after["prompts"] if p["title"] == PING_PROMPT_TITLE)
    ok = any(role["id"] in o.get("role_ids", []) for o in p2["options"])
    n_before = sum(len(p["options"]) for p in cfg["prompts"])
    n_after = sum(len(p["options"]) for p in after["prompts"])
    print(f"[3/3] ping prompt now: {[o['title'] for o in p2['options']]}  "
          f"(options total {n_before} -> {n_after}, prompts {len(cfg['prompts'])} -> {len(after['prompts'])})")
    print("ROLE_ID=" + role["id"] if ok else "!! option NOT present after PUT; restore from backup")


if __name__ == "__main__":
    main()
