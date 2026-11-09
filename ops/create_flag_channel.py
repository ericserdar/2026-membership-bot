"""Create the private #🚩-bot-flags channel next to #admin-moderators.

Copies #admin-moderators' privacy exactly (everyone hidden; Admin, Moderator
and the bot can see it), so only admins and moderators see bot flags.

Dry run by default — prints the plan, changes nothing:
    python3 ops/create_flag_channel.py
Create it for real (prints the new channel id for DISCORD_FLAG_LOG_CHANNEL_ID):
    python3 ops/create_flag_channel.py --apply

Rollback: delete the channel in Discord and unset DISCORD_FLAG_LOG_CHANNEL_ID
on Railway — the bot falls back to #admin-moderators on its own.
"""
import json
import os
import sys
import urllib.request

from dotenv import load_dotenv

load_dotenv(os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", ".env"))

GUILD_ID = "1050090756649537616"
TEMPLATE_CHANNEL_ID = "1189800941793333389"  # #admin-moderators
NAME = "🚩-bot-flags"
TOPIC = "Bot flags only: 🚩 member flags, AutoMod review cards, mood monitor, AutoMod weekly digest. Admins + mods only."
API = "https://discord.com/api/v10"
HEADERS = {
    "Authorization": "Bot " + os.environ["DISCORD_BOT_TOKEN"],
    "Content-Type": "application/json",
    "User-Agent": "CougConnectBot create_flag_channel/1.0",
}


def req(method, path, body=None):
    data = json.dumps(body).encode() if body is not None else None
    with urllib.request.urlopen(urllib.request.Request(API + path, data=data, headers=HEADERS, method=method)) as r:
        raw = r.read()
        return json.loads(raw) if raw else None


apply = "--apply" in sys.argv
roles = {r["id"]: r["name"] for r in req("GET", f"/guilds/{GUILD_ID}/roles")}
chans = req("GET", f"/guilds/{GUILD_ID}/channels")
existing = [c for c in chans if c["name"] == NAME]
if existing:
    print(f"#{NAME} already exists (id {existing[0]['id']}) — nothing to do.")
    sys.exit(0)

tpl = next(c for c in chans if c["id"] == TEMPLATE_CHANNEL_ID)
overwrites = [{"id": o["id"], "type": o["type"], "allow": o["allow"], "deny": o["deny"]} for o in tpl["permission_overwrites"]]
body = {
    "name": NAME,
    "type": 0,
    "topic": TOPIC,
    "parent_id": tpl.get("parent_id"),
    "position": tpl["position"] + 1,
    "permission_overwrites": overwrites,
}

print(f"{'CREATING' if apply else 'DRY RUN — would create'} #{NAME}")
print(f"  category: same as #{tpl['name']} ({tpl.get('parent_id')}), placed right below it")
for o in overwrites:
    who = "@everyone" if o["id"] == GUILD_ID else roles.get(o["id"], f"member {o['id']} (the bot)")
    print(f"  {who:30s} {'can see' if int(o['allow']) & (1 << 10) else 'HIDDEN'}")
print("  no messages posted, no pings")

if not apply:
    print("\nRe-run with --apply to create it.")
    sys.exit(0)

ch = req("POST", f"/guilds/{GUILD_ID}/channels", body)
print(f"\nCreated #{ch['name']} — id {ch['id']}")
print(f"Set on Railway: DISCORD_FLAG_LOG_CHANNEL_ID={ch['id']}")
