"""READ-ONLY audit for the 'Game Day' section plan: reference-channel overwrites, role member counts, onboarding prompts."""
import json, os, sys, time, urllib.request, collections
ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__))); G = "1050090756649537616"
TOK = next(l.split("=", 1)[1].strip().strip('"') for l in open(os.path.join(ROOT, ".env")) if l.startswith("DISCORD_BOT_TOKEN="))
def get(p):
    r = urllib.request.Request("https://discord.com/api/v10" + p, headers={"Authorization": "Bot " + TOK, "User-Agent": "DiscordBot (cougconnect, 1.0)"})
    return json.load(urllib.request.urlopen(r))
roles = {r["id"]: r for r in get(f"/guilds/{G}/roles")}
for cid in ("1050091238730236055", "1050104741692178432"):
    ch = get(f"/channels/{cid}"); print(f"\n#{ch['name']} parent={ch.get('parent_id')}")
    for o in ch["permission_overwrites"]:
        nm = roles.get(o["id"], {}).get("name", "member " + o["id"]) if o["type"] == 0 else "member " + o["id"]
        a, d = int(o["allow"]), int(o["deny"])
        print(f"  {nm:<28} view={'ALLOW' if a & 1024 else 'DENY' if d & 1024 else '-':<5} send={'ALLOW' if a & 2048 else 'DENY' if d & 2048 else '-'}  allow={a} deny={d}")
cnt = collections.Counter(); n = 0; after = "0"
while True:
    page = get(f"/guilds/{G}/members?limit=1000&after={after}")
    if not page: break
    for m in page:
        if m["user"].get("bot"): continue
        n += 1; cnt.update(m["roles"])
    after = page[-1]["user"]["id"]; time.sleep(0.5)
    if len(page) < 1000: break
print(f"\nhumans in server: {n}")
for rid, c in cnt.most_common():
    nm = roles.get(rid, {}).get("name", rid)
    if any(k in nm.lower() for k in ("gold", "silver", "royal", "insider", "unsub", "game", "admin", "mod")): print(f"  {nm:<30} {c}  ({rid})")
ob = get(f"/guilds/{G}/onboarding"); print(f"\nonboarding enabled={ob.get('enabled')} mode={ob.get('mode')}")
for p in ob["prompts"]: print(f"  [{p['type']}] {p['title']} ({len(p['options'])} options, required={p.get('required')}, in_onboarding={p.get('in_onboarding')})")
