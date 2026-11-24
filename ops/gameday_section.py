"""
Opt-in "🏟️ GAME DAY" section (Eric, Nov 24 2026). Game-day channels open in their own category and only the
"Game Day" role (+ Admin/Mod/bot) can see them; members turn it off/on in Channels & Roles.

    python3 ops/gameday_section.py                 DRY RUN: shows everything that would change (no writes)
    python3 ops/gameday_section.py --setup         1) role "Game Day"  2) category "🏟️ GAME DAY"
                                                   3) onboarding: + "Game-day channels" option in "What do you want to follow?"
                                                      and the old ping option renamed "Game day chats" -> "Game-day pings"
    python3 ops/gameday_section.py --grant         4) give the role to every current paid member (checkpointed, resumable)
    python3 ops/gameday_section.py --rollback      remove the role from everyone in the checkpoint, restore the onboarding
                                                   backup, delete the category (only if empty) and the role

Rate limits / failure handling (bulk grant, ~915 members):
- PUT /guilds/{g}/members/{u}/roles/{r}: per-route bucket (~10 per 10 s observed) + global 50/s. We send one call
  every 1.1 s, obey X-RateLimit-Remaining/Reset-After, and on 429 sleep retry_after then retry the same member.
- Checkpoint (ops/gameday-role-grants.json) is written after EVERY successful call; a rerun skips those ids, so a
  crash never double-sends. 404 (member left) = skipped + logged; 403 / other errors = stop.
- Adding a role sends members no notification. Audit-log reason on every call.
"""
import json, os, sys, time, urllib.error, urllib.request

HERE = os.path.dirname(os.path.abspath(__file__)); ROOT = os.path.dirname(HERE)
G = "1050090756649537616"
ROLE_NAME, CAT_NAME = "Game Day", "🏟️ GAME DAY"
FOLLOW_PROMPT, PING_PROMPT = "What do you want to follow?", "What do you want to get pinged for?"
OPT_TITLE, OPT_DESC = "Game-day channels", "Show the live BYU vs [opponent] channel for every football & basketball game"
OLD_PING_TITLE, NEW_PING_TITLE = "Game day chats", "Game-day pings"
FOOTBALL_CAT = "1395637847041511506"
# who gets the role now = everyone who can see the sport channels today
GRANT_ROLES = {"1082893434827845643": "Gold Subscriber", "1082893184402722876": "Silver Subscriber",
               "1545431633115611147": "Royal", "1359380692202553434": "Cougar Insider",
               "1359536444451983400": "Legacy Cougar Insider"}
UNSUB = "1082823895234650173"
KEEP_VIEW = {"1050102845866778734": "Admin", "1189796886073327616": "Moderator"}
CKPT = os.path.join(HERE, "gameday-role-grants.json")
OB_BACKUP = os.path.join(HERE, "onboarding-backup-gameday-section.json")
STATE = os.path.join(HERE, "gameday-section-ids.json")
TOK = next(l.split("=", 1)[1].strip().strip('"') for l in open(os.path.join(ROOT, ".env")) if l.startswith("DISCORD_BOT_TOKEN="))
VIEW = 1 << 10


def call(method, path, body=None, ok404=False):
    while True:
        req = urllib.request.Request("https://discord.com/api/v10" + path, method=method,
                                     data=json.dumps(body).encode() if body is not None else None,
                                     headers={"Authorization": "Bot " + TOK, "User-Agent": "DiscordBot (cougconnect, 1.0)",
                                              "Content-Type": "application/json", "X-Audit-Log-Reason": "Game Day section (opt-in)"})
        try:
            with urllib.request.urlopen(req, timeout=40) as r:
                raw = r.read()
                if r.headers.get("X-RateLimit-Remaining") == "0":
                    time.sleep(float(r.headers.get("X-RateLimit-Reset-After", "1")) + 0.2)
                return json.loads(raw) if raw else None
        except urllib.error.HTTPError as e:
            txt = e.read().decode()
            if e.code == 429:
                time.sleep(float(json.loads(txt).get("retry_after", 2)) + 0.5); continue
            if e.code == 404 and ok404:
                return "404"
            sys.exit(f"STOP {method} {path} -> {e.code}: {txt[:300]}")


def state():
    return json.load(open(STATE)) if os.path.exists(STATE) else {}


def members():
    out, after = [], "0"
    while True:
        page = call("GET", f"/guilds/{G}/members?limit=1000&after={after}")
        out += [m for m in page if not m["user"].get("bot")]
        if len(page) < 1000: return out
        after = page[-1]["user"]["id"]; time.sleep(0.5)


def targets(ms, role_id=None):
    return [m for m in ms if any(r in GRANT_ROLES for r in m["roles"]) and (not role_id or role_id not in m["roles"])]


def rebuild_onboarding(cfg, role_id):
    prompts, changes = [], []
    for p in cfg["prompts"]:
        opts = []
        for o in p["options"]:
            em = o.get("emoji") or {}; title = o["title"]
            if p["title"] == PING_PROMPT and title == OLD_PING_TITLE:
                title = NEW_PING_TITLE; changes.append(f"rename ping option {OLD_PING_TITLE!r} -> {NEW_PING_TITLE!r}")
            opts.append({"id": o["id"], "title": title, "description": o.get("description") or "",
                         "role_ids": o.get("role_ids", []), "channel_ids": o.get("channel_ids", []),
                         "emoji_name": em.get("name"), "emoji_id": em.get("id"), "emoji_animated": bool(em.get("animated"))})
        if p["title"] == FOLLOW_PROMPT and not any(role_id in o["role_ids"] for o in opts):
            opts.append({"title": OPT_TITLE, "description": OPT_DESC, "role_ids": [role_id], "channel_ids": [],
                         "emoji_name": "🏟️", "emoji_id": None, "emoji_animated": False})
            changes.append(f"add {OPT_TITLE!r} to {FOLLOW_PROMPT!r} -> role {ROLE_NAME}")
        prompts.append({"id": p["id"], "title": p["title"], "options": opts, "single_select": p["single_select"],
                        "required": p["required"], "in_onboarding": p["in_onboarding"], "type": p["type"]})
    return {"prompts": prompts, "default_channel_ids": cfg["default_channel_ids"], "enabled": cfg["enabled"],
            "mode": cfg["mode"]}, changes


def cat_overwrites(role_id, bot_role_ids):
    ow = [{"id": G, "type": 0, "allow": "0", "deny": str(VIEW)}, {"id": role_id, "type": 0, "allow": str(VIEW), "deny": "0"}]
    ow += [{"id": r, "type": 0, "allow": str(VIEW), "deny": "0"} for r in list(KEEP_VIEW) + bot_role_ids]
    return ow


def main():
    mode = next((a[2:] for a in sys.argv[1:] if a.startswith("--")), "dry")
    st = state()
    roles = {r["id"]: r for r in call("GET", f"/guilds/{G}/roles")}
    me = call("GET", "/users/@me")
    bot_member = call("GET", f"/guilds/{G}/members/{me['id']}")
    bot_roles = [r for r in bot_member["roles"]]
    role = next((r for r in roles.values() if r["name"] == ROLE_NAME), None)
    chans = call("GET", f"/guilds/{G}/channels")
    cat = next((c for c in chans if c["type"] == 4 and c["name"] == CAT_NAME), None)
    cfg = call("GET", f"/guilds/{G}/onboarding")

    if mode == "dry":
        ms = members(); tg = targets(ms)
        print(f"DRY RUN (nothing written)\n1) role {ROLE_NAME!r}: {'exists ' + role['id'] if role else 'CREATE (no permissions, not hoisted, not mentionable)'}")
        fcat = next(c for c in chans if c["id"] == FOOTBALL_CAT)
        print(f"2) category {CAT_NAME!r}: {'exists ' + cat['id'] if cat else 'CREATE at position ' + str(fcat['position']) + ' (just above ' + fcat['name'] + ')'}")
        print("   visible to: Game Day role, " + ", ".join(KEEP_VIEW.values()) + ", the bot's roles (" +
              ", ".join(roles[r]["name"] for r in bot_roles) + "); @everyone denied")
        _, ch = rebuild_onboarding(cfg, role["id"] if role else "NEW")
        print("3) onboarding: " + ("; ".join(ch) or "no change") + f"  (backup -> {os.path.basename(OB_BACKUP)} before PUT)")
        by = {}
        for m in tg:
            k = next(GRANT_ROLES[r] for r in GRANT_ROLES if r in m["roles"]); by[k] = by.get(k, 0) + 1
        both_unsub = sum(1 for m in tg if UNSUB in m["roles"])
        print(f"4) grant {ROLE_NAME!r} to {len(tg)} members: {by}  (of {len(ms)} humans; {both_unsub} also hold Unsubscribed)")
        print(f"   ~{len(tg) * 1.15 / 60:.0f} min at 1 call / 1.1 s; checkpoint {os.path.basename(CKPT)}; no notifications")
        print("   sample:", ", ".join((m.get("nick") or m["user"].get("global_name") or m["user"]["username"]) for m in tg[:8]), "…")
        print("5) Railway after grant: GAMEDAY_ROLE_ID=<role> GAMEDAY_CATEGORY_ID=<category> (bot switches to the section)")
        return

    if mode == "setup":
        if not role:
            role = call("POST", f"/guilds/{G}/roles", {"name": ROLE_NAME, "permissions": "0", "hoist": False, "mentionable": False})
        print("role", role["id"])
        if not cat:
            fcat = next(c for c in chans if c["id"] == FOOTBALL_CAT)
            cat = call("POST", f"/guilds/{G}/channels", {"name": CAT_NAME, "type": 4,
                                                         "permission_overwrites": cat_overwrites(role["id"], bot_roles)})
            call("PATCH", f"/guilds/{G}/channels", [{"id": cat["id"], "position": fcat["position"]}])
        print("category", cat["id"])
        if not os.path.exists(OB_BACKUP):
            json.dump(cfg, open(OB_BACKUP, "w"), indent=1)
        payload, ch = rebuild_onboarding(cfg, role["id"])
        if ch:
            call("PUT", f"/guilds/{G}/onboarding", payload)
        after = call("GET", f"/guilds/{G}/onboarding")
        n0, n1 = sum(len(p["options"]) for p in cfg["prompts"]), sum(len(p["options"]) for p in after["prompts"])
        print("onboarding:", "; ".join(ch) or "no change", f"| options {n0} -> {n1}, prompts {len(cfg['prompts'])} -> {len(after['prompts'])}")
        json.dump({"role_id": role["id"], "category_id": cat["id"]}, open(STATE, "w"))
        print(f"GAMEDAY_ROLE_ID={role['id']} GAMEDAY_CATEGORY_ID={cat['id']}")
        return

    if mode == "grant":
        rid = st.get("role_id") or sys.exit("run --setup first")
        done = json.load(open(CKPT)) if os.path.exists(CKPT) else {"granted": [], "left": []}
        skip = set(done["granted"]) | set(done["left"])
        tg = [m for m in targets(members(), rid) if m["user"]["id"] not in skip]
        print(f"granting to {len(tg)} (already done {len(done['granted'])})")
        for i, m in enumerate(tg, 1):
            uid = m["user"]["id"]
            r = call("PUT", f"/guilds/{G}/members/{uid}/roles/{rid}", ok404=True)
            (done["left"] if r == "404" else done["granted"]).append(uid)
            json.dump(done, open(CKPT, "w"))
            if i % 50 == 0: print(f"  {i}/{len(tg)}")
            time.sleep(1.1)
        print(f"DONE granted={len(done['granted'])} left={len(done['left'])}")
        return

    if mode == "rollback":
        rid = st.get("role_id")
        done = json.load(open(CKPT)) if os.path.exists(CKPT) else {"granted": []}
        for uid in done["granted"]:
            call("DELETE", f"/guilds/{G}/members/{uid}/roles/{rid}", ok404=True); time.sleep(1.1)
        if os.path.exists(OB_BACKUP):
            payload, _ = rebuild_onboarding(json.load(open(OB_BACKUP)), "__none__")
            for p in payload["prompts"]:   # backup = pre-change config: drop the follow option we'd re-add, keep titles
                p["options"] = [o for o in p["options"] if "__none__" not in o["role_ids"]]
                for o in p["options"]:
                    if p["title"] == PING_PROMPT and o["title"] == NEW_PING_TITLE: o["title"] = OLD_PING_TITLE
            call("PUT", f"/guilds/{G}/onboarding", payload)
        if st.get("category_id"):
            kids = [c for c in call("GET", f"/guilds/{G}/channels") if c.get("parent_id") == st["category_id"]]
            if kids: print("category not empty, left in place:", [c["name"] for c in kids])
            else: call("DELETE", f"/channels/{st['category_id']}", ok404=True)
        if rid: call("DELETE", f"/guilds/{G}/roles/{rid}", ok404=True)
        print("rolled back. Also: railway variables delete GAMEDAY_ROLE_ID / GAMEDAY_CATEGORY_ID")


if __name__ == "__main__":
    main()
