"""Create the "Royal" tier role and mirror Cougar Insider's channel access onto it.

Royal is a $20/mo tier with NO perks above Insider, so its Discord access must be
byte-for-byte identical to Insider's:

    For every channel, if Cougar Insider has a permission overwrite, give Royal an
    identical one (same allow, same deny). If Insider has no overwrite, Royal gets
    none.

That mirroring is exact in both directions. Channels where Insider is explicitly
DENIED (the "Gold and silver" stage) must inherit the deny; channels with no
Insider overwrite at all (gold-only, silver-members, Gold/Silver voice, admin)
must stay untouched so Royal remains locked out.

Usage:
    python3 ops/royal_role_setup.py            # dry run - prints the full plan, writes nothing
    python3 ops/royal_role_setup.py --apply    # execute
    python3 ops/royal_role_setup.py --verify   # diff Royal vs Insider across every channel

Safe to re-run: the role is looked up by name before being created, and each
overwrite is compared before being written. Applied channels are checkpointed to
ops/royal_overwrites.state.json so an interrupted run resumes where it stopped.
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
INSIDER_ROLE_ID = "1359380692202553434"   # Cougar Insider - the role we mirror
SILVER_ROLE_ID = "1082893184402722876"    # Silver Subscriber - Royal sits directly below this

ROYAL_NAME = "Royal"
ROYAL_COLOR = 0x1A3AFF                    # CougConnect brand royal blue

API = "https://discord.com/api/v10"
STATE_PATH = os.path.join(os.path.dirname(__file__), "royal_overwrites.state.json")

TOKEN = os.getenv("DISCORD_BOT_TOKEN", "")
if not TOKEN:
    sys.exit("DISCORD_BOT_TOKEN missing - run from the bot directory with .env present.")

HEADERS = {
    "Authorization": "Bot " + TOKEN,
    "Content-Type": "application/json",
    # Discord 403s Python's default urllib User-Agent, so send an explicit one.
    "User-Agent": "CougConnectBot royal_role_setup/1.0 (+https://cougconnect.com)",
}

SLEEP_BETWEEN_WRITES = 0.3   # ~3 req/s, well under Discord's 50 req/s global limit
MAX_RETRIES = 5
TIMEOUT = 15


def req(method, path, body=None, _attempt=1):
    """One Discord REST call with 429 backoff and timeout handling."""
    data = json.dumps(body).encode() if body is not None else None
    r = urllib.request.Request(API + path, data=data, headers=HEADERS, method=method)
    try:
        with urllib.request.urlopen(r, timeout=TIMEOUT) as resp:
            raw = resp.read()
            return json.loads(raw) if raw else None
    except urllib.error.HTTPError as e:
        if e.code == 429 and _attempt <= MAX_RETRIES:
            retry_after = 1.0
            try:
                retry_after = float(json.loads(e.read()).get("retry_after", 1.0))
            except Exception:
                pass
            wait = retry_after * (2 ** (_attempt - 1))
            print(f"    429 rate limited - waiting {wait:.1f}s (attempt {_attempt}/{MAX_RETRIES})")
            time.sleep(wait)
            return req(method, path, body, _attempt + 1)
        raise
    except TimeoutError:
        # A timeout may mean the write landed. Never blind-retry a write - the
        # caller re-reads channel state to decide.
        raise


def load_state():
    if os.path.exists(STATE_PATH):
        with open(STATE_PATH) as f:
            return json.load(f)
    return {"role_id": None, "positioned": False, "applied": []}


def save_state(state):
    with open(STATE_PATH, "w") as f:
        json.dump(state, f, indent=2)


def get_guild():
    roles = req("GET", f"/guilds/{GUILD_ID}/roles")
    channels = req("GET", f"/guilds/{GUILD_ID}/channels")
    return roles, channels


def insider_overwrite(channel):
    for o in channel.get("permission_overwrites", []):
        if o["id"] == INSIDER_ROLE_ID:
            return o
    return None


def role_overwrite(channel, role_id):
    for o in channel.get("permission_overwrites", []):
        if o["id"] == role_id:
            return o
    return None


def chan_label(c, cats):
    parent = cats.get(c.get("parent_id"))
    return f"{parent} / {c['name']}" if parent else c["name"]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true", help="execute (default is dry run)")
    ap.add_argument("--verify", action="store_true", help="diff Royal against Insider and exit")
    args = ap.parse_args()

    roles, channels = get_guild()
    cats = {c["id"]: c["name"] for c in channels if c["type"] == 4}
    by_name = {r["name"]: r for r in roles}
    insider_role = next(r for r in roles if r["id"] == INSIDER_ROLE_ID)
    silver_role = next(r for r in roles if r["id"] == SILVER_ROLE_ID)
    royal = by_name.get(ROYAL_NAME)
    state = load_state()

    # ---------- verify mode ----------
    if args.verify:
        if not royal:
            sys.exit(f"No '{ROYAL_NAME}' role exists yet - nothing to verify.")
        diffs = []
        for c in channels:
            ins = insider_overwrite(c)
            roy = role_overwrite(c, royal["id"])
            if ins is None and roy is None:
                continue
            if ins is None and roy is not None:
                diffs.append((chan_label(c, cats), "Royal has an overwrite Insider does not"))
            elif ins is not None and roy is None:
                diffs.append((chan_label(c, cats), "MISSING - Insider has an overwrite, Royal does not"))
            elif str(ins["allow"]) != str(roy["allow"]) or str(ins["deny"]) != str(roy["deny"]):
                diffs.append((chan_label(c, cats),
                              f"MISMATCH insider(a={ins['allow']},d={ins['deny']}) "
                              f"royal(a={roy['allow']},d={roy['deny']})"))
        mirrored = sum(1 for c in channels if insider_overwrite(c))
        print(f"Royal role id: {royal['id']}  position {royal['position']} "
              f"(Silver is {silver_role['position']})")
        print(f"Channels with an Insider overwrite: {mirrored}")
        if diffs:
            print(f"\n{len(diffs)} DIFF(S):")
            for name, why in diffs:
                print(f"  {name}: {why}")
            sys.exit(1)
        print(f"\nPASS - Royal mirrors Insider exactly across all {len(channels)} channels.")
        return

    # ---------- plan ----------
    to_write, already, skipped = [], [], []
    for c in channels:
        ins = insider_overwrite(c)
        if ins is None:
            skipped.append(chan_label(c, cats))
            continue
        target = (str(ins["allow"]), str(ins["deny"]))
        cur = role_overwrite(c, royal["id"]) if royal else None
        if cur and (str(cur["allow"]), str(cur["deny"])) == target:
            already.append(chan_label(c, cats))
        else:
            to_write.append((c, chan_label(c, cats), target))

    print("=" * 78)
    print(f"{'APPLY' if args.apply else 'DRY RUN'} - Royal role setup")
    print("=" * 78)
    if royal:
        print(f"\n1. Role '{ROYAL_NAME}' already exists (id {royal['id']}, position {royal['position']})")
    else:
        print(f"\n1. CREATE role '{ROYAL_NAME}'  color #{ROYAL_COLOR:06X}  hoist=True  mentionable=False")
        print(f"   permissions = {insider_role['permissions']} (copied from Cougar Insider)")
    print(f"2. POSITION directly below Silver Subscriber (Silver is at {silver_role['position']})")
    print(f"\n3. MIRROR {len(to_write)} channel overwrite(s) from Cougar Insider:\n")
    for _, label, (allow, deny) in to_write:
        note = "  <-- DENY inherited" if deny != "0" and allow == "0" else ""
        print(f"     {label:<52} allow={allow:<16} deny={deny}{note}")
    if already:
        print(f"\n   ({len(already)} channel(s) already correct, will be skipped)")
    print(f"\n4. LEAVE UNTOUCHED - {len(skipped)} channel(s) with no Insider overwrite,")
    print("   so Royal stays locked out of them exactly as Insider is:")
    for label in skipped:
        print(f"     {label}")

    if not args.apply:
        print("\n" + "=" * 78)
        print("Dry run only - nothing written. Re-run with --apply to execute.")
        print("=" * 78)
        return

    # ---------- apply ----------
    print("\n" + "=" * 78)
    if not royal:
        royal = req("POST", f"/guilds/{GUILD_ID}/roles", {
            "name": ROYAL_NAME,
            "color": ROYAL_COLOR,
            "hoist": True,
            "mentionable": False,
            "permissions": str(insider_role["permissions"]),
        })
        print(f"Created role '{ROYAL_NAME}' id={royal['id']}")
        state["role_id"] = royal["id"]
        save_state(state)
        time.sleep(SLEEP_BETWEEN_WRITES)
    else:
        state["role_id"] = royal["id"]
        save_state(state)

    if not state.get("positioned"):
        req("PATCH", f"/guilds/{GUILD_ID}/roles",
            [{"id": royal["id"], "position": silver_role["position"]}])
        print(f"Positioned '{ROYAL_NAME}' directly below Silver Subscriber")
        state["positioned"] = True
        save_state(state)
        time.sleep(SLEEP_BETWEEN_WRITES)

    done = set(state.get("applied", []))
    ok = 0
    for c, label, (allow, deny) in to_write:
        if c["id"] in done:
            print(f"  skip (checkpointed): {label}")
            continue
        try:
            req("PUT", f"/channels/{c['id']}/permissions/{royal['id']}",
                {"type": 0, "allow": allow, "deny": deny})
            ok += 1
            print(f"  ok: {label}")
        except urllib.error.HTTPError as e:
            print(f"  FAILED: {label} - HTTP {e.code} {e.read()[:200]!r}")
            continue
        except TimeoutError:
            # Do not blind-retry. Re-read the channel to see whether it landed.
            print(f"  TIMEOUT: {label} - re-reading to confirm")
            fresh = req("GET", f"/channels/{c['id']}")
            cur = role_overwrite(fresh, royal["id"])
            if cur and (str(cur["allow"]), str(cur["deny"])) == (allow, deny):
                print("    -> it landed")
                ok += 1
            else:
                print("    -> did NOT land, leaving for the next run")
                continue
        done.add(c["id"])
        state["applied"] = sorted(done)
        save_state(state)
        time.sleep(SLEEP_BETWEEN_WRITES)

    print("=" * 78)
    print(f"Done - {ok} overwrite(s) written, {len(already)} already correct.")
    print(f"Royal role id: {royal['id']}")
    print(f"\nSet this on Railway:  DISCORD_ROLE_ROYAL_ID={royal['id']}")
    print("Then verify with:     python3 ops/royal_role_setup.py --verify")


if __name__ == "__main__":
    main()
