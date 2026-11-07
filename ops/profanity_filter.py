"""
Extend Discord's native "Block Custom Words" AutoMod rule (Eric, Nov 7 2026):
server-wide, block DISGUISED major profanity (f*ck, sh!t, b!tch, a$$hole, effing...)
and abbreviations (wtf, stfu, fml). Mild words (crap, damn, heck) stay allowed.
Discord's preset "Commonly Flagged Words" already blocks the plain spellings.

    python3 ops/profanity_filter.py            # dry run: tests patterns on samples + real chat, shows the change
    python3 ops/profanity_filter.py --apply    # PATCH the rule (backup first)
    python3 ops/profanity_filter.py --rollback # restore the backup

Backup: ops/automod-custom-words-backup.json (written before the first apply).
No pings: a blocked message is never posted; the author sees Discord's own notice.
"""
import json, os, re, sys, urllib.request

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
GUILD = "1050090756649537616"
RULE_NAME = "Block Custom Words"
BACKUP = os.path.join(ROOT, "ops", "automod-custom-words-backup.json")

# Fixes the broken one-keyword entry 'horseshit. shittingham' and adds abbreviations/variants.
KEYWORDS = ["a$$", "ass", "cuck", "hell", "horseshit", "shittingham",
            "wtf*", "stfu*", "fml", "effing", "effin", "fking*", "fkn", "fck*", "fcking*",
            "phuck*", "fuk", "fuks", "fukk*", "fukin*", "fuking*", "fuq*", "a$$hole*", "azzhole*",
            # Plain spellings + compounds: Discord's preset let "We're very fucked" through (Nov 7 game).
            "*fuck*", "*shit*", "bitch*", "asshole*", "motherf*", "dumbass*", "jackass*", "dipshit*"]

# Rust-regex (Discord). Disguised spellings: symbols/digits/repeats standing in for letters.
REGEX = [
    r"(?i)\bf+[\W_\d]*[u*@#v]+[\W_\d]*[c*k]+[\W_\d]*k*(?:ing|in|er|ed|s)?\b",   # f*ck, fvck, f@ck, f***, fuuuck
    r"(?i)\bf[*#@%]{2,}",                                                     # f***
    r"(?i)\bs+h+[i1!*y|]+t+(?:ty|s|ting|ter)?\b|\bs[*#@]{2,}t\b",              # sh!t, sh1t, shyt, s**t
    r"(?i)\bb+[i1!*|]+t*c+h+(?:es|ing|y)?\b|\bb[*#@]{2,}h\b",                  # b!tch, b1tch, b**h
    r"(?i)\ba[s$5*]{2,}h[o0*]+le?s?\b|\ba[*]{2,}\b",                           # a$$hole, a**hole, a**
]

SHOULD_BLOCK = ["We’re very fucked", "fucking refs", "that's bullshit", "clusterfuck", "shitty call", "Bitch please",
                "what an asshole", "motherfucker", "dumbass", "f*ck", "F**k this", "fvck", "f@ck off", "f***", "fuuuuck", "what the fck", "f**king refs",
                "sh!t", "sh1t call", "s**t", "shyt", "b!tch", "b1tch", "a$$hole", "a**hole", "wtf lol", "WTF",
                "wtfff", "stfu", "fml", "effing terrible", "fkn hell", "phuck", "fuk"]
SHOULD_ALLOW = ["Crap", "damn it", "heck yes", "that was a shot", "shift the line", "focus up", "fake punt",
                "fourth down", "fullback", "fuchs", "bitcoin", "sushi", "Hitchcock", "bench him? no", "fact check",
                "first down", "f-bomb", "shooting", "assist", "classic", "pass rush", "Shiite", "fudge",
                "shih tzu", "bach", "Bachmeier", "f u n", "fcs schools", "a few", "assume", "at&t"]


def token():
    for line in open(os.path.join(ROOT, ".env")):
        if line.startswith("DISCORD_BOT_TOKEN="):
            return line.split("=", 1)[1].strip().strip('"')


def call(method, path, body=None):
    req = urllib.request.Request("https://discord.com/api/v10" + path, method=method,
                                 data=json.dumps(body).encode() if body is not None else None,
                                 headers={"Authorization": "Bot " + token(), "User-Agent": "DiscordBot (cougconnect, 1.0)",
                                          "Content-Type": "application/json",
                                          "X-Audit-Log-Reason": "Profanity filter: disguised spellings + abbreviations"})
    with urllib.request.urlopen(req) as r:
        raw = r.read()
        return json.loads(raw) if raw else None


def kw_regex(k):
    # Discord keyword semantics: plain = whole word; trailing * = word starts with; leading * = ends with.
    core = re.escape(k.strip("*"))
    pre = r"" if k.startswith("*") else r"(?<![\w$])"
    post = r"" if k.endswith("*") else r"(?![\w$])"
    return re.compile(pre + core + post, re.I)


KW = [(k, kw_regex(k)) for k in KEYWORDS]
RX = [re.compile(p.replace("(?i)", ""), re.I) for p in REGEX]


def hits(text):
    out = [k for k, r in KW if r.search(text)]
    out += [f"regex{i+1}" for i, r in enumerate(RX) if r.search(text)]
    return out


def main():
    mode = "rollback" if "--rollback" in sys.argv else "apply" if "--apply" in sys.argv else "dry"
    rule = next(r for r in call("GET", f"/guilds/{GUILD}/auto-moderation/rules") if r["name"] == RULE_NAME)

    if mode == "rollback":
        b = json.load(open(BACKUP))
        call("PATCH", f"/guilds/{GUILD}/auto-moderation/rules/{rule['id']}", {"trigger_metadata": b["trigger_metadata"]})
        print("restored", b["trigger_metadata"]); return

    bad = [s for s in SHOULD_BLOCK if not hits(s)] + [f"{s} -> {hits(s)}" for s in SHOULD_ALLOW if hits(s)]
    print(f"samples: {len(SHOULD_BLOCK)} must-block, {len(SHOULD_ALLOW)} must-allow -> {'ALL OK' if not bad else 'PROBLEMS: ' + str(bad)}")

    # Real chat: what would have been blocked (false-positive check).
    chans = {"🏈-byufootball": "1050091238730236055", "🏀-byubasketball": "1050104741692178432", "🪧-general-chat": "1050165331894751314"}
    for c in call("GET", f"/guilds/{GUILD}/channels"):
        if "-byu-at-" in c["name"] or "-byu-vs-" in c["name"]:
            chans[c["name"]] = c["id"]
    total, flagged = 0, []
    for name, cid in chans.items():
        before = None
        for _ in range(5):
            page = call("GET", f"/channels/{cid}/messages?limit=100" + (f"&before={before}" if before else ""))
            if not page: break
            before = page[-1]["id"]
            for m in page:
                if m["author"].get("bot") or not m.get("content"): continue
                total += 1
                h = hits(m["content"])
                if h: flagged.append((name, h, m["content"][:90]))
    print(f"real chat: {total} member messages scanned, {len(flagged)} would be blocked:")
    for name, h, t in flagged: print(f"   #{name[:18]:18} {h} | {t!r}")

    new_meta = {"keyword_filter": KEYWORDS, "regex_patterns": REGEX, "allow_list": rule["trigger_metadata"].get("allow_list", [])}
    print("\nrule:", rule["name"], rule["id"])
    print("  keywords now:", rule["trigger_metadata"].get("keyword_filter"))
    print("  keywords new:", KEYWORDS)
    print("  regex now:", rule["trigger_metadata"].get("regex_patterns"), "-> new:", len(REGEX), "patterns")
    if mode != "apply" or bad:
        print("\nDRY RUN: nothing changed" if mode != "apply" else "\nNOT APPLIED: sample problems"); return
    if not os.path.exists(BACKUP):
        json.dump(rule, open(BACKUP, "w"), indent=1)
    call("PATCH", f"/guilds/{GUILD}/auto-moderation/rules/{rule['id']}", {"trigger_metadata": new_meta})
    after = next(r for r in call("GET", f"/guilds/{GUILD}/auto-moderation/rules") if r["id"] == rule["id"])
    print("APPLIED. now:", after["trigger_metadata"]["keyword_filter"], "| regex:", len(after["trigger_metadata"]["regex_patterns"]), "| backup:", BACKUP)


if __name__ == "__main__":
    main()
