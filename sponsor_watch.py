"""
Sponsor watch — intake on signup, alerts on cancel / lapse.

Sponsors buy a MemberPress membership (Corporate 30211, Perks 30215) but are
rarely linked to Discord, so the member-sync path in bot.py never sees them.
This module looks at sponsor subscriptions directly and works out what changed:

    welcome    active sub with a completed payment  → email the sponsor the
                                                       intake form, alert Eric
    cancelled  sub cancelled/suspended              → alert Eric with the
                                                       paid-through date
    ended      paid coverage has run out            → alert Eric: remove now
                (an "active" sub whose last payment ran out more than
                LAPSE_GRACE_DAYS ago is a silent lapse — Stripe gave up on the
                card but MemberPress still says active)

Each notice_key() fires once; bot.py records it in sponsor_notices. "ended" is
keyed by its paid-through date, so a card that recovers and later lapses again
alerts again.
The check is a full sweep of the sponsor memberships (a handful of subs), run
shortly after any sponsor webhook and once a day, so a missed webhook only
delays an alert by a day.

Env:
    SPONSOR_MEMBERSHIP_IDS   default "30211,30215"
    SPONSOR_INTAKE_FORM_URL  default https://sponsors.zapier.app/
"""

import html
import os
from datetime import datetime, timedelta, timezone

import aiohttp

import memberpress as mp

SPONSOR_MEMBERSHIP_IDS = {int(x) for x in os.getenv("SPONSOR_MEMBERSHIP_IDS", "30211,30215").split(",") if x.strip().isdigit()}
INTAKE_FORM_URL = os.getenv("SPONSOR_INTAKE_FORM_URL", "https://sponsors.zapier.app/")

# Card retries run for days after a missed renewal; don't call it a lapse until they're over.
LAPSE_GRACE_DAYS = 3

TIER_NAMES = {30211: "Corporate Sponsorship", 30215: "Perks Sponsorship"}

# What the sponsor is told happens next, per tier.
TIER_CHECKLIST = {
    30211: [
        "Add your logo and link to the site and recap emails",
        "Schedule your first social media mention",
        "Coordinate any podcast or video sponsorship placements",
    ],
    30215: [
        "Add your logo and link to the CougConnect Perks page",
        "Set up your member discount or promo code",
        "Line up your turn as a Featured Perk",
    ],
}

# Every place a sponsor appears. Same list for adding and removing.
ROTATION_PLACES = [
    "X rotation: sponsors.json in github.com/ericserdar/cougconnect-routine (+ local ~/Claude/CougConnect/sponsors.json)",
    "Discord Wednesday spotlight: sponsors.json in the membership bot repo",
    "/perks page: $bs_partners in redesign/page-perks.php",
    "Mailchimp RSS email: partner row in rss-email-template.html",
    "Facebook: facebook-sponsor-post-template.md + any queued Buffer posts",
]


def tier_name(membership_id: int) -> str:
    return TIER_NAMES.get(membership_id, f"Sponsor membership {membership_id}")


def membership_id_of(obj) -> int | None:
    """Webhook payloads carry membership as an id or as an expanded object."""
    m = obj.get("membership") if isinstance(obj, dict) else None
    if isinstance(m, dict):
        m = m.get("id")
    try:
        return int(m)
    except (TypeError, ValueError):
        return None


def _parse_dt(value: str) -> datetime | None:
    if not value or value.startswith("0000"):
        return None
    try:
        return datetime.strptime(value, "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
    except ValueError:
        return None


async def _get(session: aiohttp.ClientSession, path: str, params: dict) -> list:
    async with session.get(mp._api(path), headers={"MEMBERPRESS-API-KEY": mp.MP_KEY}, params=params,
                           timeout=aiohttp.ClientTimeout(total=30)) as resp:
        if resp.status != 200:
            raise RuntimeError(f"MemberPress {path} → {resp.status}: {(await resp.text())[:200]}")
        return await resp.json()


async def fetch_sponsor_subs() -> list[dict]:
    """Every sponsor-tier subscription, each with paid_through (latest completed payment's expiry)."""
    subs = []
    async with aiohttp.ClientSession() as session:
        for mid in sorted(SPONSOR_MEMBERSHIP_IDS):
            for sub in await _get(session, "subscriptions", {"membership": mid, "per_page": 100}):
                if membership_id_of(sub) != mid:
                    continue  # the filter is trusted, but never act on another tier's sub
                txns = await _get(session, "transactions", {"subscription": sub["id"], "per_page": 10})
                paid = [t for t in txns if t.get("status") == "complete"]
                expiries = [_parse_dt(t.get("expires_at", "")) for t in paid]
                member = sub.get("member") if isinstance(sub.get("member"), dict) else {}
                subs.append({
                    "id": int(sub["id"]),
                    "membership_id": mid,
                    "status": sub.get("status", ""),
                    "price": sub.get("price", ""),
                    "created_at": sub.get("created_at", ""),
                    "member_id": member.get("id"),
                    "email": member.get("email", ""),
                    "name": " ".join(p for p in (member.get("first_name"), member.get("last_name")) if p) or member.get("username", ""),
                    "first_name": member.get("first_name") or "there",
                    "has_payment": bool(paid),
                    # None = never paid, or a lifetime payment with no expiry
                    "paid_through": max((e for e in expiries if e), default=None),
                    "lifetime": any(e is None for e in expiries),
                })
    return subs


def evaluate(subs: list[dict], now: datetime | None = None) -> list[tuple[str, dict]]:
    """(kind, sub) for every condition that is true right now. The caller dedupes."""
    now = now or datetime.now(timezone.utc)
    covered_members = {
        s["member_id"] for s in subs
        if s["status"] == "active" and (s["lifetime"] or (s["paid_through"] and s["paid_through"] > now))
    }
    out = []
    for s in subs:
        if s["status"] == "active" and s["has_payment"]:
            out.append(("welcome", s))
        if s["status"] in ("cancelled", "suspended") and s["has_payment"]:
            out.append(("cancelled", s))
        if s["has_payment"] and not s["lifetime"] and s["paid_through"]:
            grace = timedelta(days=LAPSE_GRACE_DAYS) if s["status"] == "active" else timedelta(0)
            if s["paid_through"] + grace < now and s["member_id"] not in covered_members:
                out.append(("ended", s))
    return out


def notice_key(kind: str, s: dict) -> str:
    if kind == "ended" and s["paid_through"]:
        return f"ended:{s['paid_through']:%Y-%m-%d}"
    return kind


def returning(sub: dict, subs: list[dict]) -> bool:
    """Paid on an earlier sponsor sub — a re-signup, who has already been through intake."""
    return any(s["id"] != sub["id"] and s["member_id"] == sub["member_id"] and s["has_payment"] for s in subs)


def replaced_by(sub: dict, subs: list[dict]) -> dict | None:
    """Another active sponsor sub for the same member — a re-signup or tier switch, not a loss."""
    for s in subs:
        if s["id"] != sub["id"] and s["member_id"] == sub["member_id"] and s["status"] == "active":
            return s
    return None


# ── Messages ──────────────────────────────────────────────────────────────────

def _date(d: datetime | None) -> str:
    return d.strftime("%b %-d, %Y") if d else "unknown"


def admin_message(kind: str, s: dict, subs: list[dict], intake_sent: bool | None = None) -> tuple[str, str]:
    """(subject, body) for the alert to Eric."""
    who = f"{s['name']} <{s['email']}>"
    head = f"{tier_name(s['membership_id'])} · ${s['price']}/mo · MemberPress sub {s['id']}"
    places = "\n".join(f"  - {p}" for p in ROTATION_PLACES)

    if kind == "welcome":
        if returning(s, subs):
            intake = "Returning sponsor (paid on an earlier sub) — intake email skipped."
        elif intake_sent is True:
            intake = f"Intake form emailed to {s['email']} ({INTAKE_FORM_URL})."
        elif intake_sent is False:
            intake = f"Intake email FAILED to send — forward {INTAKE_FORM_URL} by hand."
        else:
            intake = f"Intake email is OFF (SPONSOR_INTAKE_EMAIL) — send them {INTAKE_FORM_URL} by hand."
        return (f"New sponsor: {s['name'] or s['email']}",
                f"{who}\n{head}\nPaid through {_date(s['paid_through'])}.\n\n{intake}\n"
                f"Submissions land in the Zapier sponsor table. Once it's in, add them to:\n{places}")

    other = replaced_by(s, subs)
    if other:
        note = (f"They also hold active sponsor sub {other['id']} ({tier_name(other['membership_id'])}) — "
                f"likely a re-signup or tier switch. Probably nothing to remove; check the rate.")
    else:
        note = None

    if kind == "cancelled":
        body = f"{who}\n{head}\nStatus: {s['status']}. Paid through {_date(s['paid_through'])}.\n\n"
        body += note or (f"Keep them in rotation until {_date(s['paid_through'])} — you'll get a second alert "
                         f"once that runs out. Then remove from:\n{places}")
        return (f"Sponsor cancelled: {s['name'] or s['email']}", body)

    # ended
    lapse = s["status"] == "active"
    body = f"{who}\n{head}\n"
    body += (f"MemberPress still says ACTIVE, but the last payment only covered through {_date(s['paid_through'])} — "
             f"the card has stopped paying (silent lapse).\n\n" if lapse else
             f"Paid coverage ended {_date(s['paid_through'])}.\n\n")
    body += note or f"Remove them from:\n{places}"
    return ((f"Sponsor payment lapsed: " if lapse else "Sponsor ended — remove from rotation: ") + (s["name"] or s["email"]), body)


def intake_email(s: dict) -> tuple[str, str, str]:
    """(subject, text, html) for the sponsor."""
    tier = tier_name(s["membership_id"])
    items = TIER_CHECKLIST.get(s["membership_id"], TIER_CHECKLIST[30211])
    first = s["first_name"]
    subject = "CougConnect Sponsorship Intake Form"

    text = (
        f"Hey {first},\n\n"
        f"Welcome to the CougConnect {tier} program! Your support helps us create more BYU athlete "
        f"content and connect fans and businesses across Cougar Nation.\n\n"
        f"To get your sponsorship set up, please fill out our quick Sponsor Intake Form "
        f"(logo, website, offer details and social handles):\n{INTAKE_FORM_URL}\n\n"
        f"Once it's in, we'll:\n" + "".join(f"  - {i}\n" for i in items) +
        f"\nQuestions or ideas for how you'd like to be featured? Just reply to this email.\n\n"
        f"Go Cougs,\nThe CougConnect Team\nadmin@cougconnect.com · https://cougconnect.com\n"
    )

    esc = html.escape
    rows = "".join(
        f'<tr><td style="vertical-align:top;width:22px;font-size:18px;line-height:1.2;color:#003DA5;">&#10004;</td>'
        f'<td style="padding:0 0 8px;color:#0b1234;">{esc(i)}</td></tr>' for i in items
    )
    font = "system-ui,-apple-system,Segoe UI,Roboto,Helvetica,Arial,sans-serif"
    body_html = f"""<!doctype html>
<html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>Welcome to CougConnect Sponsorship</title></head>
<body style="margin:0;padding:0;background:#f4f7ff;">
<div style="display:none;font-size:0;line-height:0;max-height:0;opacity:0;overflow:hidden;">Welcome to CougConnect {esc(tier)} — one quick form to get you set up.</div>
<table role="presentation" cellpadding="0" cellspacing="0" border="0" width="100%" style="background:#f4f7ff;"><tr><td align="center" style="padding:24px 12px;">
<table role="presentation" cellpadding="0" cellspacing="0" border="0" width="600" style="width:100%;max-width:600px;background:#ffffff;border-radius:14px;overflow:hidden;">
<tr><td style="background:#003DA5;padding:12px 20px;"><p style="margin:0;font:700 13px/1 {font};letter-spacing:.02em;color:#ffffff;">CougConnect &bull; {esc(tier)}</p></td></tr>
<tr><td style="padding:28px 28px 8px;font:16px/1.6 {font};color:#0b1234;">
<h1 style="margin:0 0 12px;font:700 24px/1.3 {font};color:#0b1234;">Welcome to the CougConnect {esc(tier)} program!</h1>
<p style="margin:0 0 14px;">Hey <strong>{esc(first)}</strong>,</p>
<p style="margin:0 0 14px;">We&rsquo;re thrilled to have you on board. Your support helps us create more BYU athlete content and connect fans and businesses across Cougar Nation. &#128153;</p>
<p style="margin:0 0 14px;">To get your sponsorship set up, please take a minute to fill out our quick <strong>Sponsor Intake Form</strong> &mdash; logo, website, offer details and social handles.</p>
<table role="presentation" cellpadding="0" cellspacing="0" border="0" style="margin:16px 0 14px;"><tr><td bgcolor="#003DA5" style="border-radius:10px;">
<a href="{esc(INTAKE_FORM_URL)}" target="_blank" style="display:inline-block;padding:12px 20px;font:700 15px/1 {font};color:#ffffff;text-decoration:none;">Fill Out the Sponsor Intake Form</a>
</td></tr></table>
<p style="margin:0 0 6px;">Once it&rsquo;s in, we&rsquo;ll:</p>
<table role="presentation" cellpadding="0" cellspacing="0" border="0" width="100%" style="margin:4px 0 6px;">{rows}</table>
<p style="margin:12px 0 14px;">Questions or ideas for how you&rsquo;d like to be featured? Just reply to this email.</p>
<p style="margin:0 0 6px;">Go Cougs,<br><strong>The CougConnect Team</strong></p>
<p style="margin:0 0 20px;font-size:14px;color:#44506b;"><a href="mailto:admin@cougconnect.com" style="color:#003DA5;text-decoration:none;">admin@cougconnect.com</a> &middot; <a href="https://cougconnect.com" style="color:#003DA5;text-decoration:none;">cougconnect.com</a></p>
</td></tr></table></td></tr></table></body></html>"""
    return subject, text, body_html
