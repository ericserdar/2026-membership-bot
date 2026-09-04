"""
MemberPress REST API client.

Docs: https://memberpress.com/addons/developer-tools/
Base URL: {MEMBERPRESS_BASE_URL}/wp-json/mp/v1/
Auth: Basic  (username: any WP user, password: MemberPress API key)
"""

import os
import aiohttp
from datetime import datetime

MP_BASE = os.getenv("MEMBERPRESS_BASE_URL", "").rstrip("/")
MP_KEY = os.getenv("MEMBERPRESS_API_KEY", "")

# MemberPress custom-field slug that stores the BYU apartment dropdown value.
APARTMENT_FIELD = os.getenv("MEMBERPRESS_APARTMENT_FIELD", "mepr_mepr_byu_apartment")

# Tier ID sets — populated from env at startup
_GOLD_IDS: set[int] = set()
_SILVER_IDS: set[int] = set()
_ROYAL_IDS: set[int] = set()
_INSIDER_IDS: set[int] = set()


def load_tier_ids():
    global _GOLD_IDS, _SILVER_IDS, _ROYAL_IDS, _INSIDER_IDS
    def parse(env_key):
        return {int(x.strip()) for x in os.getenv(env_key, "").split(",") if x.strip().isdigit()}
    _GOLD_IDS = parse("MEMBERPRESS_TIER_GOLD_IDS")
    _SILVER_IDS = parse("MEMBERPRESS_TIER_SILVER_IDS")
    _ROYAL_IDS = parse("MEMBERPRESS_TIER_ROYAL_IDS")
    _INSIDER_IDS = parse("MEMBERPRESS_TIER_INSIDER_IDS")


def _api(path: str) -> str:
    return f"{MP_BASE}/wp-json/mp/v1/{path.lstrip('/')}"


def resolve_tier(membership_ids: list[int]) -> str:
    """Given a list of active MemberPress membership IDs, return the highest tier."""
    ids = set(membership_ids)
    if ids & _GOLD_IDS:
        return "gold"
    if ids & _SILVER_IDS:
        return "silver"
    # Royal ($20/mo) carries no perks above Insider, but it is its own identity
    # and its own Discord role, so it resolves ahead of insider.
    if ids & _ROYAL_IDS:
        return "royal"
    if ids & _INSIDER_IDS:
        return "insider"
    return "unsubscribed"


async def resolve_tier_or_none(mp_member_id: int, mp_email: str = "") -> tuple[str | None, dict | None]:
    """Resolve a member's current tier, distinguishing "unsubscribed" from "API failed".

    Returns (tier, member):
      - (tier, member_obj) when the member was fetched successfully. A genuinely
        unsubscribed member returns ("unsubscribed", member_obj) — the object still
        exists in WordPress with an empty active_memberships list.
      - (None, None) when the member could NOT be fetched at all (transient
        rest_no_route 404 or other API failure). Callers MUST treat None as
        "don't know — leave the current role alone", never as unsubscribed.

    This is the guard against the false-downgrade bug: a flaky /members/{id}
    endpoint used to zero out active_memberships and silently demote paying members.
    """
    member, active_ids = await get_member_and_active_ids(mp_member_id, mp_email)
    if member is None:
        return None, None
    return resolve_tier(active_ids), member


async def get_all_members_paged(per_page: int = 100, max_pages: int = 40) -> tuple[list[dict], int]:
    """Fetch all MemberPress members across pages.

    Returns (members, failed_pages). A page that errors/non-200 is counted in
    failed_pages and skipped rather than aborting the whole scan (the API is
    intermittently flaky). Stops on the first short/empty successful page.
    """
    import logging
    log = logging.getLogger("cougconnect")
    members: list[dict] = []
    failed = 0
    async with aiohttp.ClientSession() as session:
        for page in range(1, max_pages + 1):
            try:
                async with session.get(
                    _api("members"),
                    headers={"MEMBERPRESS-API-KEY": MP_KEY},
                    params={"per_page": per_page, "page": page},
                ) as resp:
                    if resp.status != 200:
                        failed += 1
                        log.warning(f"get_all_members_paged: page {page} status {resp.status}")
                        continue
                    batch = await resp.json()
            except Exception as e:
                failed += 1
                log.warning(f"get_all_members_paged: page {page} error {e}")
                continue
            if not batch:
                break
            members.extend(batch)
            if len(batch) < per_page:
                break
    return members, failed


async def get_member_by_email(email: str) -> dict | None:
    """Return the first MemberPress member matching the email, or None."""
    import logging
    log = logging.getLogger("cougconnect")
    async with aiohttp.ClientSession() as session:
        async with session.get(
            _api("members"),
            headers={"MEMBERPRESS-API-KEY": MP_KEY},
            params={"search": email, "per_page": 5},
        ) as resp:
            if resp.status != 200:
                body = await resp.text()
                log.error(f"MemberPress API error (status={resp.status}): {body}")
                return None
            data = await resp.json()
            log.debug(f"MemberPress /members search for {email}: {len(data)} result(s)")
    for member in data:
        if member.get("email", "").lower() == email.lower():
            return member
    return None


async def get_member_by_id(mp_member_id: int) -> dict | None:
    async with aiohttp.ClientSession() as session:
        async with session.get(
            _api(f"members/{mp_member_id}"),
            headers={"MEMBERPRESS-API-KEY": MP_KEY},
        ) as resp:
            if resp.status != 200:
                return None
            return await resp.json()


async def get_active_membership_ids(mp_member_id: int, mp_email: str = "") -> list[int]:
    """Return list of membership IDs the member currently has active.

    The /members/{id}/subscriptions endpoint returns 404 for this MemberPress setup,
    so we go straight to the member object's active_memberships field.
    """
    import logging
    log = logging.getLogger("cougconnect")
    _, ids = await get_member_and_active_ids(mp_member_id, mp_email)
    log.debug(f"Active membership IDs for mp_member_id={mp_member_id}: {ids}")
    return ids


def get_apartment_slug(member: dict) -> str | None:
    """Extract the BYU apartment dropdown slug from a member's custom fields.

    MemberPress returns dropdown values as slugs under the `profile` object.
    Returns None when the field is empty/absent.
    """
    profile = member.get("profile") or {}
    if not isinstance(profile, dict):
        return None
    val = profile.get(APARTMENT_FIELD)
    if not val or not isinstance(val, str):
        return None
    val = val.strip().lower()
    return val or None


def active_ids_from_member_object(member: dict) -> list[int]:
    """Extract active membership IDs from an already-fetched member object."""
    import logging
    log = logging.getLogger("cougconnect")
    active_memberships = member.get("active_memberships", [])
    ids = []
    for m in active_memberships:
        mid = m.get("id") if isinstance(m, dict) else m
        if mid:
            ids.append(int(mid))
    log.debug(f"active membership IDs from member object: {ids}")
    return ids


async def get_member_and_active_ids(mp_member_id: int, mp_email: str = "") -> tuple[dict | None, list[int]]:
    """Fetch the member object and its active membership IDs in one round-trip.

    Returns (member, active_ids). member is None if the lookup fails entirely.
    Callers that also need custom fields (e.g. apartment) should use this
    instead of get_active_membership_ids() to avoid a second API call.
    """
    import logging
    log = logging.getLogger("cougconnect")
    # NEVER call /members/{id} with a falsy ID. This install does not 404 on
    # /members/0 — it returns an arbitrary member object. Passing 0 through would
    # silently resolve every caller to that same stranger's tier. Callers that
    # only know an email (e.g. members adopted from the WordPress-side Discord
    # link) pass mp_member_id=0 on purpose and must go straight to the search.
    member = await get_member_by_id(mp_member_id) if mp_member_id and mp_member_id > 0 else None
    if not member:
        if mp_member_id and mp_member_id > 0:
            log.warning(f"Fallback: get_member_by_id({mp_member_id}) returned None — trying email lookup")
        else:
            log.debug(f"No MemberPress ID for {mp_email} — resolving by email")
        if mp_email:
            member = await get_member_by_email(mp_email)
        if not member:
            log.warning(f"Fallback: email lookup also failed for mp_member_id={mp_member_id} email={mp_email}")
            return None, []
    return member, active_ids_from_member_object(member)


def parse_subscription_status(member_data: dict) -> dict:
    """
    Extract a human-readable subscription status from a MemberPress member object.
    Returns: { status, expires_at (str or None), raw }
    """
    # MemberPress member object has an `active_memberships` list
    active = member_data.get("active_memberships", [])
    if not active:
        return {"status": "No active subscription", "expires_at": None}

    # Use the first active membership's subscription details
    first = active[0]
    sub = first.get("recent_subscriptions", [{}])[0] if first.get("recent_subscriptions") else {}

    raw_status = sub.get("status", "active")
    expires = sub.get("expires_at") or sub.get("expire_at")

    if raw_status == "active":
        if expires and expires != "0000-00-00 00:00:00":
            try:
                exp_dt = datetime.strptime(expires, "%Y-%m-%d %H:%M:%S")
                return {"status": "Active", "expires_at": exp_dt.strftime("%m/%d/%Y")}
            except ValueError:
                pass
        return {"status": "Active", "expires_at": None}
    elif raw_status in ("cancelled", "canceled"):
        if expires:
            try:
                exp_dt = datetime.strptime(expires, "%Y-%m-%d %H:%M:%S")
                return {"status": f"Ending on {exp_dt.strftime('%m/%d/%Y')}", "expires_at": exp_dt.strftime("%m/%d/%Y")}
            except ValueError:
                pass
        return {"status": "Cancelled", "expires_at": None}
    elif raw_status == "paused":
        return {"status": "Paused", "expires_at": None}
    elif raw_status == "expired":
        return {"status": "Expired", "expires_at": None}
    else:
        return {"status": raw_status.title(), "expires_at": None}


# ---------------------------------------------------------------------------
# Membership tenure (from WordPress, not MemberPress)
# ---------------------------------------------------------------------------
#
# MemberPress's REST API exposes members, memberships and subscriptions — but
# NOT transactions, and /members/{id}/subscriptions 404s on this install. Only
# the transaction table records paid periods, so gaps, resubscribes and tier
# switches are invisible from here.
#
# The cc-shirt-batches plugin already computes this correctly on the WordPress
# side, so we ask it rather than reimplementing the arithmetic (and its edge
# cases) in Python.

TENURE_URL = os.getenv("CCSB_TENURE_URL", "")
TENURE_KEY = os.getenv("CCSB_TENURE_KEY", "")


async def get_tenure_map() -> dict | None:
    """Fetch paid-membership tenure for everyone linked to Discord.

    Returns {email_lower: {"days": int, "years": int, "active": bool, "tier": str}}.

    Returns None — never {} — when the call fails, matching the convention in
    resolve_tier_or_none(): None means "we don't know, change nothing". A
    milestone run that treated a failed fetch as "nobody qualifies" would strip
    roles from the whole server.
    """
    import logging
    log = logging.getLogger("cougconnect")

    if not TENURE_URL or not TENURE_KEY:
        log.warning("Tenure endpoint not configured (CCSB_TENURE_URL / CCSB_TENURE_KEY)")
        return None

    headers = {"X-CCSB-Key": TENURE_KEY}

    try:
        timeout = aiohttp.ClientTimeout(total=60)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.get(TENURE_URL, headers=headers) as resp:
                if resp.status != 200:
                    log.error(f"Tenure fetch failed: HTTP {resp.status}")
                    return None
                payload = await resp.json()
    except Exception as e:
        log.error(f"Tenure fetch failed: {e}")
        return None

    members = payload.get("members") if isinstance(payload, dict) else None
    if not isinstance(members, list):
        log.error("Tenure response missing 'members' list")
        return None

    out: dict[str, dict] = {}
    for m in members:
        email = str(m.get("email", "")).strip().lower()
        if not email:
            continue
        out[email] = {
            "days": int(m.get("days") or 0),
            "years": int(m.get("years") or 0),
            "active": bool(m.get("active")),
            "tier": str(m.get("tier") or ""),
            "discord_id": str(m.get("discord_id") or ""),
        }

    log.info(f"Tenure map loaded: {len(out)} members")
    return out
