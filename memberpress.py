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
_INSIDER_IDS: set[int] = set()


def load_tier_ids():
    global _GOLD_IDS, _SILVER_IDS, _INSIDER_IDS
    def parse(env_key):
        return {int(x.strip()) for x in os.getenv(env_key, "").split(",") if x.strip().isdigit()}
    _GOLD_IDS = parse("MEMBERPRESS_TIER_GOLD_IDS")
    _SILVER_IDS = parse("MEMBERPRESS_TIER_SILVER_IDS")
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
    if ids & _INSIDER_IDS:
        return "insider"
    return "unsubscribed"


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
    member = await get_member_by_id(mp_member_id)
    if not member:
        log.warning(f"Fallback: get_member_by_id({mp_member_id}) returned None — trying email lookup")
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
