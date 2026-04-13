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
            log.info(f"MemberPress API /members status={resp.status} for email={email}")
            if resp.status != 200:
                body = await resp.text()
                log.error(f"MemberPress API error: {body}")
                return None
            data = await resp.json()
            log.info(f"MemberPress returned {len(data)} result(s): {[m.get('email') for m in data]}")
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


async def get_active_membership_ids(mp_member_id: int) -> list[int]:
    """Return list of membership IDs the member currently has active."""
    import logging
    log = logging.getLogger("cougconnect")
    async with aiohttp.ClientSession() as session:
        async with session.get(
            _api(f"members/{mp_member_id}/subscriptions"),
            headers={"MEMBERPRESS-API-KEY": MP_KEY},
        ) as resp:
            log.info(f"Subscriptions endpoint status={resp.status} for mp_member_id={mp_member_id}")
            if resp.status != 200:
                body = await resp.text()
                log.error(f"Subscriptions error: {body}")
                # Fall back to member object active_memberships
                return await _get_active_ids_from_member(mp_member_id)
            subs = await resp.json()
            log.info(f"Raw subscriptions: {subs}")
    active = []
    for sub in subs:
        status = sub.get("status")
        mid = sub.get("membership", {}).get("id") or sub.get("membership_id")
        log.info(f"  sub status={status} membership_id={mid}")
        if status in ("active", "complete") and mid:
            active.append(int(mid))
    log.info(f"Active membership IDs: {active} | Configured gold={_GOLD_IDS} silver={_SILVER_IDS} insider={_INSIDER_IDS}")
    return active


async def _get_active_ids_from_member(mp_member_id: int) -> list[int]:
    """Fallback: read active_memberships from the member object itself."""
    import logging
    log = logging.getLogger("cougconnect")
    member = await get_member_by_id(mp_member_id)
    if not member:
        return []
    active_memberships = member.get("active_memberships", [])
    log.info(f"Fallback active_memberships from member object: {active_memberships}")
    ids = []
    for m in active_memberships:
        mid = m.get("id") if isinstance(m, dict) else m
        if mid:
            ids.append(int(mid))
    return ids


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
