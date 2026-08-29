"""Mailchimp audience bridge.

The bot is the single writer for member tags in the CougConnect audience:
  * on signup webhooks  -> upsert member as `subscribed` with tier / size / Customer tags
  * on Discord verify   -> add the `discord-verified` tag (Mailchimp journeys branch on it)

Both behaviours ship OFF. Flags (Railway env):
  MAILCHIMP_API_KEY    e.g. xxxxxxxx-us21 (datacenter parsed from the suffix)
  MAILCHIMP_LIST_ID    audience id (default: CougConnect 0deabea9e3)
  MAILCHIMP_SYNC=1     enable signup upserts
  MAILCHIMP_TAGGING=1  enable discord-verified tagging
  ONBOARDING_DRY_RUN=1 log what would happen, call nothing

Members in a compliance state (unsubscribed / bounced) are never re-added:
Mailchimp rejects the PUT and we log + move on.
"""
import hashlib
import logging
import os

import aiohttp

log = logging.getLogger("cougconnect.mailchimp")

API_KEY = os.getenv("MAILCHIMP_API_KEY", "").strip()
LIST_ID = os.getenv("MAILCHIMP_LIST_ID", "0deabea9e3").strip()
_DC = API_KEY.rsplit("-", 1)[-1] if "-" in API_KEY else ""
BASE = f"https://{_DC}.api.mailchimp.com/3.0" if _DC else ""


def _flag(name: str) -> bool:
    return os.getenv(name, "").strip().lower() in ("1", "true", "yes", "on")


SYNC_ENABLED = _flag("MAILCHIMP_SYNC")
TAGGING_ENABLED = _flag("MAILCHIMP_TAGGING")
DRY_RUN = _flag("ONBOARDING_DRY_RUN")

VERIFIED_TAG = "discord-verified"
TIMEOUT = aiohttp.ClientTimeout(total=15)


def configured() -> bool:
    return bool(API_KEY and BASE and LIST_ID)


def subscriber_hash(email: str) -> str:
    return hashlib.md5(email.strip().lower().encode()).hexdigest()


async def _request(method: str, path: str, payload: dict | None = None) -> tuple[int, dict]:
    auth = aiohttp.BasicAuth("anystring", API_KEY)
    async with aiohttp.ClientSession(timeout=TIMEOUT, auth=auth) as session:
        async with session.request(method, f"{BASE}{path}", json=payload) as resp:
            try:
                body = await resp.json(content_type=None)
            except Exception:
                body = {}
            return resp.status, body if isinstance(body, dict) else {}


async def add_tags(email: str, tags: list[str]) -> bool:
    """Attach tags to an existing member. False if the member isn't in the audience."""
    if not configured():
        log.warning("Mailchimp not configured — cannot tag %s", email)
        return False
    if DRY_RUN:
        log.info("[dry-run] would tag %s with %s", email, tags)
        return True
    status, body = await _request(
        "POST", f"/lists/{LIST_ID}/members/{subscriber_hash(email)}/tags",
        {"tags": [{"name": t, "status": "active"} for t in tags]},
    )
    if status == 204:
        return True
    log.warning("Mailchimp tag %s -> %s failed: %s %s", email, tags, status, body.get("detail") or body.get("title"))
    return False


async def upsert_member(email: str, first_name: str, last_name: str, tags: list[str]) -> bool:
    """Add or update a member as `subscribed` (no double opt-in email) and apply tags."""
    if not configured():
        log.warning("Mailchimp not configured — cannot sync %s", email)
        return False
    if DRY_RUN:
        log.info("[dry-run] would upsert %s (%s %s) tags=%s", email, first_name, last_name, tags)
        return True
    payload = {
        "email_address": email,
        "status_if_new": "subscribed",
        "merge_fields": {k: v for k, v in (("FNAME", first_name), ("LNAME", last_name)) if v},
    }
    status, body = await _request("PUT", f"/lists/{LIST_ID}/members/{subscriber_hash(email)}", payload)
    if status >= 300:
        # 400 "Member In Compliance State" = unsubscribed/bounced: leave them alone, never re-add.
        log.warning("Mailchimp upsert %s failed: %s %s", email, status, body.get("detail") or body.get("title"))
        return False
    return await add_tags(email, tags)


async def tag_verified(email: str) -> bool:
    """Mark a member as Discord-verified so journeys stop nudging them."""
    if not TAGGING_ENABLED:
        return False
    return await add_tags(email, [VERIFIED_TAG])
