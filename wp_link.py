"""Report Discord links back to WordPress (cc-shirt-batches REST).

The account page shows "Verified as @name" from user meta the bot writes here,
and the tenure endpoint prefers these links over the old ExpressTech ones.
Everything is best-effort: a failed write-back never blocks a verification.

Env: CCSB_TENURE_URL (…/wp-json/ccsb/v1/tenure — the base is derived from it)
     CCSB_TENURE_KEY (X-CCSB-Key, same key the tenure endpoint uses)
"""
import logging
import os

import aiohttp

log = logging.getLogger("cougconnect.wp_link")

_TENURE_URL = os.getenv("CCSB_TENURE_URL", "").strip()
KEY = os.getenv("CCSB_TENURE_KEY", "").strip()
BASE = _TENURE_URL[: -len("/tenure")] if _TENURE_URL.endswith("/tenure") else _TENURE_URL.rstrip("/")
TIMEOUT = aiohttp.ClientTimeout(total=25)


def configured() -> bool:
    return bool(BASE and KEY)


async def _post(path: str, payload: dict) -> tuple[int, dict]:
    async with aiohttp.ClientSession(timeout=TIMEOUT) as session:
        async with session.post(f"{BASE}{path}", json=payload, headers={"X-CCSB-Key": KEY}) as resp:
            try:
                body = await resp.json(content_type=None)
            except Exception:
                body = {}
            return resp.status, body if isinstance(body, dict) else {}


async def push_link(email: str, discord_id: int | str, username: str | None = None) -> bool:
    """Record one link on the member's WordPress account."""
    if not configured():
        return False
    try:
        status, body = await _post("/discord-link", {"email": email, "discord_id": str(discord_id), "username": username or ""})
        if status == 200:
            return True
        log.info(f"WP link write-back for {email}: {status} {body.get('status') or body.get('message')}")
    except Exception as e:
        log.info(f"WP link write-back failed for {email}: {e}")
    return False


async def push_unlink(email: str) -> bool:
    if not configured():
        return False
    try:
        status, _ = await _post("/discord-unlink", {"email": email})
        return status == 200
    except Exception as e:
        log.info(f"WP unlink write-back failed for {email}: {e}")
        return False


async def push_links(links: list[dict]) -> dict:
    """Bulk backfill — chunks of 200, returns merged counts."""
    totals: dict[str, int] = {"ok": 0, "no-user": 0, "invalid": 0, "failed": 0}
    if not configured():
        totals["failed"] = len(links)
        return totals
    for i in range(0, len(links), 200):
        chunk = links[i:i + 200]
        try:
            status, body = await _post("/discord-links", {"links": chunk})
            if status == 200:
                for k, v in (body.get("counts") or {}).items():
                    totals[k] = totals.get(k, 0) + int(v)
            else:
                totals["failed"] += len(chunk)
                log.warning(f"WP bulk link write-back chunk failed: {status} {body}")
        except Exception as e:
            totals["failed"] += len(chunk)
            log.warning(f"WP bulk link write-back chunk failed: {e}")
    return totals


async def silver_access(discord_id: int | str, check: bool = False) -> tuple[int, dict]:
    """Look up or spend a member's Silver channel month.

    WordPress stays the source of truth for what they hold and how long a
    window runs; Discord is only where the button happens to be. Returns the
    raw (status, body) so the caller can tell "nothing to spend" (409) apart
    from "not linked" (404) and from the endpoint being unreachable (0).
    """
    if not configured():
        return 0, {}

    payload = {"discord_id": str(discord_id)}
    if check:
        payload["check"] = 1

    try:
        return await _post("/silver-access", payload)
    except Exception as e:
        log.error(f"silver-access call failed for {discord_id}: {e}")
        return 0, {}
