"""
Game-day channels: one channel per BYU football / men's basketball game.

Opens GAMEDAY_OPEN_HOURS_BEFORE kickoff, closes GAMEDAY_CLOSE_HOURS_AFTER the
game goes final, when its transcript is archived to the admin log and the
channel is deleted. bot.py does the Discord side; this file only knows the
schedule and the clock, so it can be tested without a bot.

The schedule comes straight from ESPN's public team-schedule API (BYU = team
252), polled every few minutes. The previous version read a hand-filled
schedule.json, which went stale; ESPN also tells us when a game actually ends,
which a static file can't.
"""

import datetime
import logging
import os
import re
from zoneinfo import ZoneInfo

import aiohttp

log = logging.getLogger("cougconnect.gameday")

MOUNTAIN = ZoneInfo("America/Denver")
EASTERN = ZoneInfo("America/New_York")
UTC = datetime.timezone.utc


def _flag(name: str) -> bool:
    return os.getenv(name, "").strip().lower() in ("1", "true", "yes", "on")


# Ships OFF. DRY_RUN reports what it would open/close to the admin log and touches nothing.
ENABLED = _flag("GAMEDAY_CHANNELS")
DRY_RUN = _flag("GAMEDAY_DRY_RUN")
OPEN_HOURS_BEFORE = float(os.getenv("GAMEDAY_OPEN_HOURS_BEFORE", "24"))
CLOSE_HOURS_AFTER = float(os.getenv("GAMEDAY_CLOSE_HOURS_AFTER", "24"))
SPORTS = [s.strip() for s in os.getenv("GAMEDAY_SPORTS", "football,basketball").split(",") if s.strip()]

TEAM_ID = "252"  # BYU on ESPN
_ESPN_PATH = {
    "football": "football/college-football",
    "basketball": "basketball/mens-college-basketball",
}
_SCHEDULE_URL = "https://site.api.espn.com/apis/site/v2/sports/{path}/teams/" + TEAM_ID + "/schedule"

# Used only when ESPN never reports the game final (outage, or the bot was down):
# assume it ended this long after kickoff, then start the CLOSE_HOURS_AFTER clock.
ASSUMED_LENGTH_H = {"football": 4.0, "basketball": 2.5}
# A TBA kickoff is treated as this local MT hour on the game date for open timing.
TBA_ASSUMED_HOUR_MT = 12

EMOJI = {"football": "🏈", "basketball": "🏀"}


def _parse_ts(s: str) -> datetime.datetime:
    # ESPN uses "2026-11-07T17:00Z"
    return datetime.datetime.fromisoformat(s.replace("Z", "+00:00")).astimezone(UTC)


def _parse_event(ev: dict, sport: str) -> dict | None:
    try:
        comp = ev["competitions"][0]
        byu = next(c for c in comp["competitors"] if str(c.get("id")) == TEAM_ID)
        opp = next(c for c in comp["competitors"] if str(c.get("id")) != TEAM_ID)
    except (KeyError, IndexError, StopIteration):
        return None
    status = (comp.get("status") or ev.get("status") or {}).get("type", {})
    kickoff = _parse_ts(ev["date"])
    time_tba = comp.get("timeValid") is False
    if time_tba:
        # ESPN parks TBA games at midnight ET of the game date (05:00Z), which is
        # the previous evening in Mountain time. Re-anchor on the ET date.
        game_date = kickoff.astimezone(EASTERN).date()
        kickoff = datetime.datetime.combine(
            game_date, datetime.time(TBA_ASSUMED_HOUR_MT), tzinfo=MOUNTAIN).astimezone(UTC)
    team = opp.get("team", {})
    tv = ", ".join(
        b.get("media", {}).get("shortName", "") for b in comp.get("broadcasts", [])
        if b.get("media", {}).get("shortName")
    )
    return {
        "event_id": str(ev["id"]),
        "sport": sport,
        "kickoff": kickoff,
        "time_tba": time_tba,
        "home": byu.get("homeAway") == "home",
        "neutral": bool(comp.get("neutralSite")),
        "opponent": team.get("shortDisplayName") or team.get("location") or team.get("displayName") or "TBD",
        "opponent_full": team.get("displayName") or team.get("location") or "TBD",
        "tv": tv,
        "state": status.get("state", ""),        # pre / in / post
        "status_name": status.get("name", ""),   # STATUS_FINAL, STATUS_POSTPONED, STATUS_CANCELED…
    }


async def fetch_schedule(session: aiohttp.ClientSession) -> list[dict]:
    """Regular season + postseason for every enabled sport. Raises if NOTHING could be fetched."""
    games: dict[str, dict] = {}
    errors = []
    for sport in SPORTS:
        path = _ESPN_PATH.get(sport)
        if not path:
            continue
        for seasontype in (2, 3):  # 2 = regular season, 3 = bowls / tournaments
            url = _SCHEDULE_URL.format(path=path)
            try:
                async with session.get(url, params={"seasontype": seasontype}) as resp:
                    if resp.status != 200:
                        errors.append(f"{sport}/{seasontype}: HTTP {resp.status}")
                        continue
                    data = await resp.json(content_type=None)
            except Exception as e:
                errors.append(f"{sport}/{seasontype}: {e}")
                continue
            for ev in data.get("events", []):
                g = _parse_event(ev, sport)
                if g:
                    games[g["event_id"]] = g
    if not games and errors:
        raise RuntimeError("; ".join(errors))
    return sorted(games.values(), key=lambda g: g["kickoff"])


def is_called_off(game: dict) -> bool:
    name = game.get("status_name", "")
    return any(w in name for w in ("CANCELED", "CANCELLED", "POSTPONED", "FORFEIT"))


def open_at(game: dict) -> datetime.datetime:
    return game["kickoff"] - datetime.timedelta(hours=OPEN_HOURS_BEFORE)


def should_open(game: dict, now: datetime.datetime) -> bool:
    """Inside the open window and not already over. A game that's already final
    never gets a channel, so a late deploy doesn't open a channel for last week."""
    if game["state"] == "post" or is_called_off(game):
        return False
    return open_at(game) <= now


def fallback_end(sport: str, kickoff: datetime.datetime) -> datetime.datetime:
    return kickoff + datetime.timedelta(hours=ASSUMED_LENGTH_H.get(sport, 4.0))


def close_at_from_end(ended: datetime.datetime) -> datetime.datetime:
    return ended + datetime.timedelta(hours=CLOSE_HOURS_AFTER)


def channel_name(game: dict) -> str:
    """Discord lowercases names and turns spaces into dashes, so build the final form here.
    Basketball plays some teams twice a season, so its names carry the date."""
    vs_at = "vs" if game["home"] or game["neutral"] else "at"
    opponent = re.sub(r"[^a-z0-9]+", "-", game["opponent"].lower()).strip("-") or "tbd"
    name = f"{EMOJI.get(game['sport'], '🏟️')}-byu-{vs_at}-{opponent}"
    if game["sport"] == "basketball":
        name += "-" + game["kickoff"].astimezone(MOUNTAIN).strftime("%b-%-d").lower()
    return name[:100]


def matchup(game: dict) -> str:
    vs_at = "vs" if game["home"] or game["neutral"] else "at"
    return f"BYU {vs_at} {game['opponent_full']}"


def kickoff_text(game: dict) -> str:
    local = game["kickoff"].astimezone(MOUNTAIN)
    day = local.strftime("%A, %B %-d")
    if game["time_tba"]:
        return f"{day} · time TBA"
    return f"{day} · {local.strftime('%-I:%M %p').lower()} MT"


def hours_text(h: float) -> str:
    return f"{h:g} hour{'s' if h != 1 else ''}"
