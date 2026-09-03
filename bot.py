"""
CougConnect Discord Membership Bot
-----------------------------------
- Verifies Discord users against MemberPress subscriptions
- Assigns roles: Gold, Silver, Insider, Unsubscribed
- Exposes an aiohttp web server for:
    GET  /verify-page  — serves the verification form to members
    POST /verify-page  — processes email submission, assigns role
    POST /webhook      — MemberPress subscription status webhooks
"""

import asyncio
import base64
from collections import Counter
import hashlib
import hmac
import html
import io
import json
import logging
import os
import re
import secrets
import sqlite3
import sys
import datetime
from datetime import datetime as dt, timezone
from urllib.parse import urlencode
from zoneinfo import ZoneInfo

import aiohttp
import aiohttp.web as web
import discord
from discord import app_commands
from discord.ext import commands, tasks
from dotenv import load_dotenv

import aggregator
import database as db
import mailchimp
import memberpress as mp
import wp_link

load_dotenv()

# stdout so Railway doesn't tag every INFO line as an error (stderr = error there)
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s", stream=sys.stdout)
log = logging.getLogger("cougconnect")

# ── Config ─────────────────────────────────────────────────────────────────────

TOKEN = os.getenv("DISCORD_BOT_TOKEN")
GUILD_ID = int(os.getenv("DISCORD_GUILD_ID", "0"))
VERIFY_CHANNEL_ID = int(os.getenv("DISCORD_VERIFY_CHANNEL_ID", "0"))
UNSUBSCRIBED_CHANNEL_ID = int(os.getenv("DISCORD_UNSUBSCRIBED_CHANNEL_ID", "1360788474374000700"))
ADMIN_LOG_CHANNEL_ID = int(os.getenv("DISCORD_ADMIN_LOG_CHANNEL_ID", "1493610618094227577"))
PORT = int(os.getenv("PORT", "8080"))
BOT_PUBLIC_URL = os.getenv("BOT_PUBLIC_URL", "http://localhost:8080")
MP_WEBHOOK_SECRET = os.getenv("MEMBERPRESS_WEBHOOK_SECRET", "")
BOT_VERIFY_SECRET = os.getenv("BOT_VERIFY_SECRET", "")
# Account-page "Connect Discord": the site mints a token signed with
# BOT_VERIFY_SECRET (Shirt Batches → Settings → Connect secret), /connect
# validates it and runs Discord OAuth (identify + guilds.join). The client
# secret comes from the Developer Portal for the bot application.
DISCORD_CLIENT_ID = os.getenv("DISCORD_CLIENT_ID", "1489119085961809981")
DISCORD_CLIENT_SECRET = os.getenv("DISCORD_CLIENT_SECRET", "")
CONNECT_REDIRECT_PATH = "/connect/callback"
# Random path segment authenticating webhooks (MemberPress can't sign reliably).
# When set, webhooks POST to /webhook/<token>; bare /webhook stays active for the
# transition unless DISABLE_LEGACY_WEBHOOK=1.
WEBHOOK_URL_TOKEN = os.getenv("WEBHOOK_URL_TOKEN", "")
DISABLE_LEGACY_WEBHOOK = os.getenv("DISABLE_LEGACY_WEBHOOK", "") == "1"

# Admin report emails (nightly DB backup + unverified-subscriber report).
# From address must be a verified SendGrid sender.
SENDGRID_API_KEY = os.getenv("SENDGRID_API_KEY", "")
REPORT_EMAIL_FROM = os.getenv("REPORT_EMAIL_FROM", "")
REPORT_EMAIL_TO = os.getenv("REPORT_EMAIL_TO", "eric@serdarconsulting.com")

ROLE_IDS = {
    "gold":         int(os.getenv("DISCORD_ROLE_GOLD_ID", "0")),
    "silver":       int(os.getenv("DISCORD_ROLE_SILVER_ID", "0")),
    "insider":      int(os.getenv("DISCORD_ROLE_INSIDER_ID", "0")),
    "unsubscribed": int(os.getenv("DISCORD_ROLE_UNSUBSCRIBED_ID", "0")),
}

GENERAL_CHANNEL_ID = int(os.getenv("DISCORD_GENERAL_CHANNEL_ID", "1050165331894751314"))

# #byu-news content aggregator. Unset (0) leaves the task idling with a single
# admin-log heads-up, so the code can deploy before the channel exists.
NEWS_CHANNEL_ID = int(os.getenv("DISCORD_NEWS_CHANNEL_ID", "0"))

# Membership milestones post here. Falls back to general so an unset value can
# never silence the feature outright.
MILESTONE_CHANNEL_ID = int(os.getenv("DISCORD_MILESTONE_CHANNEL_ID", "0")) or GENERAL_CHANNEL_ID

# Feature switches read from Railway env vars, so a behaviour flips without a
# redeploy. Defined here because the game-week config below is its first caller.
def _flag(name: str) -> bool:
    return os.getenv(name, "").strip().lower() in ("1", "true", "yes", "on")


# ── Game-week channels ────────────────────────────────────────────────────────
# One channel per BYU game: opened Monday morning of game week, deleted Sunday
# 23:59 MT after its transcript is archived to the admin log. Replaces the old
# game-day thread in #general, which lasted a single day and was public.
#
# All times are America/Denver, not a fixed UTC offset — a fixed offset would
# fire an hour early for the November games, after DST ends.
MOUNTAIN = ZoneInfo("America/Denver")
GAMEDAY_OPEN_TIME = datetime.time(hour=8, minute=30, tzinfo=MOUNTAIN)   # Monday of game week
GAMEDAY_CLOSE_TIME = datetime.time(hour=23, minute=59, tzinfo=MOUNTAIN)  # Sunday that week

# Ships OFF. GAMEDAY_DRY_RUN reports both runs to the admin log and touches
# nothing — the close half deletes a channel outright, so it gets a rehearsal.
GAMEDAY_CHANNELS = _flag("GAMEDAY_CHANNELS")
GAMEDAY_DRY_RUN = _flag("GAMEDAY_DRY_RUN")

# Football games land in Football Info, basketball in Basketball.
GAMEDAY_CATEGORY_IDS = {
    "football": int(os.getenv("DISCORD_CATEGORY_FOOTBALL_ID", "1395637847041511506")),
    "basketball": int(os.getenv("DISCORD_CATEGORY_BASKETBALL_ID", "1395638029737132103")),
}

# Who can see a game-week channel. @everyone is denied; these are granted.
# Names are documentation — Discord enforces the IDs.
GAMEDAY_ACCESS_ROLE_IDS = [int(x) for x in os.getenv(
    "DISCORD_GAMEDAY_ROLE_IDS",
    "1082893434827845643,"   # Gold Subscriber
    "1082893184402722876,"   # Silver Subscriber
    "1359380692202553434,"   # Cougar Insider
    "1454954937610932325,"   # Verified Insider
    "1359536444451983400",   # Legacy Cougar Insider
).split(",") if x.strip()]

# Mods/admins react with this emoji to flag a message: it's logged to the mod
# log channel + DB, then deleted. Only members with Manage Messages can trigger it.
FLAG_EMOJI = os.getenv("FLAG_EMOJI", "🚩")
MOD_LOG_CHANNEL_ID = int(os.getenv("DISCORD_MOD_LOG_CHANNEL_ID", "1189800941793333389"))  # #admin-moderators

FAQ_PATH = os.path.join(os.path.dirname(__file__), "faq.json")
SCHEDULE_PATH = os.path.join(os.path.dirname(__file__), "schedule.json")
SPONSORS_PATH = os.path.join(os.path.dirname(__file__), "sponsors.json")
APARTMENTS_PATH = os.path.join(os.path.dirname(__file__), "apartments.json")

# Maps MemberPress apartment dropdown slug → {label, role_name, role_id}.
# Grants a complex-specific Discord role on top of the tier role. Loaded at import.
def _load_apartments() -> dict:
    try:
        with open(APARTMENTS_PATH) as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return {}

# Big signup days produce big anniversary days exactly a year later, and those
# members have all genuinely earned the shoutout — so a large batch is never a
# reason to hold the run back. This only decides when the admin log gets a
# heads-up, so an unexpected flood is still visible rather than silent.
MILESTONE_BATCH_NOTICE = int(os.getenv("MILESTONE_BATCH_NOTICE", "25"))

# Let the nightly tier sync act on members linked only in WordPress (the
# ExpressTech plugin), not just those in member_links. Off by default: their
# first sync can move many roles at once, so preview with /tier-sync-preview
# before turning it on.
TIER_SYNC_INCLUDE_WP_LINKED = os.getenv("TIER_SYNC_INCLUDE_WP_LINKED", "").strip().lower() in ("1", "true", "yes", "on")

UPGRADE_NUDGE_DAYS = 152  # ~5 months as a member before the Insider upgrade nudge
UPGRADE_NUDGE_DAILY_CAP = 50  # spread large cohorts over multiple days
WINBACK_DAYS = 30

# ── Onboarding (new-member journey) ───────────────────────────────────────────
# Copy targets (URLs, channel IDs, per-tier channel lists, ping roles) live in
# onboarding.json so wording and links change without a code edit. Every
# behaviour below ships OFF and is flipped per flag on Railway — no redeploy.
ONBOARDING_PATH = os.path.join(os.path.dirname(__file__), "onboarding.json")



ONBOARDING_JOIN_DM = _flag("ONBOARDING_JOIN_DM")   # DM the 3 steps when someone joins the guild
ONBOARDING_DRIP = _flag("ONBOARDING_DRIP")         # day-1/3 unverified nudges + day-7/30 check-ins
ONBOARDING_DRY_RUN = _flag("ONBOARDING_DRY_RUN")   # log/admin-log "would DM …" and send nothing
ONBOARDING_DM_DAILY_CAP = int(os.getenv("ONBOARDING_DM_DAILY_CAP", "50"))
UNVERIFIED_NUDGE_HOURS = (24, 72)   # hours after joining without verifying → nudge 1, nudge 2
CHECKIN_DAYS = (7, 30)              # days after verifying → check-in DMs


def _load_onboarding() -> dict:
    try:
        with open(ONBOARDING_PATH) as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return {}


ONBOARDING = _load_onboarding()


def ob_url(key: str) -> str:
    return ONBOARDING.get("urls", {}).get(key, "https://cougconnect.com")


def ob_channel_id(key: str) -> int:
    return int(ONBOARDING.get("channels", {}).get(key, 0) or 0)


def ob_channel(key: str) -> str:
    """Clickable channel mention, falling back to a plain #name if unconfigured."""
    cid = ob_channel_id(key)
    return f"<#{cid}>" if cid else f"#{key}"


def ob_channel_link(key: str) -> str:
    cid = ob_channel_id(key)
    return f"https://discord.com/channels/{GUILD_ID}/{cid}" if cid else ob_url("invite")


def ob_chan_md(key: str) -> str:
    """Channel as a masked link — for DMs and embeds.

    In a DM, a <#id> mention renders as a bulky "Server › #channel" chip, so a
    list of them turns into a wall of badges. A masked link reads like a plain
    channel name and still opens the channel. Falls back to the bare key when
    the channel isn't cached yet (cold start).
    """
    cid = ob_channel_id(key)
    if not cid:
        return f"#{key}"
    ch = bot.get_channel(cid)
    name = ch.name if ch else key.replace("_", "-")
    return f"[#{name}]({ob_channel_link(key)})"


def ping_roles_link() -> str:
    """Discord's Channels & Roles tab, where members pick their ping roles."""
    return f"https://discord.com/channels/{GUILD_ID}/customize-community"


def _load_json(path: str) -> list[dict]:
    try:
        with open(path) as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return []


def load_faq() -> list[dict]:
    return _load_json(FAQ_PATH)


APARTMENTS = _load_apartments()

MILESTONES_PATH = os.path.join(os.path.dirname(__file__), "milestones.json")

# [{years, role_name, role_id}] — the tenure roles, highest earned one held.
MILESTONES = sorted(_load_json(MILESTONES_PATH), key=lambda m: int(m.get("years", 0)))


# ── Bot class ──────────────────────────────────────────────────────────────────

class CougConnectBot(commands.Bot):
    def __init__(self):
        intents = discord.Intents.default()
        intents.members = True
        # Needed to read the content of flagged messages before deleting them.
        # Must also be enabled under Privileged Gateway Intents in the dev portal.
        intents.message_content = True
        # Slash-commands only; when_mentioned avoids needing a command prefix
        super().__init__(command_prefix=commands.when_mentioned, intents=intents)
        self._web_runner: web.AppRunner | None = None

    async def setup_hook(self):
        db.init_db()
        mp.load_tier_ids()
        self.add_view(VerifyView())
        self.add_view(ReSyncView())
        self.add_view(FlagReasonView())
        await self.tree.sync()
        log.info("Slash commands synced.")
        self.cleanup_tokens_task.start()
        self.sync_all_members_task.start()
        self.daily_report_task.start()
        self.backup_db_task.start()
        self.expiry_notice_task.start()
        self.winback_task.start()
        self.milestone_task.start()
        self.upgrade_nudge_task.start()
        self.sponsor_spotlight_task.start()
        self.weekly_digest_task.start()
        self.gameday_open_task.start()
        self.gameday_close_task.start()
        self.news_task.start()
        self.onboarding_drip_task.start()

    async def on_ready(self):
        log.info(f"Logged in as {self.user} (ID: {self.user.id})")
        await self._post_verify_embed()
        await self._post_unsubscribed_embed()
        await post_admin_log("✅ CougConnect bot online and ready.")

    async def _upsert_button_embed(self, channel_id: int, embed: discord.Embed, view: discord.ui.View, what: str):
        """Keep one button embed per channel across restarts — quietly.

        Editing the existing message in place produces no new message, so no
        unread badge and no push notification for anyone; a fresh send (first
        run, or the old one was deleted) goes out silent (SUPPRESS_NOTIFICATIONS).
        """
        channel = self.get_channel(channel_id)
        if not channel:
            return
        existing = None
        try:
            async for msg in channel.history(limit=20):
                if msg.author == self.user and msg.components:
                    existing = msg
                    break
        except discord.Forbidden:
            log.warning(f"No Read Message History permission in {what} channel {channel_id} — sending fresh")
        if existing:
            try:
                await existing.edit(embed=embed, view=view)
                return
            except discord.HTTPException as e:
                log.warning(f"Could not edit the {what} embed ({e}); sending a fresh one silently")
        await channel.send(embed=embed, view=view, silent=True)

    async def _post_verify_embed(self):
        embed = discord.Embed(
            title="🔐 Verify Your CougConnect Membership",
            description=(
                "Click the button below to link your CougConnect subscription to Discord.\n\n"
                "Use the **email you subscribed with** — your role is assigned automatically "
                "based on your membership tier, and you'll get a DM with a channel guide once you're in."
            ),
            color=discord.Color.blue(),
        )
        embed.set_footer(text="CougConnect — Insider BYU Athletics Coverage")
        await self._upsert_button_embed(VERIFY_CHANNEL_ID, embed, VerifyView(), "verify")

    async def _post_unsubscribed_embed(self):
        embed = discord.Embed(
            title="🔄 Reactivate Your CougConnect Membership",
            description=(
                "If you've renewed or upgraded your membership, click **Re-sync My Role** "
                "and your Discord role will be updated automatically.\n\n"
                "Not yet a member? Click **Upgrade Membership** to subscribe."
            ),
            color=discord.Color.blue(),
        )
        embed.set_footer(text="CougConnect — Insider BYU Athletics Coverage")
        await self._upsert_button_embed(UNSUBSCRIBED_CHANNEL_ID, embed, ReSyncView(), "unsubscribed")

    @tasks.loop(hours=1)
    async def cleanup_tokens_task(self):
        db.cleanup_expired_tokens()

    @tasks.loop(time=datetime.time(hour=10, minute=0, tzinfo=datetime.timezone.utc))  # 3am MST (UTC-7)
    async def sync_all_members_task(self):
        members, adopted = await _tier_sync_records()
        if not members:
            return
        if adopted:
            log.info(f"Tier sync: {adopted} member(s) linked in WordPress only, not in member_links — including them")
        elif not TIER_SYNC_INCLUDE_WP_LINKED:
            log.info("Tier sync: WordPress-only members excluded (TIER_SYNC_INCLUDE_WP_LINKED not set)")
        log.info(f"Starting periodic sync for {len(members)} linked members ({adopted} WordPress-only)...")
        changed = await sync_members(members, reason="auto-sync", delay_between=5)
        log.info(f"Periodic sync complete. {changed} role(s) updated out of {len(members)} members.")

    @sync_all_members_task.before_loop
    async def before_sync(self):
        await self.wait_until_ready()

    @tasks.loop(time=datetime.time(hour=3, minute=0, tzinfo=datetime.timezone.utc))  # 8pm MST (UTC-7)
    async def daily_report_task(self):
        changes = db.get_tier_changes_since(hours=24)

        reactivations  = [c for c in changes if c["new_tier"] != "unsubscribed" and c["old_tier"] == "unsubscribed" and "webhook" in c.get("reason", "")]
        cancellations  = [c for c in changes if c["new_tier"] == "unsubscribed" and "webhook" in c.get("reason", "")]
        skipped        = [c for c in changes if "skipping downgrade" in c.get("reason", "") or c.get("reason","") == "skipped"]
        new_links      = [c for c in changes if c["old_tier"] in ("none", None) and c["new_tier"] != "unsubscribed"]
        resync         = [c for c in changes if c.get("reason") == "resync-button"]

        needs_attention = skipped  # add more conditions here as needed

        lines = ["**📊 CougConnect Daily Report**"]

        if not changes:
            lines.append("✅ All clear — no tier changes in the last 24 hours.")
        else:
            lines.append(f"✅ **{len(reactivations)}** reactivation(s)  |  ❌ **{len(cancellations)}** cancellation(s)  |  🔗 **{len(new_links)}** new verification(s)  |  🔄 **{len(resync)}** resync(s)")

            if reactivations:
                lines.append("\n**Reactivated:**")
                for c in reactivations:
                    lines.append(f"• <@{c['discord_id']}> (`{c['mp_email']}`) → **{tier_label(c['new_tier'])}** via `{c['reason']}`")

            if cancellations:
                lines.append("\n**Cancelled/Expired:**")
                for c in cancellations:
                    lines.append(f"• <@{c['discord_id']}> (`{c['mp_email']}`) via `{c['reason']}`")

            if new_links:
                lines.append("\n**New Verifications:**")
                for c in new_links:
                    lines.append(f"• <@{c['discord_id']}> (`{c['mp_email']}`) → **{tier_label(c['new_tier'])}**")

        if needs_attention:
            lines.append("\n⚠️ **Needs Attention:**")
            for c in needs_attention:
                lines.append(f"• <@{c['discord_id']}> (`{c['mp_email']}`) — skipped downgrade, use `/sync-member` if confirmed cancelled")

        # Unverified subscribers go to email only, not Discord — but the count
        # belongs in the funnel block so the front door gets watched daily.
        unlinked_lines = await self._check_unlinked_members()
        lines.extend(_onboarding_funnel_lines(verified_24h=len(new_links), never_in_discord=len(unlinked_lines)))
        await post_admin_log("\n".join(lines))

        if unlinked_lines:
            body = (
                "These members subscribed on the site but never verified in Discord — worth a nudge email.\n\n"
                + "\n".join(line.replace("• ", "- ").replace("**", "").replace("`", "") for line in unlinked_lines)
            )
            await send_report_email(
                subject=f"CougConnect: {len(unlinked_lines)} paying subscriber(s) not verified in Discord",
                body_text=body,
            )

    async def _check_unlinked_members(self) -> list[str]:
        """Look up webhook-seen MemberPress accounts with no Discord link; return report lines for active payers."""
        report = []
        for mp_id in db.get_unlinked_ids():
            if db.get_member_by_mp_id(mp_id):
                db.mark_unlinked_verified(mp_id)  # linked since we recorded them — keep the row for time-to-verify
                continue
            try:
                member = await mp.get_member_by_id(mp_id)
                if not member:
                    continue
                tier = mp.resolve_tier(mp.active_ids_from_member_object(member))
                if tier == "unsubscribed":
                    db.remove_unlinked(mp_id)  # no longer paying, stop reporting
                    continue
                report.append(f"• `{member.get('email', f'mp_member_id={mp_id}')}` — **{tier_label(tier)}**")
            except Exception as e:
                log.error(f"Unlinked-member check failed for mp_member_id={mp_id}: {e}")
            await asyncio.sleep(2)
        return report

    @daily_report_task.before_loop
    async def before_daily_report(self):
        await self.wait_until_ready()

    @tasks.loop(time=datetime.time(hour=9, minute=0, tzinfo=datetime.timezone.utc))  # 2am MST (UTC-7)
    async def backup_db_task(self):
        """Nightly SQLite backup emailed to the admin (not posted to Discord)."""
        backup_path = "/tmp/cougconnect-backup.db"
        try:
            src = sqlite3.connect(db.DB_PATH)
            dest = sqlite3.connect(backup_path)
            src.backup(dest)
            dest.close()
            src.close()
            stamp = dt.now(timezone.utc).strftime("%Y-%m-%d")
            sent = await send_report_email(
                subject=f"CougConnect nightly database backup — {stamp}",
                body_text=f"Attached is the nightly SQLite backup for {stamp}.",
                attachment_path=backup_path,
                attachment_name=f"cougconnect-{stamp}.db",
            )
            if not sent:
                await post_admin_log("❌ **Nightly DB backup email failed to send** — check SendGrid config/logs.")
        except Exception as e:
            log.error(f"DB backup failed: {e}")
            await post_admin_log(f"❌ **Nightly DB backup failed:** `{e}`")
        finally:
            if os.path.exists(backup_path):
                os.remove(backup_path)

    @backup_db_task.before_loop
    async def before_backup(self):
        await self.wait_until_ready()

    @tasks.loop(time=datetime.time(hour=16, minute=0, tzinfo=datetime.timezone.utc))  # 9am MST (UTC-7)
    async def expiry_notice_task(self):
        """DM members whose cancelled subscription ends within 3 days (once per expiry date)."""
        members = [m for m in db.get_all_members() if m["tier"] in ("gold", "silver", "insider")]
        for record in members:
            try:
                mp_data = await mp.get_member_by_id(record["mp_member_id"])
                if not mp_data:
                    continue
                sub = mp.parse_subscription_status(mp_data)
                expires_at = sub.get("expires_at")
                if not expires_at or not sub["status"].startswith("Ending on"):
                    continue
                days_left = (dt.strptime(expires_at, "%m/%d/%Y").date() - dt.now(timezone.utc).date()).days
                if not (0 <= days_left <= 3):
                    continue
                if db.expiry_notice_sent(record["discord_id"], expires_at):
                    continue
                user = await bot.fetch_user(int(record["discord_id"]))
                await user.send(
                    f"👋 Heads up — your CougConnect **{tier_label(record['tier'])}** membership "
                    f"ends on **{expires_at}**.\n\n"
                    "Renew at https://cougconnect.com/account/ to keep your access and Discord role. 🏈"
                )
                db.record_expiry_notice(record["discord_id"], expires_at)
                log.info(f"Expiry notice sent to discord_id={record['discord_id']} (expires {expires_at})")
            except Exception as e:
                log.error(f"Expiry notice error for discord_id={record['discord_id']}: {e}")
            await asyncio.sleep(2)

    @expiry_notice_task.before_loop
    async def before_expiry_notice(self):
        await self.wait_until_ready()

    @tasks.loop(time=datetime.time(hour=17, minute=0, tzinfo=datetime.timezone.utc))  # ~10am MT
    async def winback_task(self):
        """DM members 30 days after their downgrade to unsubscribed (once per downgrade)."""
        for row in db.get_downgrades_days_ago(WINBACK_DAYS):
            record = db.get_member_by_discord(row["discord_id"])
            if not record or record["tier"] != "unsubscribed":
                continue  # re-subscribed since, or unlinked
            if db.notice_sent("winback_notices", row["discord_id"], row["changed_at"]):
                continue
            try:
                user = await bot.fetch_user(int(row["discord_id"]))
                await user.send(
                    "👋 It's been a month since your CougConnect membership ended — Cougar "
                    "Nation isn't the same without you.\n\n"
                    "We've kept the insider reports, interviews, and game breakdowns rolling. "
                    "Rejoin anytime at https://cougconnect.com/become-a-subscriber/ and your "
                    "Discord role comes right back with the **Re-sync My Role** button. 🏈"
                )
                db.record_notice("winback_notices", row["discord_id"], row["changed_at"])
                log.info(f"Win-back DM sent to discord_id={row['discord_id']}")
            except Exception as e:
                log.info(f"Win-back DM failed for discord_id={row['discord_id']}: {e}")
            await asyncio.sleep(2)

    @winback_task.before_loop
    async def before_winback(self):
        await self.wait_until_ready()

    @tasks.loop(time=datetime.time(hour=15, minute=0, tzinfo=datetime.timezone.utc))  # ~9am MT
    async def milestone_task(self):
        """Celebrate membership milestones from real paid tenure.

        Tenure comes from WordPress, not from linked_at: someone can pay for
        three years and only link Discord last month, or cancel and resubscribe.
        It counts paid time on ANY tier (Insider/Silver/Gold/Basic) and freezes
        during a lapse, so ten months paid then six months away is still ten.

        Unsubscribed members are never celebrated, and lose the tenure role
        until they come back.
        """
        channel = self.get_channel(MILESTONE_CHANNEL_ID)
        if not channel:
            log.warning(f"Milestone channel {MILESTONE_CHANNEL_ID} not found")
            return

        tenure = await mp.get_tenure_map()
        if tenure is None:
            # None means the fetch failed. Acting on that would strip roles from
            # everyone, so do nothing at all.
            log.warning("Milestone run skipped: tenure unavailable")
            return

        # Count first, so a big batch can be flagged in the admin log before the
        # posts start landing. It never blocks the run: 30 people who signed up
        # on the same day a year ago all deserve the shoutout on the same day.
        targets = _milestone_targets(tenure)

        pending = 0
        for discord_id, info in targets:
            if not info["active"] or info["years"] < 1:
                continue
            if not db.notice_sent("milestone_notices", discord_id, info["years"]):
                pending += 1

        if pending > MILESTONE_BATCH_NOTICE:
            log.info(f"Milestone run: large batch of {pending} pending")
            await post_admin_log(
                f"📣 Heads up — today's milestone run is announcing **{pending}** members. "
                f"A big signup day a year ago will do that. If this looks wrong, the usual "
                f"cause is milestone_notices having been lost; `/seed-milestones` re-records "
                f"everyone silently."
            )

        announced = 0
        for discord_id, info in targets:
            # Not currently paying: no announcement, and take the badge back.
            if not info["active"]:
                try:
                    await assign_milestone_role(int(discord_id), None)
                except Exception as e:
                    log.error(f"Milestone role clear failed for discord_id={discord_id}: {e}")
                continue

            years = info["years"]
            if years < 1 or db.notice_sent("milestone_notices", discord_id, years):
                # Already recorded — this also restores a returning member's
                # role below without re-announcing.
                if years >= 1:
                    try:
                        await assign_milestone_role(int(discord_id), years)
                    except Exception as e:
                        log.error(f"Milestone role restore failed for discord_id={discord_id}: {e}")
                continue

            label = "1 year" if years == 1 else f"{years} years"
            try:
                await assign_milestone_role(int(discord_id), years)
                await channel.send(
                    f"🎉 Shoutout to <@{discord_id}> — **{label}** with CougConnect "
                    f"as a **{tier_label(info['tier'])}** member! "
                    f"Thanks for backing the Cougs with us. 🏈"
                )
                announced += 1
            except Exception as e:
                log.error(f"Milestone post failed for discord_id={discord_id}: {e}")
            finally:
                # Recorded either way — a member whose DM/role/post fails must
                # not be retried every day forever (same call as upgrade nudges).
                db.record_notice("milestone_notices", discord_id, years)
            await asyncio.sleep(2)

        if announced:
            await post_admin_log(f"🎉 Posted {announced} membership milestone(s).")

    @milestone_task.before_loop
    async def before_milestone(self):
        await self.wait_until_ready()

    @tasks.loop(time=datetime.time(hour=17, minute=30, tzinfo=datetime.timezone.utc))  # ~10:30am MT
    async def upgrade_nudge_task(self):
        """One-time DM to Insiders who've been members 5+ months about upgrading."""
        cutoff = dt.now(timezone.utc).replace(tzinfo=None) - datetime.timedelta(days=UPGRADE_NUDGE_DAYS)
        sent = 0
        for record in db.get_all_members():
            if sent >= UPGRADE_NUDGE_DAILY_CAP:
                log.info(f"Upgrade nudge daily cap ({UPGRADE_NUDGE_DAILY_CAP}) reached — resuming tomorrow.")
                break
            if record["tier"] != "insider" or not record["linked_at"]:
                continue
            if db.upgrade_nudge_sent(record["discord_id"]):
                continue
            try:
                linked = dt.fromisoformat(record["linked_at"])
            except ValueError:
                continue
            if linked > cutoff:
                continue
            try:
                user = await bot.fetch_user(int(record["discord_id"]))
                await user.send(
                    "👋 You've been part of CougConnect for 5 months now — thanks for riding with us!\n\n"
                    "Ready for the full experience? **Silver** and **Gold** members get the "
                    "CougConnect swag bag — exclusive gear you can't buy anywhere — and Gold "
                    "includes a custom jersey. Plus insider reports, AMAs, and voice chats with players.\n\n"
                    "Upgrade anytime at https://cougconnect.com/account/ — your Discord role "
                    "updates automatically. 🏈"
                )
                db.record_upgrade_nudge(record["discord_id"])
                sent += 1
                log.info(f"Upgrade nudge sent to discord_id={record['discord_id']} ({sent}/{UPGRADE_NUDGE_DAILY_CAP})")
            except Exception as e:
                # Mark attempted so closed-DM members aren't retried daily forever
                db.record_upgrade_nudge(record["discord_id"])
                log.info(f"Upgrade nudge failed for discord_id={record['discord_id']} (marked attempted): {e}")
            await asyncio.sleep(2)

    @upgrade_nudge_task.before_loop
    async def before_upgrade_nudge(self):
        await self.wait_until_ready()

    @tasks.loop(time=datetime.time(hour=16, minute=0, tzinfo=datetime.timezone.utc))  # ~10am MT
    async def sponsor_spotlight_task(self):
        """Weekly sponsor spotlight in the general channel (Wednesdays, rotating)."""
        if dt.now(timezone.utc).weekday() != 2:  # Wednesday
            return
        sponsors = _load_json(SPONSORS_PATH)
        channel = self.get_channel(GENERAL_CHANNEL_ID)
        if not sponsors or not channel:
            return
        sponsor = sponsors[dt.now(timezone.utc).isocalendar().week % len(sponsors)]
        url = (sponsor.get("url") or "").strip() or None
        description = sponsor.get("blurb", "")
        if url:
            # Visible link line so members know the spotlight is clickable
            display = re.sub(r"^https?://(www\.)?", "", url).rstrip("/")
            description = f"{description}\n\n🔗 [{display}]({url})"
        embed = discord.Embed(
            title=f"🤝 Sponsor Spotlight: {sponsor['name']}",
            description=description,
            url=url,  # makes the embed title a clickable link to the sponsor's site
            color=discord.Color.blue(),
        )
        embed.set_footer(text="CougConnect sponsors keep this community running — show them some love!")
        # Tag the sponsor in the message content (embed mentions don't ping)
        content = None
        if sponsor.get("discord_id"):
            content = f"Say thanks to <@{sponsor['discord_id']}>! 👏"
        elif sponsor.get("email"):
            record = db.get_member_by_email(sponsor["email"])
            if record:
                content = f"Say thanks to <@{record['discord_id']}>! 👏"
        try:
            await channel.send(content=content, embed=embed)
            log.info(f"Sponsor spotlight posted: {sponsor['name']}")
        except Exception as e:
            log.error(f"Sponsor spotlight failed: {e}")

    @sponsor_spotlight_task.before_loop
    async def before_sponsor_spotlight(self):
        await self.wait_until_ready()

    @tasks.loop(time=datetime.time(hour=14, minute=0, tzinfo=datetime.timezone.utc))  # ~8am MT
    async def weekly_digest_task(self):
        """Monday stats digest with week-over-week deltas, posted to admin log."""
        if dt.now(timezone.utc).weekday() != 0:  # Monday
            return
        stats = db.get_stats()
        prev = db.get_previous_snapshot()
        db.save_stats_snapshot(stats)

        def delta(key):
            if not prev:
                return ""
            d = stats[key] - prev[key]
            return f" ({'+' if d >= 0 else ''}{d})" if d != 0 else " (—)"

        new_links = [c for c in db.get_tier_changes_since(hours=168)
                     if c["old_tier"] in ("none", None, "") and c["new_tier"] != "unsubscribed"]
        cancels = [c for c in db.get_tier_changes_since(hours=168) if c["new_tier"] == "unsubscribed"]

        lines = [
            "**📈 CougConnect Weekly Digest**",
            f"🥇 Gold: **{stats['gold']}**{delta('gold')}  |  🥈 Silver: **{stats['silver']}**{delta('silver')}  |  "
            f"🔵 Insider: **{stats['insider']}**{delta('insider')}",
            f"Total verified: **{stats['total']}**{delta('total')}  |  Unsubscribed: {stats['unsubscribed']}{delta('unsubscribed')}",
            f"This week: 🔗 {len(new_links)} new verification(s), ❌ {len(cancels)} cancellation(s)",
        ]
        joins_7d = db.count_joins_since(hours=168)
        rate = f"{len(new_links) / joins_7d * 100:.0f}%" if joins_7d else "—"
        nudges = db.count_join_flag_since("nudged_24h", 168) + db.count_join_flag_since("nudged_72h", 168)
        lines.append(
            f"🚪 Onboarding: {joins_7d} joined → {len(new_links)} verified ({rate})  |  "
            f"🔔 ping-role holders: {_count_ping_role_holders()}  |  nudges sent: {nudges}  |  "
            f"verify failures: {len(db.get_verify_failures_since(168))}"
        )
        if prev:
            lines.append(f"_Compared to {prev['snapshot_date']}_")
        await post_admin_log("\n".join(lines))

    @weekly_digest_task.before_loop
    async def before_weekly_digest(self):
        await self.wait_until_ready()

    @tasks.loop(time=GAMEDAY_OPEN_TIME)
    async def gameday_open_task(self):
        """Monday of game week: open a members-only channel per BYU game."""
        if not GAMEDAY_CHANNELS:
            return
        today = dt.now(MOUNTAIN).date()
        if today.weekday() != 0:  # Monday
            return
        for game in _games_for_week_of(today):
            await open_gameday_channel(game)

    @gameday_open_task.before_loop
    async def before_gameday_open(self):
        await self.wait_until_ready()

    @tasks.loop(time=GAMEDAY_CLOSE_TIME)
    async def gameday_close_task(self):
        """Sunday 23:59 MT: archive each game-week channel's transcript, then delete it."""
        if not GAMEDAY_CHANNELS:
            return
        today = dt.now(MOUNTAIN).date().isoformat()
        for row in db.get_gameday_channels_due(today):
            await close_gameday_channel(row)

    @gameday_close_task.before_loop
    async def before_gameday_close(self):
        await self.wait_until_ready()

    @tasks.loop(time=datetime.time(hour=17, minute=45, tzinfo=datetime.timezone.utc))  # ~10:45am MT
    async def onboarding_drip_task(self):
        """New-member journey DMs: nudge joiners who never verified, check in on those who did.

        Nothing here re-sends. Unverified nudges are stamped on discord_joins and
        check-ins are recorded in onboarding_notices; a failed DM counts as sent,
        because closed DMs would otherwise be retried daily forever (same rule as
        the upgrade nudge). Capped per day so a backlog can never flood, and gated
        by ONBOARDING_DRIP — with ONBOARDING_DRY_RUN it only reports what it would do.
        """
        if not ONBOARDING_DRIP:
            return
        guild = get_guild()
        if not guild:
            return
        tier_role_ids = {rid for rid in ROLE_IDS.values() if rid}
        sent = 0

        # 1) Joined, still here, no tier role, never verified → nudge at 24h, then 72h.
        for hours, flag in zip(UNVERIFIED_NUDGE_HOURS, ("nudged_24h", "nudged_72h")):
            for row in db.get_joins_due(hours, flag):
                if sent >= ONBOARDING_DM_DAILY_CAP:
                    break
                discord_id = int(row["discord_id"])
                member = guild.get_member(discord_id)
                verified = db.get_member_by_discord(row["discord_id"]) is not None
                if not member or verified or any(r.id in tier_role_ids for r in member.roles):
                    db.set_join_flag(row["discord_id"], flag)  # verified since, or left — nothing to nudge
                    continue
                await _dm_member(discord_id, _unverified_nudge_text(), view=_join_dm_view(), what=f"unverified nudge ({hours}h)")
                db.set_join_flag(row["discord_id"], flag)
                sent += 1
                await asyncio.sleep(2)

        # 2) Verified and still paying → day-7 and day-30 check-ins.
        for days in CHECKIN_DAYS:
            step = f"checkin_d{days}"
            for record in db.get_members_linked_days_ago(days):
                if sent >= ONBOARDING_DM_DAILY_CAP:
                    break
                if record["tier"] not in ("gold", "silver", "insider"):
                    continue
                if db.onboarding_step_sent(record["discord_id"], step):
                    continue
                await _dm_member(int(record["discord_id"]), _checkin_dm_text(days, record["tier"]), what=f"day-{days} check-in")
                db.record_onboarding_step(record["discord_id"], step)
                sent += 1
                await asyncio.sleep(2)

        if sent >= ONBOARDING_DM_DAILY_CAP:
            log.info(f"Onboarding drip daily cap ({ONBOARDING_DM_DAILY_CAP}) reached — resuming tomorrow.")
        if sent:
            log.info(f"Onboarding drip: {sent} DM(s) {'simulated' if ONBOARDING_DRY_RUN else 'sent'}")

    @onboarding_drip_task.before_loop
    async def before_onboarding_drip(self):
        await self.wait_until_ready()

    @tasks.loop(minutes=20)
    async def news_task(self):
        """Poll BYU content feeds and post new items to #byu-news."""
        try:
            await aggregator.poll_feeds(self, NEWS_CHANNEL_ID, post_admin_log)
        except Exception as e:
            log.error(f"News aggregator cycle failed: {e}")

    @news_task.before_loop
    async def before_news(self):
        await self.wait_until_ready()


bot = CougConnectBot()


# ── Helpers ────────────────────────────────────────────────────────────────────

def get_guild() -> discord.Guild | None:
    return bot.get_guild(GUILD_ID)


async def post_admin_log(message: str):
    """Post a message to the admin log channel."""
    channel = bot.get_channel(ADMIN_LOG_CHANNEL_ID)
    if channel:
        try:
            await channel.send(message)
        except Exception as e:
            log.error(f"Failed to post admin log: {e}")


# ── Game-week channels ────────────────────────────────────────────────────────

def _game_week_monday(game_date: datetime.date) -> datetime.date:
    """Monday of the week a game falls in. Saturday Sep 5 → Monday Aug 31."""
    return game_date - datetime.timedelta(days=game_date.weekday())


def _game_week_sunday(game_date: datetime.date) -> datetime.date:
    return _game_week_monday(game_date) + datetime.timedelta(days=6)


def _games_for_week_of(monday: datetime.date) -> list[dict]:
    """Every scheduled game whose week starts on this Monday.

    A doubleheader week opens one channel per game; the schedule has none today
    but nothing here assumes a single game.
    """
    out = []
    for game in _load_json(SCHEDULE_PATH):
        try:
            game_date = datetime.date.fromisoformat(game["date"])
        except (KeyError, ValueError):
            log.error(f"Schedule entry has a bad date, skipped: {game!r}")
            continue
        if _game_week_monday(game_date) == monday:
            out.append(game)
    return out


def _gameday_channel_name(game: dict, week_games: list[dict] | None = None) -> str:
    """Discord lowercases channel names and turns spaces into dashes, so build
    the final form ourselves rather than letting it mangle a display string.

    Two games in one week can share an opponent — the Maui Invitational has
    back-to-back "TBD" entries — which would produce two identically named
    channels nobody could tell apart. When that happens, both get the date.
    """
    emoji = "🏈" if game.get("sport") == "football" else "🏀"
    vs_at = "vs" if game.get("home") else "at"
    opponent = re.sub(r"[^a-z0-9]+", "-", game.get("opponent", "").lower()).strip("-")
    base = f"{emoji}-byu-{vs_at}-{opponent}"
    if week_games and sum(1 for g in week_games if _gameday_channel_name(g) == base) > 1:
        base += f"-{datetime.date.fromisoformat(game['date']).strftime('%b-%-d').lower()}"
    return base[:100]


def _gameday_overwrites(guild: discord.Guild) -> tuple[dict, list[int]]:
    """Members-only: @everyone can't see it, every paid/verified/legacy tier can.

    Returns the overwrites plus any configured role IDs missing from the guild,
    so a renamed or deleted role surfaces in the admin log instead of silently
    locking that tier out.
    """
    overwrites = {
        guild.default_role: discord.PermissionOverwrite(view_channel=False),
        guild.me: discord.PermissionOverwrite(
            view_channel=True, send_messages=True, manage_channels=True,
            manage_messages=True, read_message_history=True, attach_files=True,
        ),
    }
    missing = []
    for role_id in GAMEDAY_ACCESS_ROLE_IDS:
        role = guild.get_role(role_id)
        if not role:
            missing.append(role_id)
            continue
        overwrites[role] = discord.PermissionOverwrite(
            view_channel=True, send_messages=True, read_message_history=True,
            add_reactions=True, embed_links=True, attach_files=True,
            use_application_commands=True,
        )
    return overwrites, missing


def _gameday_opener(game: dict) -> str:
    vs_at = "vs" if game.get("home") else "at"
    ping_role_id = int(ONBOARDING.get("ping_roles", {}).get("game_thread", 0) or 0)
    ping = f"<@&{ping_role_id}> " if ping_role_id else ""
    details = []
    if game.get("time"):
        details.append(f"🕐 Kickoff: **{game['time']}**")
    if game.get("tv"):
        details.append(f"📺 Watch on **{game['tv']}**")
    game_date = datetime.date.fromisoformat(game["date"])
    # The rules reminder is grounded in the two that actually break during a
    # game — personal attacks (rule 1) and profanity (rule 3) — and says why,
    # because "players and their families read this" lands harder than "be nice".
    # The outside invite is wrapped in <> so Discord doesn't render an invite
    # card for a server we aren't affiliated with.
    return (
        f"{ping}**Game week is here.** BYU {vs_at} **{game['opponent']}** — "
        f"{game_date.strftime('%A, %B %-d')} 🎉\n"
        + ("\n".join(details) + "\n" if details else "")
        + f"\nThis channel is open all week and closes Sunday night. "
        f"Predictions, film takes, tailgate plans — all of it goes here.\n\n"
        f"**Games get heated. The rules don't change.** Players and their families "
        f"read this server, so no shots at them or at each other, and keep the "
        f"language clean — the full list is in {ob_channel('rules')}.\n"
        f"Need to scream into the void? That's what Twitter is for, or the "
        f"unaffiliated gameday chat: <https://discord.gg/BCBrjx24W8>\n\n"
        f"Go Cougs!"
    )


async def announce_gameday_channel(channel: discord.TextChannel, game: dict):
    """Point members at the new channel from #cougconnect-announcements.

    Sent silent (SUPPRESS_NOTIFICATIONS): announcements reaches the whole
    membership, and a channel opening is not worth a push to all of them. The
    opener inside the game channel still mentions the Game Thread Pings role,
    so the people who opted into that notification still get it.

    Never fatal — a failure here leaves the game channel open and working.
    """
    ann_id = ob_channel_id("announcements")
    if not ann_id:
        return
    ann = bot.get_channel(ann_id)
    if not ann:
        log.error("Game-week announcement skipped — announcements channel not in cache.")
        return

    vs_at = "vs" if game.get("home") else "at"
    emoji = "🏈" if game.get("sport") == "football" else "🏀"
    game_date = datetime.date.fromisoformat(game["date"])
    details = []
    if game.get("time"):
        details.append(f"🕐 **{game['time']}**")
    if game.get("tv"):
        details.append(f"📺 **{game['tv']}**")

    embed = discord.Embed(
        title=f"{emoji} Game week — BYU {vs_at} {game['opponent']}",
        colour=discord.Colour(0x1A3EF0),
        description=(
            f"{game_date.strftime('%A, %B %-d')}"
            + ("  ·  " + "  ·  ".join(details) if details else "")
            + f"\n\n{channel.mention} is open all week — predictions, film takes, "
              "tailgate plans. It closes Sunday night."
        ),
    )
    embed.add_field(
        name="Before you dive in",
        value=("Games get heated; the rules don't. Players and their families read this "
               f"server — the full list is in {ob_channel('rules')}."),
        inline=False,
    )
    view = discord.ui.View(timeout=None)
    view.add_item(discord.ui.Button(
        label="Open the channel", style=discord.ButtonStyle.link,
        url=f"https://discord.com/channels/{GUILD_ID}/{channel.id}", emoji=emoji,
    ))
    try:
        await ann.send(embed=embed, view=view, silent=True)
    except discord.Forbidden:
        await post_admin_log(
            f"⚠️ Game-week announcement skipped — the bot can't post in <#{ann_id}>. "
            "Give **Membership Bot - 2026** a **View Channel** overwrite there "
            f"({channel.mention} itself opened fine)."
        )
    except Exception as e:
        log.error(f"Game-week announcement failed: {e}")
        await post_admin_log(f"⚠️ Game-week announcement failed: `{e}` ({channel.mention} opened fine).")


async def open_gameday_channel(game: dict) -> discord.TextChannel | None:
    """Create this game's channel. Safe to call twice — the DB row short-circuits."""
    guild = get_guild()
    if not guild:
        log.error("Game-week open skipped — guild not in cache.")
        return None

    week_games = _games_for_week_of(_game_week_monday(datetime.date.fromisoformat(game["date"])))
    name = _gameday_channel_name(game, week_games)
    existing = db.gameday_channel_open(game["date"], game.get("opponent", ""))
    if existing:
        log.info(f"Game-week channel already open for {game.get('opponent')} ({existing}).")
        return guild.get_channel(int(existing))

    category = guild.get_channel(GAMEDAY_CATEGORY_IDS.get(game.get("sport"), 0))
    overwrites, missing = _gameday_overwrites(guild)
    close_on = _game_week_sunday(datetime.date.fromisoformat(game["date"])).isoformat()

    if GAMEDAY_DRY_RUN:
        await post_admin_log(
            f"🧪 **Game-week DRY RUN — would open** `#{name}`\n"
            f"• Category: **{category.name if category else '⚠️ none — would land at the top of the server'}**\n"
            f"• Visible to: {len(overwrites) - 2} role(s)"
            + (f" ⚠️ missing role IDs: {missing}" if missing else "")
            + f"\n• Would delete: **{close_on} 23:59 MT**"
            + f"\n• Would announce it silently in <#{ob_channel_id('announcements')}>"
        )
        return None

    try:
        channel = await guild.create_text_channel(
            name=name,
            category=category,
            overwrites=overwrites,
            topic=f"BYU {'vs' if game.get('home') else 'at'} {game.get('opponent')} — "
                  f"game week. This channel is deleted Sunday {close_on} at 11:59pm MT.",
            reason="CougConnect game-week channel",
        )
    except discord.Forbidden:
        await post_admin_log(
            f"❌ **Game-week channel `#{name}` failed — the bot lacks **Manage Channels**.** "
            "Server Settings → Roles → Membership Bot - 2026 → enable Manage Channels."
        )
        return None
    except Exception as e:
        log.error(f"Game-week channel create failed for {name}: {e}")
        await post_admin_log(f"❌ Game-week channel `#{name}` failed to open: `{e}`")
        return None

    db.record_gameday_channel(str(channel.id), game["date"], game.get("opponent", ""),
                              game.get("sport", ""), close_on)
    try:
        msg = await channel.send(
            _gameday_opener(game),
            allowed_mentions=discord.AllowedMentions(roles=True, everyone=False, users=False),
        )
        await msg.pin(reason="Game-week opener")
    except Exception as e:
        log.error(f"Game-week opener failed in {name}: {e}")

    await announce_gameday_channel(channel, game)

    log.info(f"Game-week channel opened: #{name} ({channel.id}), closes {close_on}")
    await post_admin_log(
        f"🏈 **Game-week channel opened** — {channel.mention} "
        f"(BYU {'vs' if game.get('home') else 'at'} {game.get('opponent')}), "
        f"deletes **{close_on} 11:59pm MT**."
        + (f"\n⚠️ Configured role IDs not found in the guild: `{missing}`" if missing else "")
    )
    return channel


async def _gameday_transcript(channel: discord.TextChannel) -> tuple[str, int]:
    """Whole channel history, oldest first, as plain text. Deletion is permanent,
    so this runs before it and a failure here aborts the delete."""
    lines = [
        f"# {channel.name}",
        f"# {channel.topic or ''}",
        f"# archived {dt.now(MOUNTAIN).strftime('%Y-%m-%d %H:%M %Z')}",
        "",
    ]
    count = 0
    async for m in channel.history(limit=None, oldest_first=True):
        stamp = m.created_at.astimezone(MOUNTAIN).strftime("%m/%d %H:%M")
        body = m.clean_content or ""
        for a in m.attachments:
            body += f"\n    [attachment] {a.filename} — {a.url}"
        for e in m.embeds:
            if e.title or e.description:
                body += f"\n    [embed] {e.title or ''} {(e.description or '')[:200]}"
        lines.append(f"[{stamp}] {m.author.display_name}: {body}".rstrip())
        count += 1
    return "\n".join(lines), count


async def close_gameday_channel(row: dict) -> bool:
    """Archive the transcript to the admin log, then delete the channel.

    The delete is irreversible, so it only ever runs on a channel this bot
    recorded in gameday_channels, and only after the transcript upload has
    succeeded — no transcript, no delete.
    """
    guild = get_guild()
    channel = guild.get_channel(int(row["channel_id"])) if guild else None
    label = f"BYU–{row['opponent']}"  # row has no home flag; stay neutral

    if not channel:
        # Deleted by hand already — stop tracking it rather than retrying nightly.
        db.mark_gameday_channel_closed(row["channel_id"])
        log.info(f"Game-week channel {row['channel_id']} ({label}) already gone; marked closed.")
        return True

    if GAMEDAY_DRY_RUN:
        _, count = await _gameday_transcript(channel)
        await post_admin_log(
            f"🧪 **Game-week DRY RUN — would archive and delete** {channel.mention} "
            f"({label}) — **{count}** message(s). Nothing was deleted."
        )
        return False

    try:
        transcript, count = await _gameday_transcript(channel)
    except Exception as e:
        log.error(f"Game-week transcript failed for {channel.name}: {e}")
        await post_admin_log(
            f"⚠️ **{channel.mention} NOT deleted** — couldn't read its history (`{e}`). "
            "The channel is left up so nothing is lost; retrying tomorrow night."
        )
        return False

    admin = bot.get_channel(ADMIN_LOG_CHANNEL_ID)
    if not admin:
        await post_admin_log(f"⚠️ {channel.mention} NOT deleted — admin log channel unavailable.")
        return False
    try:
        await admin.send(
            f"🗄️ **Game-week archive — {label}** (`#{channel.name}`, {count} message(s)). "
            "Deleting the channel now.",
            file=discord.File(
                io.BytesIO(transcript.encode("utf-8")),
                filename=f"{row['game_date']}-{channel.name}.txt",
            ),
        )
    except discord.Forbidden:
        await post_admin_log(
            f"⚠️ **{channel.mention} NOT deleted** — the bot lacks **Attach Files** in the admin "
            "log, so the transcript can't be saved. Enable it and the channel closes tomorrow night."
        )
        return False
    except Exception as e:
        log.error(f"Game-week archive upload failed for {channel.name}: {e}")
        await post_admin_log(f"⚠️ **{channel.mention} NOT deleted** — archive upload failed: `{e}`")
        return False

    try:
        await channel.delete(reason="CougConnect game week over — transcript archived")
    except discord.Forbidden:
        await post_admin_log(
            f"❌ {channel.mention} archived but **not deleted** — the bot lacks **Manage Channels**."
        )
        return False
    except Exception as e:
        log.error(f"Game-week channel delete failed for {channel.name}: {e}")
        await post_admin_log(f"❌ {channel.mention} archived but delete failed: `{e}`")
        return False

    db.mark_gameday_channel_closed(row["channel_id"])
    log.info(f"Game-week channel deleted: #{channel.name} ({count} messages archived)")
    await post_admin_log(f"✅ **Game week closed** — `#{channel.name}` deleted, {count} message(s) archived above.")
    return True


async def send_report_email(subject: str, body_text: str, attachment_path: str | None = None, attachment_name: str | None = None) -> bool:
    """Send an admin report email via SendGrid. Returns True on success."""
    if not SENDGRID_API_KEY or not REPORT_EMAIL_FROM or not REPORT_EMAIL_TO:
        log.error("Report email not sent — SENDGRID_API_KEY / REPORT_EMAIL_FROM / REPORT_EMAIL_TO not all configured.")
        return False
    payload = {
        "personalizations": [{"to": [{"email": REPORT_EMAIL_TO}]}],
        "from": {"email": REPORT_EMAIL_FROM, "name": "CougConnect Bot"},
        "subject": subject,
        "content": [{"type": "text/plain", "value": body_text}],
    }
    if attachment_path:
        with open(attachment_path, "rb") as f:
            payload["attachments"] = [{
                "content": base64.b64encode(f.read()).decode(),
                "filename": attachment_name or os.path.basename(attachment_path),
                "type": "application/octet-stream",
                "disposition": "attachment",
            }]
    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(
                "https://api.sendgrid.com/v3/mail/send",
                json=payload,
                headers={"Authorization": f"Bearer {SENDGRID_API_KEY}"},
            ) as resp:
                if resp.status >= 300:
                    log.error(f"SendGrid error {resp.status}: {await resp.text()}")
                    return False
    except Exception as e:
        log.error(f"Report email failed: {e}")
        return False
    return True


# ── Onboarding helpers ────────────────────────────────────────────────────────

# MemberPress shirt-size dropdown values → the Mailchimp tag names already in use.
# ("mediuk" is the live option value for Medium — a typo frozen into the field.)
SIZE_LABELS = {"x-small": "XS", "small": "Small", "mediuk": "Medium", "medium": "Medium",
               "large": "Large", "x-large": "XL", "xxl": "XXL", "3xl": "3XL"}


async def _dm_member(
    discord_id: int,
    content: str,
    view: discord.ui.View | None = None,
    what: str = "DM",
    embeds: list[discord.Embed] | None = None,
    ignore_dry_run: bool = False,
) -> bool:
    """Send a DM, honouring ONBOARDING_DRY_RUN. Returns True if it went out (or would have).

    Link previews are suppressed on plain-text DMs so a YouTube or shop URL
    doesn't unfurl into a second screen of cards; DMs that carry their own
    embeds can't use the flag (it would hide those too), so their content
    never contains a bare URL.
    """
    if ONBOARDING_DRY_RUN and not ignore_dry_run:
        log.info(f"[dry-run] would send {what} to discord_id={discord_id}")
        await post_admin_log(f"🧪 [dry-run] would send **{what}** to <@{discord_id}>")
        return True
    try:
        user = await bot.fetch_user(discord_id)
        if embeds:
            await user.send(content, embeds=embeds, view=view)
        else:
            await user.send(content, view=view, suppress_embeds=True)
        return True
    except Exception as e:
        log.info(f"{what} to discord_id={discord_id} not delivered (DMs likely closed): {e}")
        return False


def _join_dm_text() -> str:
    return (
        "👋 **Welcome to the CougConnect Discord!**\n\n"
        "Right now you can only see a couple of channels — that changes in 30 seconds:\n"
        f"1️⃣ Tap **Verify Membership** below (or in {ob_chan_md('verification')}) and enter the **email you subscribed with**.\n"
        f"2️⃣ Read the rules in {ob_chan_md('rules')}.\n"
        f"3️⃣ Pick your pings in [Channels & Roles]({ping_roles_link()}) so you know when we go live.\n\n"
        f"Not a member yet? The Discord is part of a [CougConnect membership]({ob_url('subscribe')}).\n\n"
        "Go Cougs 🤙"
    )


def _join_dm_view() -> discord.ui.View:
    """The persistent Verify button plus link buttons — works from a DM."""
    view = VerifyView()
    view.add_item(discord.ui.Button(label="#verification", url=ob_channel_link("verification"), style=discord.ButtonStyle.link, emoji="📍"))
    view.add_item(discord.ui.Button(label="Rules", url=ob_channel_link("rules"), style=discord.ButtonStyle.link, emoji="📜"))
    return view


def _unverified_nudge_text() -> str:
    return (
        "👋 Still on the outside? Verifying takes 30 seconds — tap **Verify Membership** below and enter the "
        f"**email you subscribed with**. If it says 'not found', open a ticket in {ob_chan_md('support')} and "
        "we'll sort it same day.\n\n"
        f"Not a member yet? The Discord is part of a [CougConnect membership]({ob_url('subscribe')}).\n\n"
        "Go Cougs 🤙"
    )


def _welcome_dm_text(tier: str, apartment_slug: str | None = None) -> str:
    tier_perks = {
        "gold": "As a **Gold** member you have the full run of the place — player reports, the Gold lounge, AMAs and "
                "voice chats, plus your custom jersey and $55 in store credit every year you stay.",
        "silver": "As a **Silver** member you get the player reports, the Silver channels and community events, the swag "
                  "box, and $25 in store credit every year you stay.",
        "insider": "As an **Insider** you get the player reports and the community channels — everything the crew talks about all week.",
    }
    apartment_line = ""
    cfg = APARTMENTS.get(apartment_slug) if apartment_slug else None
    if cfg:
        apartment_line = (f"\n\n🏠 We also gave you the **{cfg.get('label', apartment_slug)}** role — "
                          "your complex has its own channel for meeting neighbors.")
    return f"{tier_perks.get(tier, '')}{apartment_line}"


def _discord_tips_embed() -> discord.Embed:
    """Six Discord habits that keep a busy server pleasant. Reused by /faq."""
    tips = discord.Embed(
        title="🔕 Discord tips — it can get noisy",
        colour=discord.Colour(0x4E5058),
        description=(
            "**Mute a channel** — right-click it (phone: press and hold) → **Mute Channel** → pick how long. "
            "You can still read it; it just stops badging you.\n\n"
            "**Only get pinged when it matters** — click the server name at the top → **Notification Settings** → "
            f"**Only @mentions**. The pings you pick in [Channels & Roles]({ping_roles_link()}) still come through.\n\n"
            "**Hide what you never open** — right-click the server → **Hide Muted Channels**. "
            "Click a category name to collapse it.\n\n"
            "**Game-week channels** — a channel opens Monday of every game week and is deleted "
            "Sunday night. Mute it like any other channel if it gets loud.\n\n"
            "**Someone's a problem** — right-click their name → **Block**; their messages collapse for you. "
            f"Then tell us in {ob_chan_md('support')} — we'd rather know.\n\n"
            "**Too many red badges?** — **Shift+Esc** marks the whole server read."
        ),
    )
    return tips


def _welcome_dm_parts(tier: str, apartment_slug: str | None = None) -> tuple[str, list[discord.Embed], discord.ui.View]:
    """Welcome DM v3: one-line headline, a "Start here" card, the tips card, four link buttons.

    Everything lives in embeds + buttons so nothing unfurls and channel names
    read as names instead of "Server › #channel" chips.
    """
    tier_keys = ONBOARDING.get("tier_channels", {}).get(tier, [])
    blurbs = {
        "general": "the daily conversation",
        "news": "every BYU story, auto-posted as it breaks",
        "player_reports": "what you subscribed for",
        "player_ama": "ask the players directly during AMAs",
        "insider_info": "notes and nuggets between reports",
        "vc_recaps": "catch up on any voice chat you missed",
        "gold_lounge": "the Gold lounge",
        "silver": "Silver & Gold members' channel",
    }
    order = ["general", "news", "player_reports", "player_ama", "gold_lounge", "silver", "insider_info", "vc_recaps"]
    keys = ["general", "news"] + [k for k in order if k in tier_keys and k not in ("general", "news")]
    channel_lines = "\n".join(f"{ob_chan_md(k)} — {blurbs.get(k, '')}".rstrip(" —") for k in keys if ob_channel_id(k))

    start = discord.Embed(
        title="Start here",
        colour=discord.Colour(0x1A3EF0),
        description=_welcome_dm_text(tier, apartment_slug),
    )
    start.add_field(name="Your channels", value=channel_lines or "Open the server and look around.", inline=False)
    start.add_field(
        name="🎙️ Voice chats & AMAs",
        value=(f"Announced in {ob_chan_md('announcements')}. Turn on **VC & AMA Pings** in "
               f"[Channels & Roles]({ping_roles_link()}) so you get a heads-up before we go live."),
        inline=False,
    )
    start.add_field(
        name="🏈 Game days & Pick'em",
        value=f"A **BYU vs [opponent]** channel opens Monday of every game week and closes Sunday night · "
              f"Pick'em runs in {ob_chan_md('pickem')}.",
        inline=False,
    )
    start.add_field(
        name="▶️ Podcast · 🛍️ Gear",
        value=(f"New episodes land in {ob_chan_md('youtube')} almost every day — subscribe on YouTube so they hit "
               "your feed too. Player tees (Kyler Kasper, Royal Nights) are in the shop."),
        inline=False,
    )
    start.set_footer(text="Questions? Open a ticket in #support-ticket or just reply here. Go Cougs! 🏈")

    view = discord.ui.View(timeout=None)
    view.add_item(discord.ui.Button(label="Open your channels", url=ob_channel_link("general"), style=discord.ButtonStyle.link, emoji="💬"))
    view.add_item(discord.ui.Button(label="Pick your pings", url=ping_roles_link(), style=discord.ButtonStyle.link, emoji="🔔"))
    view.add_item(discord.ui.Button(label="Subscribe on YouTube", url=ob_url("youtube"), style=discord.ButtonStyle.link, emoji="▶️"))
    view.add_item(discord.ui.Button(label="Shop player tees", url=ob_url("shop"), style=discord.ButtonStyle.link, emoji="🛍️"))

    content = f"🎉 **You're verified — your {tier_label(tier)} role is on.**"
    return content, [start, _discord_tips_embed()], view


def _checkin_dm_text(days: int, tier: str) -> str:
    credit = tier in ("silver", "gold")
    if days == 7:
        return (
            "👋 **One week in — here's how to get the most out of CougConnect:**\n"
            f"🔔 Pick your pings in [Channels & Roles]({ping_roles_link()}) — VC & AMA · Pick'em · Game Thread.\n"
            f"🏈 Game weeks: a **BYU vs [opponent]** channel opens every Monday there's a game and closes "
            f"Sunday night — Pick'em is in {ob_chan_md('pickem')}.\n"
            f"📰 {ob_chan_md('news')} pulls every BYU story into one feed; voice-chat recaps land on the site.\n"
            f"▶️ The daily podcast is on YouTube — [subscribe]({ob_url('youtube')}) so episodes hit your feed.\n"
            f"🛍️ Gear: player tees (Kyler Kasper, Royal Nights, more) in [the shop]({ob_url('shop')})."
            + (" Silver and Gold members earn store credit every year they stay." if credit else "")
            + "\n🔕 Getting noisy? Right-click any channel → **Mute Channel**. Server name → **Notification Settings** → "
            "**Only @mentions** keeps just the pings you picked."
            + f"\n\nAnything confusing? Reply here or open a ticket in {ob_chan_md('support')}. Go Cougs!"
        )
    return (
        "🙏 **A month with us — thank you.** Your membership is what pays the players who show up here.\n\n"
        "One question: what's the one thing you'd like more of? Reply to this message — a human reads every one.\n\n"
        f"Heading to a game? [Wear the crew's colors]({ob_url('shop')})."
        + (" Your yearly store credit unlocks at 12 months." if credit else "")
        + "\n\nGo Cougs 🤙"
    )


def _count_ping_role_holders() -> int:
    """Members holding at least one opt-in ping role (Channels & Roles uptake)."""
    guild = get_guild()
    ids = {int(v) for v in ONBOARDING.get("ping_roles", {}).values() if v}
    if not guild or not ids:
        return 0
    return sum(1 for m in guild.members if not m.bot and any(r.id in ids for r in m.roles))


def _onboarding_funnel_lines(verified_24h: int, never_in_discord: int) -> list[str]:
    """Daily-report block: is the front door working?"""
    joins = db.count_joins_since(hours=24)
    stale = db.count_unverified_joins_older_than(hours=72)
    dm_failed = db.count_join_flag_since("welcome_dm_failed", hours=24)
    failures = db.get_verify_failures_since(hours=24)
    lines = [
        "\n**🚪 Onboarding funnel (24h)**",
        f"👋 Joined Discord: **{joins}**  |  🔗 Verified: **{verified_24h}**  |  ⏳ Joined >72h ago, still unverified: **{stale}**",
        f"💸 Paying, never in Discord: **{never_in_discord}**  |  📪 Welcome DMs undeliverable: **{dm_failed}**  |  ❌ Verify failures: **{len(failures)}**",
    ]
    if failures:
        tops = Counter(f"{f['email']} ({f['reason']})" for f in failures).most_common(5)
        lines.append("Failed attempts: " + ", ".join(f"`{k}` ×{n}" for k, n in tops))
    return lines


async def _mailchimp_sync_member(mp_member_id: int, event: str):
    """Upsert a member into the Mailchimp audience with tier/size tags (flag-gated).

    Replaces two older paths that both wrote to Mailchimp on signup — the
    MemberPress Mailchimp-Tags plugin (which sent a double-opt-in confirmation
    email nobody needed) and a Zap that flipped them to subscribed a minute
    later. One writer, no confirmation email, and tags the email journey can
    branch on (`tier:*`, later `discord-verified`).
    """
    if not mailchimp.SYNC_ENABLED:
        return
    await asyncio.sleep(20)  # let MemberPress commit the signup before reading it back
    try:
        member = await mp.get_member_by_id(mp_member_id)
        if not member:
            return
        email = (member.get("email") or "").strip().lower()
        if not email:
            return
        tier = mp.resolve_tier(mp.active_ids_from_member_object(member))
        tags = ["Customer"]
        if tier in ("gold", "silver", "insider"):
            tags.append(f"tier:{tier}")
            # One umbrella trigger for the Mailchimp journeys: the plan caps a journey at
            # four points, so the drip is three short journeys that all start here.
            tags.append("new-member")
        size = ((member.get("profile") or {}).get("mepr_what_is_your_t_shirt_size") or "").strip().lower()
        if SIZE_LABELS.get(size):
            tags.append(SIZE_LABELS[size])
        if db.mailchimp_synced(mp_member_id, tags):
            return
        ok = await mailchimp.upsert_member(email, member.get("first_name") or "", member.get("last_name") or "", tags)
        if ok:
            db.record_mailchimp_sync(mp_member_id, email, tags)
            log.info(f"Mailchimp synced {email} tags={tags} via {event}")
    except Exception as e:
        log.error(f"Mailchimp sync failed for mp_member_id={mp_member_id}: {e}")


async def assign_role(discord_id: int, tier: str) -> bool:
    """Remove all tier roles and assign the correct one. Returns True on success."""
    guild = get_guild()
    if not guild:
        return False
    member = guild.get_member(discord_id)
    if not member:
        try:
            member = await guild.fetch_member(discord_id)
        except discord.NotFound:
            log.warning(f"Member {discord_id} not found in guild — cannot assign role")
            return False

    tier_roles = [guild.get_role(rid) for rid in ROLE_IDS.values()]
    roles_to_remove = [r for r in tier_roles if r and r in member.roles]
    new_role = guild.get_role(ROLE_IDS.get(tier, 0))
    if not new_role:
        log.warning(f"Role ID not configured for tier '{tier}'")
        return False

    if roles_to_remove:
        await member.remove_roles(*roles_to_remove, reason="CougConnect sync")
    await member.add_roles(new_role, reason=f"CougConnect tier: {tier}")
    return True


def _resolve_apartment_role(guild: discord.Guild, cfg: dict) -> discord.Role | None:
    """Resolve an apartment role by ID (preferred) or exact name."""
    rid = cfg.get("role_id") or 0
    if rid:
        role = guild.get_role(int(rid))
        if role:
            return role
    name = cfg.get("role_name")
    if name:
        return discord.utils.get(guild.roles, name=name)
    return None


async def assign_apartment_role(discord_id: int, slug: str | None) -> None:
    """Grant the Discord role for a member's BYU apartment complex.

    Adds the role matching `slug` (a MemberPress dropdown slug) and removes any
    other apartment role the member holds. A None/unknown/non-housing slug clears
    all apartment roles. Never touches tier roles.
    """
    if not APARTMENTS:
        return
    guild = get_guild()
    if not guild:
        return
    member = guild.get_member(discord_id)
    if not member:
        try:
            member = await guild.fetch_member(discord_id)
        except discord.NotFound:
            return

    # Resolve every configured apartment role once.
    slug_to_role: dict[str, discord.Role] = {}
    for s, cfg in APARTMENTS.items():
        role = _resolve_apartment_role(guild, cfg)
        if role:
            slug_to_role[s] = role
        else:
            log.warning(f"Apartment role for slug '{s}' not found in guild (role_id={cfg.get('role_id')}, role_name={cfg.get('role_name')!r})")

    target_role = slug_to_role.get(slug) if slug else None
    apartment_roles = set(slug_to_role.values())
    to_remove = [r for r in member.roles if r in apartment_roles and r != target_role]

    if to_remove:
        await member.remove_roles(*to_remove, reason="CougConnect apartment sync")
    if target_role and target_role not in member.roles:
        await member.add_roles(target_role, reason=f"CougConnect apartment: {slug}")


def _milestone_targets(tenure: dict) -> list[tuple[str, dict]]:
    """Everyone we can reach on Discord, from BOTH linking systems.

    CougConnect links members to Discord two independent ways and neither is
    complete: the ExpressTech MemberPress-Discord plugin writes
    _ets_memberpress_discord_user_id in WordPress (and the tenure endpoint hands
    that back as discord_id), while the bot keeps its own member_links from the
    verify button. Iterating member_links alone loses everyone in the first
    group — they hold a tier role granted by the WordPress plugin but the bot
    has never heard of them, so they never get a tenure role.

    Keyed by EMAIL rather than discord_id on purpose: if the two systems
    disagree about someone's Discord account, a discord_id key would role both
    of them. This way each member resolves to exactly one ID.

    Returns [(discord_id, tenure_info), ...].
    """
    by_email: dict[str, tuple[str, dict]] = {}

    for email, info in tenure.items():
        discord_id = str(info.get("discord_id") or "")
        if discord_id:
            by_email[email] = (discord_id, info)

    # The bot's own link wins on conflict — the member made it themselves, it is
    # what /get-info reports, and it is the more recently confirmed of the two.
    for record in db.get_all_members():
        email = (record.get("mp_email") or "").strip().lower()
        if not email:
            continue
        info = tenure.get(email)
        if info:
            by_email[email] = (str(record["discord_id"]), info)

    return list(by_email.values())


async def _tier_sync_records(force_adopt: bool = False) -> tuple[list[dict], int]:
    """Members for the nightly tier sync, from BOTH Discord linking systems.

    Same gap as _milestone_targets, but for tier roles: db.get_all_members()
    only knows members who pressed the verify button. Members linked by the
    ExpressTech MemberPress-Discord plugin hold tier roles the bot has never
    seen, so the bot never downgrades them when they stop paying and never
    upgrades them when they change tier — the plugin's own sync is the only
    thing touching them.

    Strictly additive: every existing db record is returned untouched, plus a
    synthesised record for each tenure entry whose email AND discord_id the db
    doesn't already have. The discord_id check matters as much as the email one:
    when an admin /link-member points a Discord user at a different MemberPress
    account than the WordPress-side link knows about (duplicate accounts), the
    old email stops matching any db record — adopting it would synthesise a
    stale record for the same discord_id that runs after the good one and
    overwrites both the role and the stored link. The bot's own link always
    wins. Synthesised records carry mp_member_id=0 (sync
    falls back to the email lookup, then persists the real ID) and the tier the
    tenure endpoint reports, so a member who currently looks paid still gets the
    30-second double-check before any downgrade.

    Adoption is OFF until TIER_SYNC_INCLUDE_WP_LINKED is set — these members have
    never been synced by the bot, so their first pass can move a lot of roles at
    once. Preview it with /tier-sync-preview (which passes force_adopt=True and
    writes nothing), then set the env var to let the nightly sync act on it.

    Returns (records, adopted_count).
    """
    records = list(db.get_all_members())
    if not (force_adopt or TIER_SYNC_INCLUDE_WP_LINKED):
        return records, 0

    known = {(r.get("mp_email") or "").strip().lower() for r in records}
    known.discard("")
    known_ids = {str(r.get("discord_id") or "") for r in records}
    known_ids.discard("")

    tenure = await mp.get_tenure_map()
    if tenure is None:
        # Endpoint down — sync the members we do know rather than skipping the
        # night entirely. Same convention as everywhere else: None changes nothing.
        log.warning("Tier sync: tenure unavailable, syncing bot-linked members only")
        return records, 0

    adopted = 0
    for email, info in tenure.items():
        discord_id = str(info.get("discord_id") or "")
        if not discord_id or email in known or discord_id in known_ids:
            continue
        tier = info.get("tier") or ""
        records.append({
            "discord_id": discord_id,
            "mp_member_id": 0,
            "mp_email": email,
            "tier": tier if tier in ("gold", "silver", "insider") else "none",
        })
        adopted += 1

    return records, adopted


def _resolve_milestone_role(guild: discord.Guild, cfg: dict) -> discord.Role | None:
    """Resolve a tenure role by ID (preferred) or exact name."""
    rid = cfg.get("role_id") or 0
    if rid:
        role = guild.get_role(int(rid))
        if role:
            return role
    name = cfg.get("role_name")
    if name:
        return discord.utils.get(guild.roles, name=name)
    return None


async def assign_milestone_role(discord_id: int, years: int | None) -> bool:
    """Grant the tenure role for `years` and remove any other tenure role.

    Modelled on assign_apartment_role, NOT assign_role: assign_role deliberately
    strips every role in ROLE_IDS before adding one, so a tenure role placed
    there would be wiped by the 3am sync. This only ever touches roles listed in
    milestones.json.

    years=None clears every tenure role — used when a member is no longer
    subscribed, so nobody wears a badge they aren't currently paying for.

    Returns True if Discord roles were actually changed, False if the member
    already had exactly the right role. Callers use this to skip their rate-limit
    sleep when no API call was made — a repeat seed over ~700 already-correct
    members otherwise spends minutes sleeping between no-ops.
    """
    if not MILESTONES:
        return False
    guild = get_guild()
    if not guild:
        return False
    member = guild.get_member(discord_id)
    if not member:
        try:
            member = await guild.fetch_member(discord_id)
        except discord.NotFound:
            return False

    years_to_role: dict[int, discord.Role] = {}
    for cfg in MILESTONES:
        role = _resolve_milestone_role(guild, cfg)
        if role:
            years_to_role[int(cfg["years"])] = role
        else:
            log.warning(
                f"Milestone role for {cfg.get('years')} years not found in guild "
                f"(role_id={cfg.get('role_id')}, role_name={cfg.get('role_name')!r})"
            )

    # Award the highest configured milestone they have reached.
    target_role = None
    if years:
        earned = [y for y in years_to_role if y <= years]
        if earned:
            target_role = years_to_role[max(earned)]

    milestone_roles = set(years_to_role.values())
    to_remove = [r for r in member.roles if r in milestone_roles and r != target_role]

    changed = False
    if to_remove:
        await member.remove_roles(*to_remove, reason="CougConnect tenure sync")
        changed = True
    if target_role and target_role not in member.roles:
        await member.add_roles(target_role, reason=f"CougConnect tenure: {years} years")
        changed = True
    return changed


async def sync_members(members: list, reason: str, delay_between: float) -> int:
    """Re-check each linked member against MemberPress and update tier/role.

    A member whose MemberPress record can't be fetched (transient rest_no_route
    404 / API failure) is SKIPPED — their role is left untouched — rather than
    being treated as unsubscribed. Downgrades are only applied when we get a real
    member object showing no active memberships, and are double-checked after 30s.

    Records may come from member_links or, via _tier_sync_records(), from the
    WordPress-side link with mp_member_id=0 — those resolve by email on the first
    pass and store their real ID.

    Returns the number of roles changed.
    """
    changed = 0
    skipped = 0
    for record in members:
        try:
            new_tier, member_obj = await mp.resolve_tier_or_none(record["mp_member_id"], record["mp_email"])
            if new_tier is None:
                # Couldn't reach MemberPress for this member — do NOT downgrade.
                skipped += 1
                log.warning(f"{reason}: discord_id={record['discord_id']} ({record['mp_email']}) unreachable in MemberPress — skipping, role unchanged")
                await asyncio.sleep(delay_between)
                continue
            apartment_slug = mp.get_apartment_slug(member_obj) if member_obj else None
            # Members adopted from the WordPress-only link arrive with
            # mp_member_id=0 and were resolved by email. Persist the real ID so
            # the next sync hits /members/{id} directly instead of searching.
            mp_id = record["mp_member_id"]
            if member_obj and member_obj.get("id"):
                mp_id = int(member_obj["id"])
            if new_tier != record["tier"]:
                # Double-check EVERY downgrade, including records adopted from the
                # WordPress-side link (tier "none") — those used to skip this guard,
                # so one transiently-empty active_memberships response demoted them.
                if new_tier == "unsubscribed" and record["tier"] != "unsubscribed":
                    await asyncio.sleep(30)
                    verify_tier, _ = await mp.resolve_tier_or_none(record["mp_member_id"], record["mp_email"])
                    if verify_tier is None:
                        skipped += 1
                        log.warning(f"{reason}: discord_id={record['discord_id']} showed unsubscribed then unreachable on recheck — transient API issue, skipping")
                    elif verify_tier == "unsubscribed":
                        log.info(f"{reason} confirmed downgrade for discord_id={record['discord_id']} ({record['mp_email']}) — tier {record['tier']} → unsubscribed")
                        db.log_tier_change(record["discord_id"], record["mp_email"], record["tier"], "unsubscribed", reason=f"{reason}:confirmed")
                        db.upsert_member(record["discord_id"], mp_id, record["mp_email"], "unsubscribed")
                        await assign_role(int(record["discord_id"]), "unsubscribed")
                        await assign_apartment_role(int(record["discord_id"]), None)
                        changed += 1
                    else:
                        log.warning(f"{reason}: discord_id={record['discord_id']} showed unsubscribed then {verify_tier} — transient API issue, skipping")
                else:
                    log.info(f"{reason}: discord_id={record['discord_id']} tier {record['tier']} → {new_tier}")
                    db.log_tier_change(record["discord_id"], record["mp_email"], record["tier"], new_tier, reason=reason)
                    db.upsert_member(record["discord_id"], mp_id, record["mp_email"], new_tier)
                    await assign_role(int(record["discord_id"]), new_tier)
                    await assign_apartment_role(int(record["discord_id"]), apartment_slug)
                    changed += 1
            else:
                db.upsert_member(record["discord_id"], mp_id, record["mp_email"], new_tier)
                # Keep apartment role in sync even when tier is unchanged (member may
                # have set/changed their housing on the website since last sync).
                if new_tier in ("gold", "silver", "insider"):
                    await assign_apartment_role(int(record["discord_id"]), apartment_slug)
        except Exception as e:
            log.error(f"{reason} error for discord_id={record['discord_id']}: {e}")
        await asyncio.sleep(delay_between)
    if skipped:
        log.warning(f"{reason}: {skipped} member(s) skipped due to MemberPress API failures (roles left unchanged).")
    return changed


def tier_label(tier: str) -> str:
    return {"gold": "Gold", "silver": "Silver", "insider": "Insider", "unsubscribed": "Unsubscribed"}.get(tier, tier.title())


async def send_welcome_dm(discord_id: int, tier: str, apartment_slug: str | None = None, email: str | None = None):
    """Welcome DM after a successful verification: tier perks + a linked channel guide.

    A failed DM is recorded (not just logged) so the daily report can count
    members who got a role but no orientation. Also tags the member
    `discord-verified` in Mailchimp (flag-gated) so the email journey stops
    nudging them to verify.
    """
    content, embeds, view = _welcome_dm_parts(tier, apartment_slug)
    sent = await _dm_member(discord_id, content, view=view, embeds=embeds, what="welcome DM")
    db.set_join_flag(str(discord_id), "welcome_dm_sent" if sent else "welcome_dm_failed")
    if email:
        try:
            await mailchimp.tag_verified(email)
        except Exception as e:
            log.info(f"Mailchimp verified-tag failed for {email}: {e}")


def add_active_subscriptions_field(embed: discord.Embed, mp_member: dict | None):
    """List all active MemberPress subscriptions on the embed when there's more than one."""
    if not mp_member:
        return
    active_memberships = mp_member.get("active_memberships", [])
    if len(active_memberships) > 1:
        names = [m.get("title", f"ID {m.get('id')}") if isinstance(m, dict) else f"ID {m}" for m in active_memberships]
        embed.add_field(name="Active Subscriptions", value="\n".join(f"• {n}" for n in names), inline=False)


def tier_color(tier: str) -> discord.Color:
    return {
        "gold": discord.Color.gold(),
        "silver": discord.Color.light_grey(),
        "insider": discord.Color.blue(),
        "unsubscribed": discord.Color.dark_grey(),
    }.get(tier, discord.Color.default())


# ── Verify button (self-service) ───────────────────────────────────────────────

class VerifyView(discord.ui.View):
    def __init__(self):
        super().__init__(timeout=None)

    @discord.ui.button(label="Verify Membership", style=discord.ButtonStyle.primary, emoji="🔐", custom_id="verify_membership")
    async def verify_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        try:
            discord_id = str(interaction.user.id)
            existing = db.get_member_by_discord(discord_id)
            if existing:
                embed = discord.Embed(
                    title="Already Verified",
                    description=(
                        f"Your account is already linked to `{existing['mp_email']}` "
                        f"with the **{tier_label(existing['tier'])}** role.\n\n"
                        f"Just renewed or upgraded? Use **Re-sync My Role** in {ob_channel('unsubscribed')}. "
                        f"Wrong email on file? Open a ticket in {ob_channel('support')}."
                    ),
                    color=tier_color(existing["tier"]),
                )
                await interaction.response.send_message(embed=embed, ephemeral=True)
                return
            token = db.create_token(discord_id)
            url = f"{BOT_PUBLIC_URL}/verify-page?token={token}&discord_id={discord_id}"
            embed = discord.Embed(
                title="Verify Your Membership",
                description=(
                    "Click the button below to verify your CougConnect subscription.\n\n"
                    "You'll enter your CougConnect email address to confirm your subscription. "
                    "This link expires in **15 minutes**."
                ),
                color=discord.Color.blue(),
            )
            view = discord.ui.View()
            view.add_item(discord.ui.Button(label="Verify My Membership", url=url, style=discord.ButtonStyle.link, emoji="🔗"))
            await interaction.response.send_message(embed=embed, view=view, ephemeral=True)
        except Exception as e:
            log.error(f"Error in verify_button: {e}")
            try:
                await interaction.response.send_message("Something went wrong. Please try again in a moment.", ephemeral=True)
            except Exception:
                pass


# ── Re-sync button (unsubscribed channel) ─────────────────────────────────────

class ReSyncView(discord.ui.View):
    def __init__(self):
        super().__init__(timeout=None)
        self.add_item(discord.ui.Button(
            label="Upgrade Membership",
            url="https://cougconnect.com/account/",
            style=discord.ButtonStyle.link,
            emoji="⬆️",
        ))

    @discord.ui.button(label="Re-sync My Role", style=discord.ButtonStyle.success, emoji="🔄", custom_id="resync_role")
    async def resync_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.defer(ephemeral=True)
        discord_id = str(interaction.user.id)
        record = db.get_member_by_discord(discord_id)
        if not record:
            embed = discord.Embed(
                title="Not Verified",
                description=(
                    "Your Discord account isn't linked to a CougConnect membership yet.\n\n"
                    "Head to the verify channel and click **Verify Membership** to get started."
                ),
                color=discord.Color.red(),
            )
            await interaction.followup.send(embed=embed, ephemeral=True)
            return

        member_obj, active_ids = await mp.get_member_and_active_ids(record["mp_member_id"], record["mp_email"])
        new_tier = mp.resolve_tier(active_ids)

        if new_tier == "unsubscribed":
            embed = discord.Embed(
                title="No Active Subscription Found",
                description=(
                    f"We checked your account (`{record['mp_email']}`) but couldn't find an active membership.\n\n"
                    "If you just subscribed, it may take a minute — please try again shortly. "
                    "Otherwise, click **Upgrade Membership** above to subscribe."
                ),
                color=discord.Color.dark_grey(),
            )
            await interaction.followup.send(embed=embed, ephemeral=True)
            return

        old_tier = record["tier"]
        db.log_tier_change(discord_id, record["mp_email"], old_tier, new_tier, reason="resync-button")
        db.upsert_member(discord_id, record["mp_member_id"], record["mp_email"], new_tier)
        await assign_role(int(discord_id), new_tier)
        apartment_slug = mp.get_apartment_slug(member_obj) if member_obj else None
        await assign_apartment_role(int(discord_id), apartment_slug)

        embed = discord.Embed(
            title="✅ Role Updated!",
            description=f"Your membership was confirmed and you've been given the **{tier_label(new_tier)}** role. Welcome back!",
            color=tier_color(new_tier),
        )
        await interaction.followup.send(embed=embed, ephemeral=True)
        log.info(f"Resync button: discord_id={discord_id} {old_tier} → {new_tier}")


# ── Message flagging (mods react 🚩 → log + delete) ───────────────────────────

class FlagReasonModal(discord.ui.Modal, title="Flag Reason"):
    reason = discord.ui.TextInput(
        label="Why was this message flagged?",
        style=discord.TextStyle.paragraph,
        required=True,
        max_length=500,
    )

    def __init__(self, flag_id: int, log_message: discord.Message):
        super().__init__()
        self.flag_id = flag_id
        self.log_message = log_message

    async def on_submit(self, interaction: discord.Interaction):
        db.set_flag_reason(self.flag_id, str(self.reason))
        embed = self.log_message.embeds[0]
        embed.add_field(name="Reason", value=str(self.reason), inline=False)
        await self.log_message.edit(embed=embed, view=None)
        await interaction.response.send_message("Reason saved.", ephemeral=True)


class FlagReasonView(discord.ui.View):
    def __init__(self):
        super().__init__(timeout=None)

    @discord.ui.button(label="Add Reason", style=discord.ButtonStyle.secondary, emoji="📝", custom_id="flag_add_reason")
    async def add_reason(self, interaction: discord.Interaction, button: discord.ui.Button):
        # Flag id is carried in the log embed footer ("Flag #<id>") so the
        # button survives bot restarts without per-message state.
        try:
            flag_id = int(interaction.message.embeds[0].footer.text.split("#")[1])
        except (IndexError, ValueError, AttributeError):
            await interaction.response.send_message("Couldn't find the flag record for this log entry.", ephemeral=True)
            return
        await interaction.response.send_modal(FlagReasonModal(flag_id, interaction.message))


@bot.event
async def on_member_join(member: discord.Member):
    """First contact. A new joiner can only see three channels, so say the one
    thing that matters — verify — and hand them the button to do it from the DM."""
    if member.bot or member.guild.id != GUILD_ID:
        return
    db.record_join(str(member.id))
    existing = db.get_member_by_discord(str(member.id))
    if existing and existing["tier"] in ("gold", "silver", "insider"):
        # A rejoin: restore the role they already earned and skip the tour.
        try:
            await assign_role(member.id, existing["tier"])
        except Exception as e:
            log.error(f"Role restore on rejoin failed for discord_id={member.id}: {e}")
        if ONBOARDING_JOIN_DM:
            # Members who connected from the website arrive here already linked
            # but without the guide — send the full welcome, not a "welcome back".
            await send_welcome_dm(member.id, existing["tier"], None, existing.get("mp_email"))
        return
    if not ONBOARDING_JOIN_DM:
        return
    sent = await _dm_member(member.id, _join_dm_text(), view=_join_dm_view(), what="join DM")
    log.info(f"Join DM {'sent' if sent else 'not delivered'} to discord_id={member.id}")


@bot.event
async def on_message(message: discord.Message):
    # The onboarding DMs say "reply here" — route those replies to the admin log
    # so a human actually sees them. Guild messages pass straight through.
    if message.guild is None and not message.author.bot and message.content:
        await post_admin_log(f"💬 **DM reply from** <@{message.author.id}> ({message.author}): {message.content[:1500]}")
    await bot.process_commands(message)


@bot.event
async def on_raw_reaction_add(payload: discord.RawReactionActionEvent):
    if payload.guild_id != GUILD_ID or str(payload.emoji) != FLAG_EMOJI:
        return
    guild = get_guild()
    if not guild:
        return
    flagger = guild.get_member(payload.user_id)
    if not flagger or flagger.bot or not flagger.guild_permissions.manage_messages:
        return
    channel = guild.get_channel(payload.channel_id) or bot.get_channel(payload.channel_id)
    if not channel:
        return
    try:
        message = await channel.fetch_message(payload.message_id)
    except (discord.NotFound, discord.Forbidden):
        return

    content = message.content or "(no text — embed/attachment only)"
    attachments = ", ".join(a.url for a in message.attachments)
    flag_id = db.log_flagged_message(
        str(message.id), str(channel.id), getattr(channel, "name", "?"),
        str(message.author.id), str(message.author),
        message.content + (f"\n[attachments: {attachments}]" if attachments else ""),
        str(flagger.id), str(flagger),
    )

    embed = discord.Embed(
        title="🚩 Message Flagged & Deleted",
        color=discord.Color.red(),
        timestamp=discord.utils.utcnow(),
    )
    author_flags = db.count_flags_for_author(str(message.author.id))
    embed.add_field(name="Author", value=f"{message.author.mention} ({message.author})", inline=True)
    embed.add_field(name="Author's Flag Count", value=f"{author_flags} total", inline=True)
    embed.add_field(name="Channel", value=channel.mention, inline=True)
    embed.add_field(name="Flagged by", value=flagger.mention, inline=True)
    embed.add_field(name="Content", value=content[:1024], inline=False)
    if attachments:
        embed.add_field(name="Attachments", value=attachments[:1024], inline=False)
    embed.set_footer(text=f"Flag #{flag_id}")

    log_channel = bot.get_channel(MOD_LOG_CHANNEL_ID)
    if log_channel:
        try:
            await log_channel.send(embed=embed, view=FlagReasonView())
        except Exception as e:
            log.error(f"Failed to post flag log for flag_id={flag_id}: {e}")

    try:
        await message.delete()
        log.info(f"Flagged message {message.id} in #{getattr(channel, 'name', '?')} deleted (flag_id={flag_id}, by {flagger})")
    except discord.Forbidden:
        log.error(f"Missing permission to delete flagged message {message.id} in #{getattr(channel, 'name', '?')} (flag_id={flag_id})")
        if log_channel:
            await log_channel.send(f"⚠️ Flag #{flag_id}: I couldn't delete the message — missing **Manage Messages** in {channel.mention}.")
    except discord.NotFound:
        pass


# ── Slash commands ─────────────────────────────────────────────────────────────

@bot.tree.command(name="link-member", description="Manually link a Discord user to their CougConnect account")
@app_commands.describe(user="Discord member to link", email="Their CougConnect email address")
@app_commands.default_permissions(manage_roles=True)
async def link_member(interaction: discord.Interaction, user: discord.Member, email: str):
    await interaction.response.defer(ephemeral=True)
    mp_member = await mp.get_member_by_email(email)
    if not mp_member:
        await interaction.followup.send(f"❌ No MemberPress account found for `{email}`.", ephemeral=True)
        return

    mp_id = mp_member.get("id")
    active_ids = mp.active_ids_from_member_object(mp_member)
    if not active_ids:
        # Email search showed no active memberships. Confirm with a direct fetch
        # before trusting it — a flaky by-id lookup must not fabricate an unsubscribe.
        confirm_tier, _ = await mp.resolve_tier_or_none(mp_id, email)
        if confirm_tier is None:
            await interaction.followup.send(
                f"⚠️ Found `{email}` but couldn't confirm their membership status right now "
                "(MemberPress API error). **No role assigned.** Try `/link-member` again in a minute.",
                ephemeral=True,
            )
            log.warning(f"link-member: {email} found by email but status unconfirmable — aborting to avoid false downgrade")
            return
        tier = confirm_tier
    else:
        tier = mp.resolve_tier(active_ids)

    existing = db.get_member_by_discord(str(user.id))
    old_tier = existing["tier"] if existing else "none"
    db.log_tier_change(str(user.id), email, old_tier, tier, reason="link-member")
    db.upsert_member(str(user.id), mp_id, email, tier)
    db.mark_unlinked_verified(mp_id)
    success = await assign_role(user.id, tier)
    asyncio.create_task(wp_link.push_link(email, user.id, user.display_name))

    if success:
        await interaction.followup.send(
            f"✅ Linked **{user.display_name}** to `{email}` — assigned **{tier_label(tier)}** role.",
            ephemeral=True,
        )
    else:
        await interaction.followup.send(
            "⚠️ Saved link but couldn't assign role — check role IDs in config.",
            ephemeral=True,
        )


@bot.tree.command(name="unlink-member", description="Remove a member's CougConnect link and set to Unsubscribed")
@app_commands.describe(user="Discord member to unlink")
@app_commands.default_permissions(administrator=True)
async def unlink_member(interaction: discord.Interaction, user: discord.Member):
    await interaction.response.defer(ephemeral=True)
    existing = db.get_member_by_discord(str(user.id))
    if not existing:
        await interaction.followup.send(f"❌ {user.display_name} has no linked account.", ephemeral=True)
        return
    db.remove_member(str(user.id))
    await assign_role(user.id, "unsubscribed")
    asyncio.create_task(wp_link.push_unlink(existing["mp_email"]))
    await interaction.followup.send(f"✅ Unlinked **{user.display_name}** and set role to Unsubscribed.", ephemeral=True)


@bot.tree.command(name="sync-member", description="Re-fetch membership status from MemberPress and update role")
@app_commands.describe(user="Discord member to sync")
@app_commands.default_permissions(manage_roles=True)
async def sync_member(interaction: discord.Interaction, user: discord.Member):
    await interaction.response.defer(ephemeral=True)
    existing = db.get_member_by_discord(str(user.id))
    if not existing:
        await interaction.followup.send(f"❌ {user.display_name} has no linked account. Use `/link-member` first.", ephemeral=True)
        return

    tier, member_obj = await mp.resolve_tier_or_none(existing["mp_member_id"], existing["mp_email"])
    if tier is None:
        await interaction.followup.send(
            f"⚠️ Couldn't reach MemberPress for **{user.display_name}** (`{existing['mp_email']}`) right now — "
            "the API returned an error. **Role left unchanged.** Try again in a minute.",
            ephemeral=True,
        )
        log.warning(f"sync-member: {existing['mp_email']} unreachable in MemberPress — role left unchanged")
        return
    if tier != existing["tier"]:
        db.log_tier_change(str(user.id), existing["mp_email"], existing["tier"], tier, reason="sync-member")
    db.upsert_member(str(user.id), existing["mp_member_id"], existing["mp_email"], tier)
    await assign_role(user.id, tier)
    apartment_slug = mp.get_apartment_slug(member_obj) if member_obj else None
    if tier in ("gold", "silver", "insider"):
        await assign_apartment_role(user.id, apartment_slug)
    else:
        await assign_apartment_role(user.id, None)
    await interaction.followup.send(
        f"✅ Synced **{user.display_name}** — current tier: **{tier_label(tier)}**", ephemeral=True
    )


@bot.tree.command(name="lookup-email", description="Find which Discord account is linked to an email address")
@app_commands.describe(email="The CougConnect email to look up")
@app_commands.default_permissions(manage_roles=True)
async def lookup_email(interaction: discord.Interaction, email: str):
    record = db.get_member_by_email(email.strip().lower())
    if not record:
        await interaction.response.send_message(f"❌ No Discord account is linked to `{email}`.", ephemeral=True)
        return
    try:
        user = await bot.fetch_user(int(record["discord_id"]))
        user_display = f"{user.mention} (`{user.name}` — ID: `{record['discord_id']}`)"
    except Exception:
        user_display = f"Unknown user (ID: `{record['discord_id']}`)"
    embed = discord.Embed(title="Email Lookup", color=tier_color(record["tier"]))
    embed.add_field(name="Email", value=record["mp_email"], inline=False)
    embed.add_field(name="Discord Account", value=user_display, inline=False)
    embed.add_field(name="Tier", value=tier_label(record["tier"]), inline=True)
    embed.add_field(name="Linked On", value=record["linked_at"][:10] if record["linked_at"] else "—", inline=True)
    await interaction.response.send_message(embed=embed, ephemeral=True)


@bot.tree.command(name="get-info", description="Show the email address tied to a Discord user")
@app_commands.describe(user="Discord member to look up")
@app_commands.default_permissions(manage_roles=True)
async def get_info(interaction: discord.Interaction, user: discord.Member):
    await interaction.response.defer(ephemeral=True)
    record = db.get_member_by_discord(str(user.id))
    if not record:
        await interaction.followup.send(f"❌ **{user.display_name}** has not verified their membership.", ephemeral=True)
        return
    embed = discord.Embed(title="Member Info", color=discord.Color.blue())
    embed.add_field(name="Discord", value=f"{user.mention} (`{user.id}`)", inline=False)
    embed.add_field(name="Email", value=record["mp_email"], inline=False)
    embed.add_field(name="Tier", value=tier_label(record["tier"]), inline=True)
    embed.add_field(name="Linked", value=record["linked_at"][:10] if record["linked_at"] else "—", inline=True)

    mp_member = await mp.get_member_by_id(record["mp_member_id"])
    add_active_subscriptions_field(embed, mp_member)

    await interaction.followup.send(embed=embed, ephemeral=True)


@bot.tree.command(name="profile", description="Full membership profile for a Discord user")
@app_commands.describe(user="Discord member to look up")
@app_commands.default_permissions(manage_roles=True)
async def profile(interaction: discord.Interaction, user: discord.Member):
    await interaction.response.defer(ephemeral=True)
    record = db.get_member_by_discord(str(user.id))
    if not record:
        await interaction.followup.send(f"❌ **{user.display_name}** has not verified their membership.", ephemeral=True)
        return

    mp_data = await mp.get_member_by_id(record["mp_member_id"])
    sub_status = mp.parse_subscription_status(mp_data) if mp_data else {"status": "Unknown", "expires_at": None}

    embed = discord.Embed(
        title=f"Profile — {user.display_name}",
        color=tier_color(record["tier"]),
    )
    embed.set_thumbnail(url=user.display_avatar.url)
    embed.add_field(name="Email", value=record["mp_email"], inline=False)
    embed.add_field(name="Tier", value=tier_label(record["tier"]), inline=True)
    embed.add_field(name="Status", value=sub_status["status"], inline=True)
    if sub_status.get("expires_at"):
        embed.add_field(name="Expires", value=sub_status["expires_at"], inline=True)
    embed.add_field(name="Linked On", value=record["linked_at"][:10] if record["linked_at"] else "—", inline=True)
    embed.add_field(name="Last Synced", value=record["last_synced"][:10] if record["last_synced"] else "—", inline=True)

    apartment_slug = mp.get_apartment_slug(mp_data) if mp_data else None
    if apartment_slug:
        cfg = APARTMENTS.get(apartment_slug)
        embed.add_field(name="Apartment", value=cfg.get("label", apartment_slug) if cfg else apartment_slug, inline=True)

    add_active_subscriptions_field(embed, mp_data)

    embed.set_footer(text="CougConnect Membership Bot")
    await interaction.followup.send(embed=embed, ephemeral=True)


@bot.tree.command(name="faq", description="View CougConnect FAQs")
@app_commands.describe(number="Optional: enter a question number to see just that answer")
async def faq(interaction: discord.Interaction, number: int | None = None):
    faqs = load_faq()
    if not faqs:
        await interaction.response.send_message("❌ No FAQs configured yet.", ephemeral=True)
        return

    if number is not None:
        idx = number - 1
        if idx < 0 or idx >= len(faqs):
            await interaction.response.send_message(f"❌ No FAQ #{number}. There are {len(faqs)} FAQs.", ephemeral=True)
            return
        item = faqs[idx]
        embed = discord.Embed(
            title=f"FAQ #{number}: {item['q']}",
            description=item["a"],
            color=discord.Color.blue(),
        )
        await interaction.response.send_message(embed=embed, ephemeral=True)
        return

    # Ephemeral: a new member running /faq in a busy channel shouldn't dump
    # thirteen answers on everyone else.
    embed = discord.Embed(title="CougConnect FAQ", color=discord.Color.blue())
    for i, item in enumerate(faqs, 1):
        embed.add_field(name=f"{i}. {item['q']}", value=item["a"], inline=False)
    embed.set_footer(text="Use /faq <number> to view a specific answer")
    await interaction.response.send_message(embed=embed, ephemeral=True)


@bot.tree.command(name="sync-all", description="Manually trigger a full membership sync against MemberPress")
@app_commands.default_permissions(administrator=True)
async def sync_all(interaction: discord.Interaction):
    await interaction.response.defer(ephemeral=True)
    members = db.get_all_members()
    if not members:
        await interaction.followup.send("No linked members to sync.", ephemeral=True)
        return
    changed = await sync_members(members, reason="sync-all", delay_between=0.5)
    await interaction.followup.send(
        f"✅ Sync complete — checked **{len(members)}** members, updated **{changed}** role(s).",
        ephemeral=True,
    )


@bot.tree.command(name="seed-milestones", description="Backfill tenure roles silently — assigns roles, announces nobody")
@app_commands.default_permissions(administrator=True)
async def seed_milestones(interaction: discord.Interaction):
    """Bring existing members up to date without spamming the channel.

    Hundreds of members are already years in. Running milestone_task cold would
    announce every one of them, so this records their milestones as already
    handled and applies the roles quietly.

    Notices are written for EVERY year up to their current one, so a member at
    three years never later triggers a "1 year" post. Roles go only to members
    who are currently subscribed; lapsed members still get their notices
    recorded so they are never retro-announced if they return.
    """
    await interaction.response.defer(ephemeral=True)

    tenure = await mp.get_tenure_map()
    if tenure is None:
        await interaction.followup.send(
            "❌ Could not reach the tenure endpoint — nothing was changed. "
            "Check CCSB_TENURE_URL and CCSB_TENURE_KEY.",
            ephemeral=True,
        )
        return

    seeded = 0
    roled = 0
    already_correct = 0
    skipped_lapsed = 0
    corrected = 0

    for discord_id, info in _milestone_targets(tenure):
        if info["years"] < 1:
            continue

        # Tenure can be recalculated downward — overlapping memberships used to
        # be double-counted — so drop notices for years they have not actually
        # reached. Leaving them would silently suppress the real milestone.
        corrected += db.clear_notices_above("milestone_notices", discord_id, info["years"])

        for year in range(1, info["years"] + 1):
            if not db.notice_sent("milestone_notices", discord_id, year):
                db.record_notice("milestone_notices", discord_id, year)
        seeded += 1

        if not info["active"]:
            skipped_lapsed += 1
            continue

        try:
            # Only sleep when the call actually hit the Discord API. A repeat seed
            # is almost entirely no-ops, and sleeping through those turned a fast
            # re-run into a five-minute one for nothing.
            if await assign_milestone_role(int(discord_id), info["years"]):
                roled += 1
                await asyncio.sleep(0.5)
            else:
                already_correct += 1
        except Exception as e:
            log.error(f"Seed role failed for discord_id={discord_id}: {e}")
            await asyncio.sleep(0.5)

    await post_admin_log(
        f"🌱 Milestone seed: {seeded} member(s) recorded, {roled} role(s) applied, "
        f"{already_correct} already correct, "
        f"{skipped_lapsed} lapsed skipped, {corrected} stale notice(s) cleared. "
        f"No announcements sent."
    )
    await interaction.followup.send(
        f"✅ Seeded **{seeded}** member(s) — **{roled}** role(s) applied, "
        f"**{already_correct}** already correct, "
        f"**{skipped_lapsed}** lapsed skipped, **{corrected}** stale notice(s) cleared.\n"
        f"Nothing was announced. The next milestone run will only post genuinely new ones.",
        ephemeral=True,
    )


@bot.tree.command(name="tier-sync-preview", description="Dry run: what the nightly tier sync would change (writes nothing)")
@app_commands.default_permissions(administrator=True)
@app_commands.describe(only_new="Only check members the bot has never synced (WordPress-linked only). Default true.")
async def tier_sync_preview(interaction: discord.Interaction, only_new: bool = True):
    """Report what sync_all_members_task would do, without touching anything.

    Exists because _tier_sync_records() adopts members the bot has never seen —
    their first sync could move a lot of roles at once, and that deserves eyes on
    it before it runs unattended at 3am. Deliberately does no db writes and no
    role changes: it only reads MemberPress.
    """
    await interaction.response.defer(ephemeral=True)

    records, adopted = await _tier_sync_records(force_adopt=True)
    if adopted == 0 and only_new:
        await interaction.followup.send(
            "✅ No WordPress-only members to adopt — `member_links` already covers "
            "everyone the tenure endpoint knows about. Run with `only_new: False` "
            "to preview the full sync.",
            ephemeral=True,
        )
        return

    # Adopted records are the tail of the list, in the order _tier_sync_records
    # appended them.
    targets = records[len(records) - adopted:] if only_new else records

    # A deferred interaction can only be followed up for 15 minutes and each
    # member costs ~0.7s, so cap the walk and say so rather than timing out
    # halfway with a report that looks complete.
    PREVIEW_CAP = 900
    truncated = max(0, len(targets) - PREVIEW_CAP)
    targets = targets[:PREVIEW_CAP]

    changes: list[str] = []
    unreachable = 0
    unchanged = 0
    counts: dict[str, int] = {}

    for record in targets:
        try:
            new_tier, _ = await mp.resolve_tier_or_none(record["mp_member_id"], record["mp_email"])
        except Exception as e:
            log.error(f"tier-sync-preview error for {record['mp_email']}: {e}")
            unreachable += 1
            continue
        if new_tier is None:
            unreachable += 1
        elif new_tier != record["tier"]:
            counts[f"{record['tier']} → {new_tier}"] = counts.get(f"{record['tier']} → {new_tier}", 0) + 1
            changes.append(f"{record['mp_email']} <@{record['discord_id']}> {record['tier']} → {new_tier}")
        else:
            unchanged += 1
        await asyncio.sleep(0.3)

    summary = (
        f"**Tier sync dry run** — {'WordPress-only members' if only_new else 'all linked members'}\n"
        f"Checked **{len(targets)}** · **{len(changes)}** would change · "
        f"**{unchanged}** already correct · **{unreachable}** unreachable (would be skipped)\n"
    )
    if counts:
        summary += "\n".join(f"• `{k}` — **{v}**" for k, v in sorted(counts.items(), key=lambda kv: -kv[1]))
    if truncated:
        summary += f"\n⚠️ Stopped after {PREVIEW_CAP} — **{truncated}** member(s) not checked."
    summary += "\n\n_Nothing was changed._"

    if changes:
        import io
        buf = io.BytesIO("\n".join(changes).encode("utf-8"))
        await interaction.followup.send(
            summary,
            file=discord.File(buf, filename="tier-sync-preview.txt"),
            ephemeral=True,
        )
    else:
        await interaction.followup.send(summary, ephemeral=True)


@bot.tree.command(name="sync-links-to-wp", description="Push every Discord link the bot knows to WordPress (account-page status)")
@app_commands.default_permissions(administrator=True)
async def sync_links_to_wp(interaction: discord.Interaction):
    """One-time backfill so the account page can show 'Verified' for members who
    linked through the bot before WordPress kept a copy. Safe to re-run."""
    await interaction.response.defer(ephemeral=True)
    if not wp_link.configured():
        await interaction.followup.send("❌ CCSB_TENURE_URL / CCSB_TENURE_KEY are not set — nothing sent.", ephemeral=True)
        return
    guild = get_guild()
    links = []
    for r in db.get_all_members():
        if r["tier"] not in ("gold", "silver", "insider") or not r.get("mp_email"):
            continue
        m = guild.get_member(int(r["discord_id"])) if guild else None
        links.append({"email": r["mp_email"], "discord_id": str(r["discord_id"]), "username": m.display_name if m else ""})
    totals = await wp_link.push_links(links)
    await interaction.followup.send(
        f"✅ Sent **{len(links)}** link(s) to WordPress — stored: **{totals.get('ok', 0)}**, "
        f"no matching WP user: {totals.get('no-user', 0)}, invalid: {totals.get('invalid', 0)}, failed: {totals.get('failed', 0)}.",
        ephemeral=True,
    )


@bot.tree.command(name="preview-onboarding-dm", description="DM yourself one of the onboarding messages to check the copy")
@app_commands.default_permissions(administrator=True)
@app_commands.describe(kind="Which message", tier="Tier to render the welcome / day-7 copy for")
@app_commands.choices(
    kind=[
        app_commands.Choice(name="Welcome DM (after verifying)", value="welcome"),
        app_commands.Choice(name="Join DM (on joining the server)", value="join"),
        app_commands.Choice(name="Unverified nudge (day 1 / 3)", value="nudge"),
        app_commands.Choice(name="Day-7 check-in", value="day7"),
        app_commands.Choice(name="Day-30 check-in", value="day30"),
        app_commands.Choice(name="Discord tips only", value="tips"),
    ],
    tier=[
        app_commands.Choice(name="Gold", value="gold"),
        app_commands.Choice(name="Silver", value="silver"),
        app_commands.Choice(name="Insider", value="insider"),
    ],
)
async def preview_onboarding_dm(interaction: discord.Interaction, kind: app_commands.Choice[str], tier: app_commands.Choice[str] | None = None):
    """Sends the real message to the admin who ran it — bypasses dry-run, touches no member state."""
    await interaction.response.defer(ephemeral=True)
    t = tier.value if tier else "insider"
    k = kind.value
    if k == "welcome":
        content, embeds, view = _welcome_dm_parts(t)
    elif k == "join":
        content, embeds, view = _join_dm_text(), None, _join_dm_view()
    elif k == "nudge":
        content, embeds, view = _unverified_nudge_text(), None, _join_dm_view()
    elif k == "day7":
        content, embeds, view = _checkin_dm_text(7, t), None, None
    elif k == "day30":
        content, embeds, view = _checkin_dm_text(30, t), None, None
    else:
        content, embeds, view = "🔕 **Discord tips**", [_discord_tips_embed()], None
    ok = await _dm_member(interaction.user.id, content, view=view, embeds=embeds, what=f"preview {k}", ignore_dry_run=True)
    await interaction.followup.send(
        f"{'📬 Sent — check your DMs.' if ok else '❌ Could not DM you (DMs closed?).'} `{k}` / `{t}`",
        ephemeral=True,
    )


@bot.tree.command(name="pending-links", description="List paying MemberPress accounts not linked to Discord")
@app_commands.default_permissions(manage_roles=True)
async def pending_links(interaction: discord.Interaction):
    await interaction.response.defer(ephemeral=True)
    lines = await bot._check_unlinked_members()
    if not lines:
        await interaction.followup.send("✅ No unlinked paying members — everyone the bot has seen is verified.", ephemeral=True)
        return
    embed = discord.Embed(
        title="💸 Paying but not in Discord",
        description="\n".join(lines),
        color=discord.Color.orange(),
    )
    embed.set_footer(text="Seen via MemberPress webhooks · use /link-member to link manually")
    await interaction.followup.send(embed=embed, ephemeral=True)


@bot.tree.command(name="flag-history", description="Show recently flagged (and deleted) messages")
@app_commands.default_permissions(manage_messages=True)
@app_commands.describe(limit="How many entries to show (default 10)")
async def flag_history(interaction: discord.Interaction, limit: int = 10):
    flags = db.get_flagged_messages(limit=min(limit, 25))
    if not flags:
        await interaction.response.send_message("No flagged messages on record.", ephemeral=True)
        return
    embed = discord.Embed(title="🚩 Flagged Message History", color=discord.Color.red())
    # Group by author, worst offenders first
    by_author: dict[str, list[dict]] = {}
    for f in flags:
        by_author.setdefault(f["author_id"], []).append(f)
    for author_id, items in sorted(by_author.items(), key=lambda kv: len(kv[1]), reverse=True):
        total = db.count_flags_for_author(author_id)
        lines = []
        for f in items:
            content = (f["content"] or "")[:100] or "(no text)"
            reason = f" — {f['reason']}" if f["reason"] else ""
            lines.append(f"`#{f['id']}` {f['flagged_at']} · #{f['channel_name']}: {content}{reason}")
        embed.add_field(
            name=f"{items[0]['author_name']} — {total} flag{'s' if total != 1 else ''}",
            value="\n".join(lines)[:1024],
            inline=False,
        )
    await interaction.response.send_message(embed=embed, ephemeral=True)


@bot.tree.command(name="flag-stats", description="Running totals of flagged messages, by author")
@app_commands.default_permissions(manage_messages=True)
async def flag_stats(interaction: discord.Interaction):
    stats = db.get_flag_totals()
    embed = discord.Embed(
        title="🚩 Flagged Message Totals",
        description=f"**{stats['total']}** all-time · **{stats['last30']}** in the last 30 days",
        color=discord.Color.red(),
    )
    if stats["by_author"]:
        lines = [
            f"**{i}.** {a['author_name']} (<@{a['author_id']}>) — **{a['count']}**"
            for i, a in enumerate(stats["by_author"], 1)
        ]
        embed.add_field(name="By Author", value="\n".join(lines)[:1024], inline=False)
    await interaction.response.send_message(embed=embed, ephemeral=True)


@bot.tree.command(name="gameday-status", description="Show open game-week channels and what's scheduled next")
@app_commands.default_permissions(administrator=True)
async def gameday_status(interaction: discord.Interaction):
    today = dt.now(MOUNTAIN).date()
    embed = discord.Embed(title="🏈 Game-week channels", color=discord.Color.blue())
    state = "🟢 on" if GAMEDAY_CHANNELS else "🔴 off (GAMEDAY_CHANNELS)"
    if GAMEDAY_CHANNELS and GAMEDAY_DRY_RUN:
        state += " · 🧪 dry run"
    embed.add_field(name="Status", value=state, inline=False)

    open_rows = db.get_open_gameday_channels()
    embed.add_field(
        name="Open now",
        value="\n".join(
            f"<#{r['channel_id']}> — {r['opponent']} · deletes **{r['close_on']} 11:59pm MT**"
            for r in open_rows
        ) or "None.",
        inline=False,
    )

    upcoming = []
    for game in _load_json(SCHEDULE_PATH):
        try:
            game_date = datetime.date.fromisoformat(game["date"])
        except (KeyError, ValueError):
            continue
        if game_date < today:
            continue
        monday = _game_week_monday(game_date)
        when = "opens **today**" if monday == today else (
            f"opened {monday}" if monday < today else f"opens {monday}")
        upcoming.append(f"`{_gameday_channel_name(game)}` — {when}")
    embed.add_field(name="Next up", value="\n".join(upcoming[:6]) or "Season's over.", inline=False)
    await interaction.response.send_message(embed=embed, ephemeral=True)


@bot.tree.command(name="gameday-open", description="Open this week's game channel now (catch-up if Monday was missed)")
@app_commands.default_permissions(administrator=True)
@app_commands.describe(game_date="Game date as YYYY-MM-DD. Defaults to this week's game.")
async def gameday_open(interaction: discord.Interaction, game_date: str | None = None):
    await interaction.response.defer(ephemeral=True)
    today = dt.now(MOUNTAIN).date()
    if game_date:
        try:
            target = datetime.date.fromisoformat(game_date)
        except ValueError:
            await interaction.followup.send(f"`{game_date}` isn't a YYYY-MM-DD date.", ephemeral=True)
            return
        games = [g for g in _load_json(SCHEDULE_PATH) if g.get("date") == target.isoformat()]
    else:
        games = _games_for_week_of(_game_week_monday(today))

    if not games:
        await interaction.followup.send(
            "No game in `schedule.json` for that week. `/gameday-status` shows what's scheduled.",
            ephemeral=True)
        return

    opened = []
    for game in games:
        channel = await open_gameday_channel(game)
        opened.append(channel.mention if channel else f"`{_gameday_channel_name(game, games)}` — see the admin log")
    await interaction.followup.send("Opened: " + ", ".join(opened), ephemeral=True)


@bot.tree.command(name="gameday-close", description="Archive and delete an open game-week channel now")
@app_commands.default_permissions(administrator=True)
@app_commands.describe(channel="The game-week channel to archive and delete")
async def gameday_close(interaction: discord.Interaction, channel: discord.TextChannel):
    await interaction.response.defer(ephemeral=True)
    row = next((r for r in db.get_open_gameday_channels() if r["channel_id"] == str(channel.id)), None)
    if not row:
        await interaction.followup.send(
            f"{channel.mention} isn't a game-week channel this bot opened, so it won't be touched. "
            "Delete it by hand if that's what you want.", ephemeral=True)
        return
    ok = await close_gameday_channel(row)
    await interaction.followup.send(
        "Archived and deleted — transcript is in the admin log." if ok
        else "Not deleted. The admin log says why.", ephemeral=True)


@bot.tree.command(name="tier-history", description="Show recent tier changes")
@app_commands.default_permissions(administrator=True)
async def tier_history(interaction: discord.Interaction):
    changes = db.get_tier_changes(limit=20)
    if not changes:
        await interaction.response.send_message("No tier changes recorded yet.", ephemeral=True)
        return
    embed = discord.Embed(title="📋 Recent Tier Changes", color=discord.Color.blurple())
    lines = []
    for c in changes:
        lines.append(
            f"`{c['changed_at'][:16]}` **{c['mp_email']}** — "
            f"{tier_label(c['old_tier'])} → {tier_label(c['new_tier'])} *(_{c['reason']}_)*"
        )
    embed.description = "\n".join(lines)
    await interaction.response.send_message(embed=embed, ephemeral=True)


@bot.tree.command(name="stats", description="Membership stats breakdown")
@app_commands.default_permissions(administrator=True)
async def stats(interaction: discord.Interaction):
    s = db.get_stats()
    embed = discord.Embed(title="📊 CougConnect Member Stats", color=discord.Color.gold())
    embed.add_field(name="Total Verified", value=str(s["total"]), inline=False)
    embed.add_field(name="🥇 Gold", value=str(s["gold"]), inline=True)
    embed.add_field(name="🥈 Silver", value=str(s["silver"]), inline=True)
    embed.add_field(name="🔵 Insider", value=str(s["insider"]), inline=True)
    embed.add_field(name="❌ Unsubscribed", value=str(s["unsubscribed"]), inline=True)
    embed.set_footer(text=f"As of {dt.now(timezone.utc).strftime('%m/%d/%Y %H:%M')} UTC")
    await interaction.response.send_message(embed=embed, ephemeral=True)


@bot.tree.command(name="apartment-stats", description="How many members chose each BYU apartment complex")
@app_commands.default_permissions(administrator=True)
async def apartment_stats(interaction: discord.Interaction):
    await interaction.response.defer(ephemeral=True)
    members, failed = await mp.get_all_members_paged()

    tally = Counter()
    for m in members:
        slug = mp.get_apartment_slug(m)
        if slug and slug != "not-applicable":
            tally[slug] += 1

    guild = get_guild()
    # Combine complexes people actually chose with the ones we've configured,
    # so admins see both real demand and which configured roles are still empty.
    all_slugs = set(tally) | set(APARTMENTS)
    rows = []  # (count, has_role, label, line)
    for slug in all_slugs:
        count = tally.get(slug, 0)
        cfg = APARTMENTS.get(slug)
        if cfg:
            role = _resolve_apartment_role(guild, cfg) if guild else None
            marker = "" if role else " ⚠️ _role missing_"
            label = cfg.get("label", slug)
            rows.append((count, f"**{label}** — {count}{marker}"))
        else:
            # A complex members selected that has no Discord role/channel yet.
            rows.append((count, f"`{slug}` — {count}  🆕 _no role yet_"))
    rows.sort(key=lambda r: (-r[0], r[1].lower()))

    embed = discord.Embed(
        title="🏠 Apartment Complex Breakdown",
        description="\n".join(r[1] for r in rows) or "No members have selected an apartment yet.",
        color=discord.Color.blue(),
    )
    embed.add_field(name="Members scanned", value=str(len(members)), inline=True)
    embed.add_field(name="With a complex set", value=str(sum(tally.values())), inline=True)
    footer = f"As of {dt.now(timezone.utc).strftime('%m/%d/%Y %H:%M')} UTC"
    if failed:
        footer += f" · ⚠️ {failed} page(s) failed to load — counts may be low"
    embed.set_footer(text=footer)
    await interaction.followup.send(embed=embed, ephemeral=True)


@bot.tree.command(name="churn", description="Churn analysis — cancellations vs new members over recent months")
@app_commands.default_permissions(administrator=True)
async def churn(interaction: discord.Interaction):
    data = db.get_churn_data(months=6)
    if not data["monthly"]:
        await interaction.response.send_message("No tier-change data recorded yet.", ephemeral=True)
        return

    embed = discord.Embed(title="📉 Churn Report — Last 6 Months", color=discord.Color.red())

    month_lines = []
    for m in data["monthly"]:
        net = m["new_links"] - m["cancels"]
        month_lines.append(f"`{m['month']}` — 🔗 {m['new_links']} new, ❌ {m['cancels']} cancelled, net **{'+' if net >= 0 else ''}{net}**")
    embed.add_field(name="By Month", value="\n".join(month_lines), inline=False)

    if data["cancels_by_tier"]:
        tier_lines = [f"**{tier_label(t)}**: {n}" for t, n in sorted(data["cancels_by_tier"].items())]
        embed.add_field(name="Cancellations by Tier", value="  |  ".join(tier_lines), inline=False)

    if data["avg_days_before_cancel"] is not None:
        months_avg = data["avg_days_before_cancel"] / 30.4
        embed.add_field(
            name="Avg Membership Length Before Cancelling",
            value=f"**{data['avg_days_before_cancel']:.0f} days** (~{months_avg:.1f} months, n={data['churn_sample_size']})",
            inline=False,
        )

    embed.set_footer(text="Source: tier_changes audit log · new = first verification, cancelled = downgrade to Unsubscribed")
    await interaction.response.send_message(embed=embed, ephemeral=True)


# ── aiohttp web server ─────────────────────────────────────────────────────────

def _page(title: str, body: str) -> web.Response:
    """Render a simple branded HTML page."""
    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>{title} — CougConnect</title>
  <style>
    * {{ box-sizing: border-box; margin: 0; padding: 0; }}
    body {{
      background: #0f1117;
      color: #fff;
      font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif;
      display: flex;
      align-items: center;
      justify-content: center;
      min-height: 100vh;
      padding: 24px;
    }}
    .card {{
      background: #1a1d27;
      border: 1px solid #2a2d3a;
      border-radius: 16px;
      padding: 48px 40px;
      max-width: 480px;
      width: 100%;
      text-align: center;
    }}
    .logo {{ font-size: 14px; color: #6b7280; letter-spacing: 2px; text-transform: uppercase; margin-bottom: 32px; }}
    h1 {{ font-size: 24px; font-weight: 700; margin-bottom: 12px; }}
    p {{ color: #9ca3af; font-size: 15px; line-height: 1.6; margin-bottom: 24px; }}
    input[type=email] {{
      width: 100%;
      padding: 12px 16px;
      border-radius: 8px;
      border: 1px solid #374151;
      background: #111827;
      color: #fff;
      font-size: 15px;
      margin-bottom: 16px;
      outline: none;
    }}
    input[type=email]:focus {{ border-color: #3b82f6; }}
    button {{
      width: 100%;
      padding: 13px;
      background: #2563eb;
      color: #fff;
      border: none;
      border-radius: 8px;
      font-size: 16px;
      font-weight: 600;
      cursor: pointer;
    }}
    button:hover {{ background: #1d4ed8; }}
    .back {{ display: inline-block; margin-top: 24px; color: #6b7280; font-size: 13px; text-decoration: none; }}
    .back:hover {{ color: #9ca3af; }}
  </style>
</head>
<body>
  <div class="card">
    <div class="logo">CougConnect</div>
    {body}
    <a href="https://cougconnect.com" class="back">← Back to CougConnect</a>
  </div>
</body>
</html>"""
    return web.Response(text=html, content_type="text/html")


LINK_STYLE = 'style="color:#3b82f6;"'
NOTICE_STYLE = ('style="background:#3b1d1d;border:1px solid #7f1d1d;border-radius:8px;padding:12px 14px;'
                'color:#fecaca;font-size:14px;line-height:1.5;margin-bottom:16px;text-align:left"')
BTN_PRIMARY = ('style="display:block;padding:12px;margin:8px 0;background:#2563eb;color:#fff;border-radius:8px;'
               'text-decoration:none;font-weight:600"')
BTN_SECONDARY = ('style="display:block;padding:12px;margin:8px 0;background:#1f2937;color:#fff;border-radius:8px;'
                 'text-decoration:none;font-weight:600"')


def _verify_form(token: str, discord_id: str, notice: str = "", email: str = "") -> str:
    return f"""
        <h1>🔐 Verify Membership</h1>
        {notice}
        <p>Enter the email you used when you subscribed on cougconnect.com — that's the only one that will match.</p>
        <form method="POST" action="/verify-page">
          <input type="hidden" name="token" value="{html.escape(token, quote=True)}">
          <input type="hidden" name="discord_id" value="{html.escape(discord_id, quote=True)}">
          <input type="email" name="email" placeholder="your@email.com" value="{html.escape(email, quote=True)}" required autofocus>
          <button type="submit">Verify My Membership</button>
        </form>
    """


def _verify_failure_page(token: str, discord_id: str, email: str, reason: str, title: str, message_html: str) -> web.Response:
    """Re-render the form with the error so a wrong email can be corrected on the same link.

    Records the attempt; a second failure inside 24h adds the support-ticket
    escape hatch and pings the admin log, so nobody bounces off silently.
    """
    db.record_verify_failure(discord_id, email, reason)
    failures = db.count_verify_failures(discord_id, hours=24)
    extra = ""
    if failures >= 2:
        extra = (f'<p style="font-size:13px">Still stuck? <a href="{ob_channel_link("support")}" {LINK_STYLE}>Open a '
                 f'support ticket</a> and we\'ll link you by hand — usually same day.</p>')
        asyncio.create_task(post_admin_log(
            f"⚠️ Verify failed {failures}× in 24h — <@{discord_id}> tried `{email}` ({reason})"
        ))
    notice = f"<div {NOTICE_STYLE}>{message_html}</div>{extra}"
    return _page(title, _verify_form(token, discord_id, notice=notice, email=email))


async def handle_verify_page_get(request: web.Request) -> web.Response:
    """Serve the email entry form."""
    token = request.rel_url.query.get("token", "")
    discord_id = request.rel_url.query.get("discord_id", "")

    if not token or not discord_id:
        return _page("Error", """
            <h1>❌ Invalid Link</h1>
            <p>This verification link is invalid. Please click the button in Discord again.</p>
        """)

    return _page("Verify Membership", _verify_form(token, discord_id))


async def handle_verify_page_post(request: web.Request) -> web.Response:
    """Process the submitted email, look up MemberPress, assign role."""
    try:
        data = await request.post()
    except Exception:
        return _page("Error", "<h1>❌ Bad Request</h1><p>Something went wrong. Please try again.</p>")

    token = data.get("token", "")
    discord_id = data.get("discord_id", "")
    email = data.get("email", "").strip().lower()
    safe_email = html.escape(email)

    if not all([token, discord_id, email]):
        return _page("Error", "<h1>❌ Missing Info</h1><p>Please go back and fill in your email address.</p>")

    # Validate the token without burning it — it's only consumed on success, so a
    # mistyped email can be retried on the same link inside the 15 minutes.
    stored_discord_id = db.peek_token(token)
    if not stored_discord_id:
        return _page("Link Expired", """
            <h1>⏰ Link Expired</h1>
            <p>This verification link has expired or already been used.</p>
            <p>Click the <strong>Verify Membership</strong> button in Discord to get a new link.</p>
        """)
    if stored_discord_id != discord_id:
        return _page("Error", "<h1>❌ Invalid Link</h1><p>This link is not valid for your account.</p>")

    # Email already linked to a different Discord account
    existing_link = db.get_member_by_email(email)
    if existing_link and existing_link["discord_id"] != discord_id:
        return _verify_failure_page(token, discord_id, email, "already-linked", "Already Linked",
            f"<strong>{safe_email}</strong> is already connected to a different Discord account. "
            f"If that's you on another account, <a href=\"{ob_channel_link('support')}\" {LINK_STYLE}>open a support ticket</a> "
            "and we'll move it. Otherwise try a different email below.")

    # Look up member in MemberPress
    mp_member = await mp.get_member_by_email(email)
    if not mp_member:
        return _verify_failure_page(token, discord_id, email, "not-found", "Not Found",
            f"No CougConnect account was found for <strong>{safe_email}</strong>. Double-check the email you used at "
            f"checkout — it's on your welcome email and your <a href=\"{ob_url('account')}\" {LINK_STYLE}>account page</a>. "
            f"Not a member yet? <a href=\"{ob_url('subscribe')}\" {LINK_STYLE}>Subscribe here</a>.")

    mp_id = mp_member.get("id")
    active_ids = mp.active_ids_from_member_object(mp_member)
    if not active_ids:
        active_ids = await mp.get_active_membership_ids(mp_id)
    tier = mp.resolve_tier(active_ids)

    if tier == "unsubscribed":
        return _verify_failure_page(token, discord_id, email, "no-active-sub", "No Active Subscription",
            f"The account for <strong>{safe_email}</strong> doesn't have an active CougConnect membership. "
            "Just paid? Give it two minutes and try again. Otherwise "
            f"<a href=\"{ob_url('subscribe')}\" {LINK_STYLE}>subscribe here</a> or check your "
            f"<a href=\"{ob_url('account')}\" {LINK_STYLE}>account page</a>.")

    db.consume_token(token)
    db.upsert_member(discord_id, mp_id, email, tier)
    db.mark_unlinked_verified(mp_id)
    success = await assign_role(int(discord_id), tier)

    if not success:
        log.error(f"Role assignment failed for discord_id={discord_id}")
        return _page("Error", f"""
            <h1>⚠️ Role Assignment Failed</h1>
            <p>We verified your membership but couldn't assign your Discord role.
               Please <a href="{ob_channel_link('support')}" {LINK_STYLE}>open a support ticket</a> and we'll fix it by hand.</p>
        """)

    apartment_slug = mp.get_apartment_slug(mp_member)
    await assign_apartment_role(int(discord_id), apartment_slug)

    log.info(f"Verified discord_id={discord_id} email={email} tier={tier} apartment={apartment_slug}")
    asyncio.create_task(send_welcome_dm(int(discord_id), tier, apartment_slug, email))
    asyncio.create_task(wp_link.push_link(email, discord_id, _display_name(int(discord_id))))
    return _success_page(tier)


# ── Account-page "Connect Discord" (OAuth) ────────────────────────────────────

def _display_name(discord_id: int) -> str:
    guild = get_guild()
    member = guild.get_member(discord_id) if guild else None
    if member:
        return member.display_name or member.name
    user = bot.get_user(discord_id)
    return user.name if user else ""


def _success_page(tier: str, in_server: bool = True) -> web.Response:
    """Shared 'you're verified' page for the verify form and the OAuth connect."""
    tier_display = tier_label(tier)
    home_key = {"gold": "gold_lounge", "silver": "silver", "insider": "insider_info"}.get(tier, "general")
    if in_server:
        note = "We also sent you a Discord DM with a channel guide."
        first = f'<a href="{ob_channel_link(home_key)}" {BTN_PRIMARY}>Open your channels</a>'
    else:
        note = "One more step: join the server with the invite below — your role applies the moment you're in."
        first = f'<a href="{ob_url("invite")}" {BTN_PRIMARY}>Join the Discord</a>'
    return _page("Verified!", f"""
        <h1>✅ You're Verified!</h1>
        <p>Your <strong>{tier_display}</strong> role is on. {note}</p>
        {first}
        <a href="{ping_roles_link()}" {BTN_SECONDARY}>Pick your pings (Channels &amp; Roles)</a>
        <a href="{ob_channel_link('rules')}" {BTN_SECONDARY}>Read the rules</a>
        <a href="{ob_url('youtube')}" {BTN_SECONDARY}>▶ Subscribe on YouTube — daily podcast</a>
    """)


def _parse_connect_token(token: str) -> dict | None:
    """Validate a site-minted Connect token: base64url(JSON) '.' hex(HMAC-SHA256 with BOT_VERIFY_SECRET).

    The email inside a valid token is the ONLY identity we trust — never a query
    parameter — so a URL can't be edited to link someone else's membership.
    """
    if not BOT_VERIFY_SECRET or "." not in token:
        return None
    encoded, sig = token.rsplit(".", 1)
    expected = hmac.new(BOT_VERIFY_SECRET.encode(), encoded.encode(), hashlib.sha256).hexdigest()
    if not hmac.compare_digest(expected, sig):
        return None
    try:
        payload = json.loads(base64.urlsafe_b64decode(encoded + "=" * (-len(encoded) % 4)))
    except Exception:
        return None
    if not isinstance(payload, dict) or int(payload.get("exp") or 0) < int(dt.now(timezone.utc).timestamp()):
        return None
    email = str(payload.get("email") or "").strip().lower()
    if "@" not in email:
        return None
    return {"email": email, "uid": int(payload.get("uid") or 0)}


def _connect_expired_page() -> web.Response:
    return _page("Link expired", f"""
        <h1>⏰ This link has expired</h1>
        <p>Connect links last 15 minutes. Go back to your account page and click <strong>Connect Discord</strong> again.</p>
        <a href="{ob_url('account')}" {BTN_PRIMARY}>Back to my account</a>
    """)


def _connect_problem_page(message_html: str) -> web.Response:
    return _page("Not connected", f"""
        <h1>⚠️ We couldn't finish that</h1>
        <p>{message_html}</p>
        <a href="{ob_channel_link('support')}" {BTN_PRIMARY}>Open a support ticket</a>
        <a href="{ob_url('account')}" {BTN_SECONDARY}>Back to my account</a>
    """)


async def _guild_join(discord_id: str, access_token: str) -> bool:
    """Add the member to the server via guilds.join. True if they end up in it.

    Needs "Create Invite" on the bot's role; without it Discord answers 403 and
    the success page falls back to the invite link.
    """
    guild = get_guild()
    if guild and guild.get_member(int(discord_id)):
        return True
    try:
        async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=20)) as session:
            async with session.put(
                f"https://discord.com/api/v10/guilds/{GUILD_ID}/members/{discord_id}",
                json={"access_token": access_token},
                headers={"Authorization": f"Bot {TOKEN}"},
            ) as resp:
                if resp.status in (201, 204):
                    return True
                log.warning(f"Connect: guilds.join returned {resp.status}: {(await resp.text())[:200]}")
    except Exception as e:
        log.warning(f"Connect: guilds.join failed: {e}")
    return False


async def handle_connect(request: web.Request) -> web.Response:
    """Site → bot hand-off. Validates the signed token, parks the email behind a
    one-time state, and sends the member to Discord's login."""
    if not DISCORD_CLIENT_SECRET or not BOT_VERIFY_SECRET or not BOT_PUBLIC_URL.startswith("http"):
        return _page("Connect", f"""
            <h1>Connect isn't switched on yet</h1>
            <p>Use the invite and the <strong>Verify Membership</strong> button in #verification instead.</p>
            <a href="{ob_url('invite')}" {BTN_PRIMARY}>Join the Discord</a>
        """)
    payload = _parse_connect_token(request.rel_url.query.get("t", ""))
    if not payload:
        return _connect_expired_page()
    state = secrets.token_urlsafe(24)
    db.create_connect_state(state, payload["email"], payload["uid"])
    params = {
        "client_id": DISCORD_CLIENT_ID,
        "response_type": "code",
        "redirect_uri": BOT_PUBLIC_URL + CONNECT_REDIRECT_PATH,
        "scope": "identify guilds.join",
        "state": state,
        "prompt": "consent",
    }
    raise web.HTTPFound("https://discord.com/oauth2/authorize?" + urlencode(params))


async def handle_connect_callback(request: web.Request) -> web.Response:
    """Discord → bot: exchange the code, confirm the membership, link, join, role, DM."""
    q = request.rel_url.query
    ctx = db.consume_connect_state(q.get("state", ""))
    if not ctx:
        return _connect_expired_page()
    if q.get("error") or not q.get("code"):
        return _page("Not connected", f"""
            <h1>No problem</h1>
            <p>You cancelled the Discord login, so nothing was linked. Whenever you're ready, click
               <strong>Connect Discord</strong> on your account page again.</p>
            <a href="{ob_url('account')}" {BTN_PRIMARY}>Back to my account</a>
        """)
    email = ctx["email"]

    # 1) Authorization code → access token → who is this?
    try:
        async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=20)) as session:
            async with session.post(
                "https://discord.com/api/oauth2/token",
                data={
                    "client_id": DISCORD_CLIENT_ID,
                    "client_secret": DISCORD_CLIENT_SECRET,
                    "grant_type": "authorization_code",
                    "code": q["code"],
                    "redirect_uri": BOT_PUBLIC_URL + CONNECT_REDIRECT_PATH,
                },
                headers={"Content-Type": "application/x-www-form-urlencoded"},
            ) as resp:
                tok = await resp.json(content_type=None)
                access = tok.get("access_token") if resp.status == 200 and isinstance(tok, dict) else None
            if not access:
                log.warning(f"Connect: token exchange failed for {email}: {resp.status} {str(tok)[:200]}")
                return _connect_problem_page("Discord didn't complete the login. Try <strong>Connect Discord</strong> again from your account page.")
            async with session.get("https://discord.com/api/users/@me", headers={"Authorization": f"Bearer {access}"}) as resp:
                me = await resp.json(content_type=None)
    except Exception as e:
        log.error(f"Connect: Discord API error for {email}: {e}")
        return _connect_problem_page("Discord didn't answer. Give it a minute and try again from your account page.")

    discord_id = str((me or {}).get("id") or "")
    username = (me or {}).get("global_name") or (me or {}).get("username") or ""
    if not discord_id:
        return _connect_problem_page("Discord didn't tell us who you are. Try again from your account page.")

    # 2) The membership behind the token's email.
    mp_member = await mp.get_member_by_email(email)
    if not mp_member:
        return _connect_problem_page(f"We couldn't find a CougConnect account for <strong>{html.escape(email)}</strong>.")
    mp_id = mp_member.get("id")
    active_ids = mp.active_ids_from_member_object(mp_member) or await mp.get_active_membership_ids(mp_id)
    tier = mp.resolve_tier(active_ids)
    if tier == "unsubscribed":
        return _page("No Active Subscription", f"""
            <h1>⚠️ No active membership</h1>
            <p>The account for <strong>{html.escape(email)}</strong> doesn't have an active CougConnect membership.
               Just paid? Give it two minutes and try again.</p>
            <a href="{ob_url('subscribe')}" {BTN_PRIMARY}>See memberships</a>
            <a href="{ob_url('account')}" {BTN_SECONDARY}>Back to my account</a>
        """)

    # 3) Conflicts — the same guards the verify form applies.
    by_email = db.get_member_by_email(email)
    if by_email and by_email["discord_id"] != discord_id:
        db.record_verify_failure(discord_id, email, "already-linked")
        return _connect_problem_page(
            f"<strong>{html.escape(email)}</strong> is already linked to a different Discord account. "
            "If that's you on another account, open a support ticket and we'll move it."
        )
    by_discord = db.get_member_by_discord(discord_id)
    if by_discord and (by_discord.get("mp_email") or "").lower() != email:
        db.record_verify_failure(discord_id, email, "discord-already-linked")
        return _connect_problem_page(
            "This Discord account is already verified for a different CougConnect account. "
            "Open a support ticket and we'll sort out which one should be linked."
        )

    # 4) Into the server (best effort), then link, role, guide.
    in_server = await _guild_join(discord_id, access)
    old_tier = by_email["tier"] if by_email else "none"
    if old_tier != tier:
        db.log_tier_change(discord_id, email, old_tier, tier, reason="connect-oauth")
    db.upsert_member(discord_id, mp_id, email, tier)
    db.mark_unlinked_verified(mp_id)

    role_ok = False
    if in_server:
        role_ok = await assign_role(int(discord_id), tier)
        if not role_ok:
            await asyncio.sleep(2)  # freshly joined members can lag the cache
            role_ok = await assign_role(int(discord_id), tier)
        if role_ok:
            await assign_apartment_role(int(discord_id), mp.get_apartment_slug(mp_member))
            asyncio.create_task(send_welcome_dm(int(discord_id), tier, mp.get_apartment_slug(mp_member), email))
    # Not in the server yet: on_member_join restores the role and sends the guide when they arrive.
    asyncio.create_task(wp_link.push_link(email, discord_id, username or _display_name(int(discord_id))))
    if email and not (in_server and role_ok):  # send_welcome_dm tags the rest
        asyncio.create_task(mailchimp.tag_verified(email))
    log.info(f"Connect: linked discord_id={discord_id} email={email} tier={tier} in_server={in_server} role={role_ok}")
    return _success_page(tier, in_server=in_server and role_ok)


INACTIVE_EVENTS = {
    "subscription-expired",
    "member-account-expired",
    # NOTE: subscription-stopped and subscription-cancelled are NOT here.
    # Those fire when auto-renewal is cancelled but access continues until the
    # paid period ends. We re-fetch from MemberPress to get the real current tier
    # rather than immediately demoting. The member will be downgraded when
    # subscription-expired fires at the end of their paid period.
}
REACTIVATE_EVENTS = {
    "subscription-resumed", "subscription-renewed", "subscription-upgraded",
    "subscription-created", "transaction-completed",
    "member-signup-completed", "subscription-paused",
    "subscription-stopped", "subscription-cancelled",
}

# Signup / tier-change events that (re)sync the member into the Mailchimp audience.
MAILCHIMP_SYNC_EVENTS = {
    "member-signup-completed", "subscription-created",
    "subscription-upgraded", "subscription-downgraded",
}

# MemberPress fires the same event in bursts; drop repeats seen within this window
WEBHOOK_DEDUPE_SECONDS = 30
_recent_webhooks: dict[tuple[int, str], float] = {}

# Reactivation lookups retry on this schedule before giving up
REACTIVATE_RETRY_DELAYS = [15, 60, 300]


async def process_webhook_event(event: str, mp_member_id: int, record: dict):
    """Apply a MemberPress webhook in the background (handler already returned 200)."""
    discord_id = int(record["discord_id"])

    if event in INACTIVE_EVENTS:
        # "Expired" does not always mean "no longer paying": a tier upgrade
        # creates the new subscription and then expires the OLD one, so this
        # event fires minutes after an upgrade for a member who is actively
        # paying. Verify against MemberPress before demoting.
        current_tier, member_obj = await mp.resolve_tier_or_none(mp_member_id, record["mp_email"])
        if current_tier is None:
            log.warning(f"Webhook {event} for discord_id={discord_id}: MemberPress unreachable — leaving role unchanged")
            await post_admin_log(
                f"⚠️ **Expiry webhook unverifiable** — <@{discord_id}> (`{record['mp_email']}`)\n"
                f"Event `{event}` fired but MemberPress could not be reached to confirm. "
                f"Role left unchanged; use `/sync-member` to re-check."
            )
            return
        if current_tier != "unsubscribed":
            # Still holds an active membership (e.g. the expired sub was
            # replaced by an upgrade). Record the real tier instead of demoting.
            db.log_tier_change(record["discord_id"], record["mp_email"], record["tier"], current_tier, reason=f"webhook:{event}:still-active")
            db.upsert_member(record["discord_id"], mp_member_id, record["mp_email"], current_tier)
            await assign_role(discord_id, current_tier)
            await assign_apartment_role(discord_id, mp.get_apartment_slug(member_obj) if member_obj else None)
            log.info(f"Webhook {event} for discord_id={discord_id}: still {current_tier} in MemberPress — not demoting")
            return
        db.log_tier_change(record["discord_id"], record["mp_email"], record["tier"], "unsubscribed", reason=f"webhook:{event}")
        db.upsert_member(record["discord_id"], mp_member_id, record["mp_email"], "unsubscribed")
        await assign_role(discord_id, "unsubscribed")
        await assign_apartment_role(discord_id, None)
        log.info(f"Set discord_id={discord_id} to unsubscribed via event={event}")
        try:
            user = await bot.fetch_user(discord_id)
            await user.send(
                "Your CougConnect membership has expired or been cancelled. "
                "Renew at https://cougconnect.com to restore your access. 🏈"
            )
        except Exception:
            pass

    elif event in REACTIVATE_EVENTS:
        # Webhooks fire before MemberPress commits the subscription, so retry
        # with backoff until the API reflects an active membership.
        tier = "unsubscribed"
        member_obj = None
        for attempt, delay in enumerate(REACTIVATE_RETRY_DELAYS, 1):
            await asyncio.sleep(delay)
            member_obj, active_ids = await mp.get_member_and_active_ids(mp_member_id, record["mp_email"])
            tier = mp.resolve_tier(active_ids)
            if tier != "unsubscribed":
                break
            log.info(f"Webhook {event} for discord_id={discord_id}: still unsubscribed after attempt {attempt}/{len(REACTIVATE_RETRY_DELAYS)}")

        if tier == "unsubscribed":
            log.warning(f"Webhook {event} for discord_id={discord_id} resolved to unsubscribed after all retries.")
            await post_admin_log(
                f"⚠️ **Webhook race condition** — <@{discord_id}> (`{record['mp_email']}`)\n"
                f"Event `{event}` fired but MemberPress still shows no active membership after "
                f"{len(REACTIVATE_RETRY_DELAYS)} retries over ~6 minutes.\n"
                f"Use `/sync-member` to retry manually."
            )
            return
        db.log_tier_change(record["discord_id"], record["mp_email"], record["tier"], tier, reason=f"webhook:{event}")
        db.upsert_member(record["discord_id"], mp_member_id, record["mp_email"], tier)
        await assign_role(discord_id, tier)
        apartment_slug = mp.get_apartment_slug(member_obj) if member_obj else None
        await assign_apartment_role(discord_id, apartment_slug)
        log.info(f"Re-activated discord_id={discord_id} as tier={tier} apartment={apartment_slug} via event={event}")


async def handle_webhook(request: web.Request) -> web.Response:
    """
    MemberPress subscription webhook.
    Fired on: subscription-expired, subscription-cancelled, subscription-stopped,
              subscription-resumed, subscription-upgraded, etc.

    Validates and returns 200 immediately; the actual processing (which may wait
    minutes for MemberPress to commit) runs as a background task.
    """
    body = await request.read()

    # Validate HMAC if secret is configured
    if MP_WEBHOOK_SECRET:
        sig = request.headers.get("X-Memberpress-Signature", "")
        expected = hmac.new(
            MP_WEBHOOK_SECRET.encode(),
            body,
            hashlib.sha256,
        ).hexdigest()
        if not hmac.compare_digest(sig, expected):
            return web.json_response({"error": "Invalid signature"}, status=403)

    try:
        data = json.loads(body)
    except Exception:
        return web.json_response({"error": "Invalid JSON"}, status=400)

    event = data.get("event", "")
    payload = data.get("data") if isinstance(data.get("data"), dict) else {}
    member_data = payload.get("member") or data.get("member") or {}
    if not member_data and data.get("type") == "member":
        member_data = payload  # member-* events carry the member object itself
    mp_member_id = member_data.get("id") or payload.get("member_id")

    if not mp_member_id:
        return web.json_response({"status": "ignored — no member_id"})

    mp_member_id = int(mp_member_id)
    if event in MAILCHIMP_SYNC_EVENTS:
        asyncio.create_task(_mailchimp_sync_member(mp_member_id, event))
    record = db.get_member_by_mp_id(mp_member_id)
    if not record:
        db.record_unlinked(mp_member_id, member_data.get("email"), member_data.get("registered_at"))
        log.info(f"Webhook for unlinked mp_member_id={mp_member_id} — recorded for daily report.")
        return web.json_response({"status": "ignored — member not linked"})

    now = asyncio.get_event_loop().time()
    key = (mp_member_id, event)
    if now - _recent_webhooks.get(key, 0) < WEBHOOK_DEDUPE_SECONDS:
        return web.json_response({"status": "ignored — duplicate"})
    _recent_webhooks[key] = now
    for k in [k for k, t in _recent_webhooks.items() if now - t > WEBHOOK_DEDUPE_SECONDS]:
        del _recent_webhooks[k]

    asyncio.create_task(process_webhook_event(event, mp_member_id, record))
    return web.json_response({"status": "accepted"})


async def handle_legacy_webhook(request: web.Request) -> web.Response:
    """Bare /webhook — kept during the move to the tokened URL, then disabled."""
    if DISABLE_LEGACY_WEBHOOK:
        return web.json_response({"error": "Gone — webhook URL has changed"}, status=410)
    if WEBHOOK_URL_TOKEN:
        log.warning("Webhook received on legacy /webhook path — update the MemberPress webhook URL to the tokened path.")
    return await handle_webhook(request)


async def start_web_server():
    app = web.Application(client_max_size=50*1024*1024)
    app.router.add_get("/verify-page", handle_verify_page_get)
    app.router.add_post("/verify-page", handle_verify_page_post)
    app.router.add_post("/webhook", handle_legacy_webhook)
    app.router.add_get("/news.json", aggregator.handle_news_json)
    app.router.add_get("/connect", handle_connect)
    app.router.add_get(CONNECT_REDIRECT_PATH, handle_connect_callback)
    if WEBHOOK_URL_TOKEN:
        app.router.add_post(f"/webhook/{WEBHOOK_URL_TOKEN}", handle_webhook)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", PORT)
    await site.start()
    log.info(f"Web server listening on port {PORT}")


# ── Entry point ────────────────────────────────────────────────────────────────

async def main():
    async with bot:
        await start_web_server()
        await bot.start(TOKEN)


if __name__ == "__main__":
    asyncio.run(main())
