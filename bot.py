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
import hashlib
import hmac
import html
import json
import logging
import os
import sqlite3
import sys
import datetime
from datetime import datetime as dt, timezone

import aiohttp
import aiohttp.web as web
import discord
from discord import app_commands
from discord.ext import commands, tasks
from dotenv import load_dotenv

import database as db
import memberpress as mp

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

# Mods/admins react with this emoji to flag a message: it's logged to the mod
# log channel + DB, then deleted. Only members with Manage Messages can trigger it.
FLAG_EMOJI = os.getenv("FLAG_EMOJI", "🚩")
MOD_LOG_CHANNEL_ID = int(os.getenv("DISCORD_MOD_LOG_CHANNEL_ID", "1189800941793333389"))  # #admin-moderators

FAQ_PATH = os.path.join(os.path.dirname(__file__), "faq.json")
SCHEDULE_PATH = os.path.join(os.path.dirname(__file__), "schedule.json")
SPONSORS_PATH = os.path.join(os.path.dirname(__file__), "sponsors.json")

UPGRADE_NUDGE_DAYS = 152  # ~5 months as a member before the Insider upgrade nudge
UPGRADE_NUDGE_DAILY_CAP = 50  # spread large cohorts over multiple days
WINBACK_DAYS = 30


def _load_json(path: str) -> list[dict]:
    try:
        with open(path) as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return []


def load_faq() -> list[dict]:
    return _load_json(FAQ_PATH)


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
        self.gameday_thread_task.start()

    async def on_ready(self):
        log.info(f"Logged in as {self.user} (ID: {self.user.id})")
        await self._post_verify_embed()
        await self._post_unsubscribed_embed()
        await post_admin_log("✅ CougConnect bot online and ready.")

    async def _post_verify_embed(self):
        channel = self.get_channel(VERIFY_CHANNEL_ID)
        if not channel:
            return
        # Delete any old bot verify embeds and repost fresh
        async for msg in channel.history(limit=20):
            if msg.author == self.user and msg.components:
                await msg.delete()
                break
        embed = discord.Embed(
            title="🔐 Verify Your CougConnect Membership",
            description=(
                "Click the button below to link your CougConnect subscription to Discord.\n\n"
                "Your role will be assigned automatically based on your membership tier."
            ),
            color=discord.Color.blue(),
        )
        embed.set_footer(text="CougConnect — Insider BYU Athletics Coverage")
        await channel.send(embed=embed, view=VerifyView())

    async def _post_unsubscribed_embed(self):
        channel = self.get_channel(UNSUBSCRIBED_CHANNEL_ID)
        if not channel:
            return
        try:
            async for msg in channel.history(limit=20):
                if msg.author == self.user and msg.components:
                    await msg.delete()
                    break
        except discord.Forbidden:
            log.warning(f"No Read Message History permission in unsubscribed channel {UNSUBSCRIBED_CHANNEL_ID} — skipping cleanup")
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
        await channel.send(embed=embed, view=ReSyncView())

    @tasks.loop(hours=1)
    async def cleanup_tokens_task(self):
        db.cleanup_expired_tokens()

    @tasks.loop(time=datetime.time(hour=10, minute=0, tzinfo=datetime.timezone.utc))  # 3am MST (UTC-7)
    async def sync_all_members_task(self):
        members = db.get_all_members()
        if not members:
            return
        log.info(f"Starting periodic sync for {len(members)} linked members...")
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

        await post_admin_log("\n".join(lines))

        # Unverified subscribers go to email only, not Discord.
        unlinked_lines = await self._check_unlinked_members()
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
                db.remove_unlinked(mp_id)  # linked since we recorded them
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
        """Celebrate membership anniversaries in the general channel."""
        channel = self.get_channel(GENERAL_CHANNEL_ID)
        if not channel:
            return
        today = dt.now(timezone.utc).date()
        for record in db.get_all_members():
            if record["tier"] not in ("gold", "silver", "insider") or not record["linked_at"]:
                continue
            try:
                linked = dt.fromisoformat(record["linked_at"]).date()
            except ValueError:
                continue
            if (linked.month, linked.day) != (today.month, today.day):
                continue
            years = today.year - linked.year
            if years < 1 or db.notice_sent("milestone_notices", record["discord_id"], years):
                continue
            label = "1 year" if years == 1 else f"{years} years"
            try:
                await channel.send(
                    f"🎉 Shoutout to <@{record['discord_id']}> — **{label}** as a CougConnect "
                    f"**{tier_label(record['tier'])}** member today! Thanks for backing the Cougs with us. 🏈"
                )
                db.record_notice("milestone_notices", record["discord_id"], years)
            except Exception as e:
                log.error(f"Milestone post failed for discord_id={record['discord_id']}: {e}")
            await asyncio.sleep(2)

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
        embed = discord.Embed(
            title=f"🤝 Sponsor Spotlight: {sponsor['name']}",
            description=sponsor.get("blurb", ""),
            color=discord.Color.blue(),
        )
        embed.set_footer(text="CougConnect sponsors keep this community running — show them some love!")
        # Tag the sponsor in the message content (embed mentions don't ping)
        content = None
        if sponsor.get("email"):
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
        if prev:
            lines.append(f"_Compared to {prev['snapshot_date']}_")
        await post_admin_log("\n".join(lines))

    @weekly_digest_task.before_loop
    async def before_weekly_digest(self):
        await self.wait_until_ready()

    @tasks.loop(time=datetime.time(hour=14, minute=30, tzinfo=datetime.timezone.utc))  # ~8:30am MT
    async def gameday_thread_task(self):
        """Open a game-day thread in the general channel on BYU game days."""
        today = dt.now(timezone.utc).astimezone().strftime("%Y-%m-%d")
        games = [g for g in _load_json(SCHEDULE_PATH) if g["date"] == today]
        if not games:
            return
        channel = self.get_channel(GENERAL_CHANNEL_ID)
        if not channel:
            return
        for game in games:
            vs_at = "vs" if game.get("home") else "at"
            emoji = "🏈" if game.get("sport") == "football" else "🏀"
            name = f"{emoji} BYU {vs_at} {game['opponent']} — Game Thread"
            try:
                thread = await channel.create_thread(
                    name=name,
                    type=discord.ChannelType.public_thread,
                    auto_archive_duration=1440,
                )
                details = []
                if game.get("time"):
                    details.append(f"🕐 Kickoff: **{game['time']}**")
                if game.get("tv"):
                    details.append(f"📺 Watch on **{game['tv']}**")
                await thread.send(
                    f"**It's game day, Cougar Nation!** BYU {vs_at} **{game['opponent']}** 🎉\n"
                    + ("\n".join(details) + "\n" if details else "")
                    + "\nDrop your predictions and talk the game right here. Go Cougs!"
                )
                log.info(f"Game-day thread created: {name}")
            except Exception as e:
                log.error(f"Game-day thread failed: {e}")

    @gameday_thread_task.before_loop
    async def before_gameday(self):
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


async def sync_members(members: list, reason: str, delay_between: float) -> int:
    """Re-check each linked member against MemberPress and update tier/role.

    Downgrades to unsubscribed are double-checked after 30s to guard against
    transient API failures. Returns the number of roles changed.
    """
    changed = 0
    for record in members:
        try:
            active_ids = await mp.get_active_membership_ids(record["mp_member_id"], record["mp_email"])
            new_tier = mp.resolve_tier(active_ids)
            if new_tier != record["tier"]:
                if new_tier == "unsubscribed" and record["tier"] in ("gold", "silver", "insider"):
                    await asyncio.sleep(30)
                    verify_ids = await mp.get_active_membership_ids(record["mp_member_id"], record["mp_email"])
                    verify_tier = mp.resolve_tier(verify_ids)
                    if verify_tier == "unsubscribed":
                        log.info(f"{reason} confirmed downgrade for discord_id={record['discord_id']} ({record['mp_email']}) — tier {record['tier']} → unsubscribed")
                        db.log_tier_change(record["discord_id"], record["mp_email"], record["tier"], "unsubscribed", reason=f"{reason}:confirmed")
                        db.upsert_member(record["discord_id"], record["mp_member_id"], record["mp_email"], "unsubscribed")
                        await assign_role(int(record["discord_id"]), "unsubscribed")
                        changed += 1
                    else:
                        log.warning(f"{reason}: discord_id={record['discord_id']} showed unsubscribed then {verify_tier} — transient API issue, skipping")
                else:
                    log.info(f"{reason}: discord_id={record['discord_id']} tier {record['tier']} → {new_tier}")
                    db.log_tier_change(record["discord_id"], record["mp_email"], record["tier"], new_tier, reason=reason)
                    db.upsert_member(record["discord_id"], record["mp_member_id"], record["mp_email"], new_tier)
                    await assign_role(int(record["discord_id"]), new_tier)
                    changed += 1
            else:
                db.upsert_member(record["discord_id"], record["mp_member_id"], record["mp_email"], new_tier)
        except Exception as e:
            log.error(f"{reason} error for discord_id={record['discord_id']}: {e}")
        await asyncio.sleep(delay_between)
    return changed


def tier_label(tier: str) -> str:
    return {"gold": "Gold", "silver": "Silver", "insider": "Insider", "unsubscribed": "Unsubscribed"}.get(tier, tier.title())


async def send_welcome_dm(discord_id: int, tier: str):
    """Welcome DM after a successful verification, with a tier-specific channel guide."""
    tier_perks = {
        "gold": "As a **Gold** member you have full access — insider reports, the Gold lounge, AMAs, and voice chats.",
        "silver": "As a **Silver** member you get insider reports and the Silver channels, plus community events.",
        "insider": "As an **Insider** you have access to the community channels and insider discussions.",
    }
    try:
        user = await bot.fetch_user(discord_id)
        await user.send(
            f"🎉 Welcome to CougConnect! Your **{tier_label(tier)}** role is set.\n\n"
            f"{tier_perks.get(tier, '')}\n\n"
            "Introduce yourself in the community channel and jump into the conversation. Go Cougs! 🏈"
        )
    except Exception:
        log.info(f"Could not DM welcome to discord_id={discord_id} (DMs likely closed)")


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
                        "If you need to update your membership, contact an admin."
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

        active_ids = await mp.get_active_membership_ids(record["mp_member_id"], record["mp_email"])
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
        active_ids = await mp.get_active_membership_ids(mp_id)
    tier = mp.resolve_tier(active_ids)

    existing = db.get_member_by_discord(str(user.id))
    old_tier = existing["tier"] if existing else "none"
    db.log_tier_change(str(user.id), email, old_tier, tier, reason="link-member")
    db.upsert_member(str(user.id), mp_id, email, tier)
    db.remove_unlinked(mp_id)
    success = await assign_role(user.id, tier)

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

    active_ids = await mp.get_active_membership_ids(existing["mp_member_id"], existing["mp_email"])
    tier = mp.resolve_tier(active_ids)
    if tier != existing["tier"]:
        db.log_tier_change(str(user.id), existing["mp_email"], existing["tier"], tier, reason="sync-member")
    db.upsert_member(str(user.id), existing["mp_member_id"], existing["mp_email"], tier)
    await assign_role(user.id, tier)
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
        await interaction.response.send_message(embed=embed)
        return

    embed = discord.Embed(title="CougConnect FAQ", color=discord.Color.blue())
    for i, item in enumerate(faqs, 1):
        embed.add_field(name=f"{i}. {item['q']}", value=item["a"], inline=False)
    embed.set_footer(text="Use /faq <number> to view a specific answer")
    await interaction.response.send_message(embed=embed)


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


async def handle_verify_page_get(request: web.Request) -> web.Response:
    """Serve the email entry form."""
    token = request.rel_url.query.get("token", "")
    discord_id = request.rel_url.query.get("discord_id", "")

    if not token or not discord_id:
        return _page("Error", """
            <h1>❌ Invalid Link</h1>
            <p>This verification link is invalid. Please click the button in Discord again.</p>
        """)

    form = f"""
        <h1>🔐 Verify Membership</h1>
        <p>Enter the email address associated with your CougConnect subscription.</p>
        <form method="POST" action="/verify-page">
          <input type="hidden" name="token" value="{html.escape(token, quote=True)}">
          <input type="hidden" name="discord_id" value="{html.escape(discord_id, quote=True)}">
          <input type="email" name="email" placeholder="your@email.com" required autofocus>
          <button type="submit">Verify My Membership</button>
        </form>
    """
    return _page("Verify Membership", form)


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

    # Validate token
    stored_discord_id = db.consume_token(token)
    if not stored_discord_id:
        return _page("Link Expired", """
            <h1>⏰ Link Expired</h1>
            <p>This verification link has expired or already been used.</p>
            <p>Click the <strong>Verify Membership</strong> button in Discord to get a new link.</p>
        """)
    if stored_discord_id != discord_id:
        return _page("Error", "<h1>❌ Invalid Link</h1><p>This link is not valid for your account.</p>")

    # Check if email is already linked to a different Discord account
    existing_link = db.get_member_by_email(email)
    if existing_link and existing_link["discord_id"] != discord_id:
        return _page("Already Linked", f"""
            <h1>⚠️ Email Already Linked</h1>
            <p><strong>{safe_email}</strong> is already connected to a different Discord account.</p>
            <p>If this is a mistake, please contact an admin in the Discord server.</p>
        """)

    # Look up member in MemberPress
    mp_member = await mp.get_member_by_email(email)
    if not mp_member:
        return _page("Not Found", f"""
            <h1>❌ Email Not Found</h1>
            <p>No CougConnect account was found for <strong>{safe_email}</strong>.</p>
            <p>Make sure you're using the email you signed up with, or
               <a href="https://cougconnect.com" style="color:#3b82f6;">visit CougConnect</a> to create an account.</p>
        """)

    mp_id = mp_member.get("id")
    active_ids = mp.active_ids_from_member_object(mp_member)
    if not active_ids:
        active_ids = await mp.get_active_membership_ids(mp_id)
    tier = mp.resolve_tier(active_ids)

    if tier == "unsubscribed":
        return _page("No Active Subscription", f"""
            <h1>⚠️ No Active Subscription</h1>
            <p>The account for <strong>{safe_email}</strong> doesn't have an active CougConnect membership.</p>
            <p><a href="https://cougconnect.com/become-a-subscriber/" style="color:#3b82f6;">Subscribe here</a>
               to get access.</p>
        """)

    db.upsert_member(discord_id, mp_id, email, tier)
    db.remove_unlinked(mp_id)
    success = await assign_role(int(discord_id), tier)

    if not success:
        log.error(f"Role assignment failed for discord_id={discord_id}")
        return _page("Error", """
            <h1>⚠️ Role Assignment Failed</h1>
            <p>We verified your membership but couldn't assign your Discord role.
               Please contact an admin in the Discord server.</p>
        """)

    log.info(f"Verified discord_id={discord_id} email={email} tier={tier}")
    asyncio.create_task(send_welcome_dm(int(discord_id), tier))
    tier_display = tier_label(tier)
    return _page("Verified!", f"""
        <h1>✅ You're Verified!</h1>
        <p>Your <strong>{tier_display}</strong> membership has been confirmed.</p>
        <p>Head back to Discord — your <strong>{tier_display}</strong> role has been assigned. You're all set!</p>
    """)


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

# MemberPress fires the same event in bursts; drop repeats seen within this window
WEBHOOK_DEDUPE_SECONDS = 30
_recent_webhooks: dict[tuple[int, str], float] = {}

# Reactivation lookups retry on this schedule before giving up
REACTIVATE_RETRY_DELAYS = [15, 60, 300]


async def process_webhook_event(event: str, mp_member_id: int, record: dict):
    """Apply a MemberPress webhook in the background (handler already returned 200)."""
    discord_id = int(record["discord_id"])

    if event in INACTIVE_EVENTS:
        db.log_tier_change(record["discord_id"], record["mp_email"], record["tier"], "unsubscribed", reason=f"webhook:{event}")
        db.upsert_member(record["discord_id"], mp_member_id, record["mp_email"], "unsubscribed")
        await assign_role(discord_id, "unsubscribed")
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
        for attempt, delay in enumerate(REACTIVATE_RETRY_DELAYS, 1):
            await asyncio.sleep(delay)
            active_ids = await mp.get_active_membership_ids(mp_member_id, record["mp_email"])
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
        log.info(f"Re-activated discord_id={discord_id} as tier={tier} via event={event}")


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
    member_data = data.get("data", {}).get("member") or data.get("member") or {}
    mp_member_id = member_data.get("id") or data.get("data", {}).get("member_id")

    if not mp_member_id:
        return web.json_response({"status": "ignored — no member_id"})

    mp_member_id = int(mp_member_id)
    record = db.get_member_by_mp_id(mp_member_id)
    if not record:
        db.record_unlinked(mp_member_id)
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
