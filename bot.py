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
import hashlib
import hmac
import json
import logging
import os
import datetime
from datetime import datetime as dt

import aiohttp
import aiohttp.web as web
import discord
from discord import app_commands
from discord.ext import commands, tasks
from dotenv import load_dotenv

import database as db
import memberpress as mp

load_dotenv()

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
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

ROLE_IDS = {
    "gold":         int(os.getenv("DISCORD_ROLE_GOLD_ID", "0")),
    "silver":       int(os.getenv("DISCORD_ROLE_SILVER_ID", "0")),
    "insider":      int(os.getenv("DISCORD_ROLE_INSIDER_ID", "0")),
    "unsubscribed": int(os.getenv("DISCORD_ROLE_UNSUBSCRIBED_ID", "0")),
}

FAQ_PATH = os.path.join(os.path.dirname(__file__), "faq.json")


def load_faq() -> list[dict]:
    try:
        with open(FAQ_PATH) as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return []


# ── Bot class ──────────────────────────────────────────────────────────────────

class CougConnectBot(commands.Bot):
    def __init__(self):
        intents = discord.Intents.default()
        intents.members = True
        super().__init__(command_prefix="!", intents=intents)
        self._web_runner: web.AppRunner | None = None

    async def setup_hook(self):
        db.init_db()
        mp.load_tier_ids()
        self.add_view(VerifyView())
        self.add_view(ReSyncView())
        await self.tree.sync()
        log.info("Slash commands synced.")
        self.cleanup_tokens_task.start()
        self.sync_all_members_task.start()

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
        changed = 0
        for record in members:
            try:
                active_ids = await mp.get_active_membership_ids(record["mp_member_id"], record["mp_email"])
                new_tier = mp.resolve_tier(active_ids)
                if new_tier != record["tier"]:
                    # Safety: never downgrade an active member in the auto-sync.
                    # Real cancellations are handled by MemberPress webhooks.
                    # This prevents API failures from incorrectly demoting members.
                    if new_tier == "unsubscribed" and record["tier"] in ("gold", "silver", "insider"):
                        log.warning(f"Auto-sync skipping downgrade for discord_id={record['discord_id']} ({record['mp_email']}) — use webhook or /sync-member to demote")
                        await post_admin_log(
                            f"⚠️ **Skipped downgrade** — <@{record['discord_id']}> (`{record['mp_email']}`)\n"
                            f"MemberPress shows **Unsubscribed** but member has **{record['tier'].title()}** role.\n"
                            f"Use `/sync-member` to demote manually if confirmed cancelled."
                        )
                    else:
                        log.info(f"Sync: discord_id={record['discord_id']} tier {record['tier']} → {new_tier}")
                        db.log_tier_change(record["discord_id"], record["mp_email"], record["tier"], new_tier, reason="auto-sync")
                        db.upsert_member(record["discord_id"], record["mp_member_id"], record["mp_email"], new_tier)
                        await assign_role(int(record["discord_id"]), new_tier)
                        changed += 1
                else:
                    db.upsert_member(record["discord_id"], record["mp_member_id"], record["mp_email"], new_tier)
            except Exception as e:
                log.error(f"Sync error for discord_id={record['discord_id']}: {e}")
            await asyncio.sleep(5)
        log.info(f"Periodic sync complete. {changed} role(s) updated out of {len(members)} members.")

    @sync_all_members_task.before_loop
    async def before_sync(self):
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


async def assign_role(discord_id: int, tier: str) -> bool:
    """Remove all tier roles and assign the correct one. Returns True on success."""
    guild = get_guild()
    if not guild:
        return False
    member = guild.get_member(discord_id) or await guild.fetch_member(discord_id)
    if not member:
        return False

    roles_to_remove = [
        guild.get_role(rid) for rid in ROLE_IDS.values()
        if guild.get_role(rid) and guild.get_role(rid) in member.roles
    ]
    new_role = guild.get_role(ROLE_IDS.get(tier, 0))
    if not new_role:
        log.warning(f"Role ID not configured for tier '{tier}'")
        return False

    if roles_to_remove:
        await member.remove_roles(*roles_to_remove, reason="CougConnect sync")
    await member.add_roles(new_role, reason=f"CougConnect tier: {tier}")
    return True


def tier_label(tier: str) -> str:
    return {"gold": "Gold", "silver": "Silver", "insider": "Insider", "unsubscribed": "Unsubscribed"}.get(tier, tier.title())


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
    success = await assign_role(user.id, tier)

    if success:
        await interaction.followup.send(
            f"✅ Linked **{user.display_name}** to `{email}` — assigned **{tier_label(tier)}** role.",
            ephemeral=True,
        )
    else:
        await interaction.followup.send(
            f"⚠️ Saved link but couldn't assign role — check role IDs in config.",
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

    # Show active membership IDs from MemberPress
    mp_member = await mp.get_member_by_id(record["mp_member_id"])
    if mp_member:
        active_memberships = mp_member.get("active_memberships", [])
        if len(active_memberships) > 1:
            names = [m.get("title", f"ID {m.get('id')}") if isinstance(m, dict) else f"ID {m}" for m in active_memberships]
            embed.add_field(name="Active Subscriptions", value="\n".join(f"• {n}" for n in names), inline=False)

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

    # Show all active subscriptions if more than one
    if mp_data:
        active_memberships = mp_data.get("active_memberships", [])
        if len(active_memberships) > 1:
            names = [m.get("title", f"ID {m.get('id')}") if isinstance(m, dict) else f"ID {m}" for m in active_memberships]
            embed.add_field(name="Active Subscriptions", value="\n".join(f"• {n}" for n in names), inline=False)

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
    changed = 0
    for record in members:
        try:
            active_ids = await mp.get_active_membership_ids(record["mp_member_id"], record["mp_email"])
            new_tier = mp.resolve_tier(active_ids)
            if new_tier != record["tier"]:
                if new_tier == "unsubscribed" and record["tier"] in ("gold", "silver", "insider"):
                    log.warning(f"sync-all skipping downgrade for discord_id={record['discord_id']} ({record['mp_email']}) — use /sync-member to demote manually")
                    await post_admin_log(
                        f"⚠️ **Skipped downgrade** — <@{record['discord_id']}> (`{record['mp_email']}`)\n"
                        f"MemberPress shows **Unsubscribed** but member has **{record['tier'].title()}** role.\n"
                        f"Use `/sync-member` to demote manually if confirmed cancelled."
                    )
                else:
                    db.log_tier_change(record["discord_id"], record["mp_email"], record["tier"], new_tier, reason="sync-all")
                    db.upsert_member(record["discord_id"], record["mp_member_id"], record["mp_email"], new_tier)
                    await assign_role(int(record["discord_id"]), new_tier)
                    changed += 1
            else:
                db.upsert_member(record["discord_id"], record["mp_member_id"], record["mp_email"], new_tier)
        except Exception as e:
            log.error(f"sync-all error for discord_id={record['discord_id']}: {e}")
        await asyncio.sleep(0.5)
    await interaction.followup.send(
        f"✅ Sync complete — checked **{len(members)}** members, updated **{changed}** role(s).",
        ephemeral=True,
    )


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
    embed.set_footer(text=f"As of {dt.utcnow().strftime('%m/%d/%Y %H:%M')} UTC")
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
          <input type="hidden" name="token" value="{token}">
          <input type="hidden" name="discord_id" value="{discord_id}">
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
            <p><strong>{email}</strong> is already connected to a different Discord account.</p>
            <p>If this is a mistake, please contact an admin in the Discord server.</p>
        """)

    # Look up member in MemberPress
    mp_member = await mp.get_member_by_email(email)
    if not mp_member:
        return _page("Not Found", f"""
            <h1>❌ Email Not Found</h1>
            <p>No CougConnect account was found for <strong>{email}</strong>.</p>
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
            <p>The account for <strong>{email}</strong> doesn't have an active CougConnect membership.</p>
            <p><a href="https://cougconnect.com/become-a-subscriber/" style="color:#3b82f6;">Subscribe here</a>
               to get access.</p>
        """)

    db.upsert_member(discord_id, mp_id, email, tier)
    success = await assign_role(int(discord_id), tier)

    if not success:
        log.error(f"Role assignment failed for discord_id={discord_id}")
        return _page("Error", """
            <h1>⚠️ Role Assignment Failed</h1>
            <p>We verified your membership but couldn't assign your Discord role.
               Please contact an admin in the Discord server.</p>
        """)

    log.info(f"Verified discord_id={discord_id} email={email} tier={tier}")
    tier_display = tier_label(tier)
    return _page("Verified!", f"""
        <h1>✅ You're Verified!</h1>
        <p>Your <strong>{tier_display}</strong> membership has been confirmed.</p>
        <p>Head back to Discord — your <strong>{tier_display}</strong> role has been assigned. You're all set!</p>
    """)


async def handle_webhook(request: web.Request) -> web.Response:
    """
    MemberPress subscription webhook.
    Fired on: subscription-expired, subscription-cancelled, subscription-stopped,
              subscription-resumed, subscription-upgraded, etc.
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
        log.info(f"Webhook for unknown mp_member_id={mp_member_id}, ignoring.")
        return web.json_response({"status": "ignored — member not linked"})

    discord_id = int(record["discord_id"])

    inactive_events = {
        "subscription-expired", "subscription-cancelled", "subscription-stopped",
        "member-account-expired",
        # NOTE: subscription-paused is NOT here — paused means billing is paused
        # but access continues until the expiry date. We re-fetch from MemberPress
        # to let the API decide rather than immediately demoting.
    }
    reactivate_events = {
        "subscription-resumed", "subscription-renewed", "subscription-upgraded",
        "subscription-created", "transaction-completed",
        "member-signup-completed", "subscription-paused",
    }

    if event in inactive_events:
        db.log_tier_change(record["discord_id"], record["mp_email"], record["tier"], "unsubscribed", reason=f"webhook:{event}")
        db.upsert_member(record["discord_id"], mp_member_id, record["mp_email"], "unsubscribed")
        await assign_role(discord_id, "unsubscribed")
        log.info(f"Set discord_id={discord_id} to unsubscribed via event={event}")

        # DM the member
        try:
            user = await bot.fetch_user(discord_id)
            await user.send(
                "Your CougConnect membership has expired or been cancelled. "
                "Renew at https://cougconnect.com to restore your access. 🏈"
            )
        except Exception:
            pass

    elif event in reactivate_events:
        # Wait for MemberPress to fully commit the subscription before querying.
        # Webhooks fire immediately on payment but the subscription may not be
        # in the API yet — this prevents incorrectly resolving to Unsubscribed.
        await asyncio.sleep(15)
        active_ids = await mp.get_active_membership_ids(mp_member_id, record["mp_email"])
        tier = mp.resolve_tier(active_ids)
        if tier == "unsubscribed":
            log.warning(
                f"Webhook {event} for discord_id={discord_id} resolved to unsubscribed — "
                f"MemberPress may still be processing. Skipping update; use /sync-member to retry."
            )
            await post_admin_log(
                f"⚠️ **Webhook race condition** — <@{discord_id}> (`{record['mp_email']}`)\n"
                f"Event `{event}` fired but MemberPress API returned no active membership.\n"
                f"Use `/sync-member` in a few minutes to retry."
            )
            return web.json_response({"status": "skipped — unsubscribed after reactivate event"})
        db.log_tier_change(record["discord_id"], record["mp_email"], record["tier"], tier, reason=f"webhook:{event}")
        db.upsert_member(record["discord_id"], mp_member_id, record["mp_email"], tier)
        await assign_role(discord_id, tier)
        log.info(f"Re-activated discord_id={discord_id} as tier={tier} via event={event}")

    return web.json_response({"status": "ok"})


async def handle_admin_import(request: web.Request) -> web.Response:
    """Temporary endpoint to bulk-import member records from the migration script."""
    try:
        data = await request.json()
    except Exception:
        return web.json_response({"error": "Invalid JSON"}, status=400)
    if data.get("secret") != BOT_VERIFY_SECRET:
        return web.json_response({"error": "Unauthorized"}, status=403)
    members = data.get("members", [])
    imported = 0
    skipped = 0
    for m in members:
        if db.get_member_by_discord(m["discord_id"]):
            skipped += 1
            continue
        db.upsert_member(m["discord_id"], m["mp_member_id"], m["mp_email"], m["tier"])
        imported += 1
    log.info(f"Admin import: {imported} imported, {skipped} skipped")
    return web.json_response({"imported": imported, "skipped": skipped})


async def start_web_server():
    app = web.Application(client_max_size=50*1024*1024)
    app.router.add_get("/verify-page", handle_verify_page_get)
    app.router.add_post("/verify-page", handle_verify_page_post)
    app.router.add_post("/webhook", handle_webhook)
    app.router.add_post("/admin/import", handle_admin_import)
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
