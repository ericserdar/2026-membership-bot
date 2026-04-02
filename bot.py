"""
CougConnect Discord Membership Bot
-----------------------------------
- Verifies Discord users against MemberPress subscriptions
- Assigns roles: Gold, Silver, Insider, Unsubscribed
- Exposes an aiohttp web server for:
    POST /verify   — called by cougconnect.com after user logs in
    POST /webhook  — MemberPress subscription status webhooks
"""

import asyncio
import hashlib
import hmac
import json
import logging
import os
from datetime import datetime

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
PORT = int(os.getenv("PORT", "8080"))
BOT_PUBLIC_URL = os.getenv("BOT_PUBLIC_URL", "http://localhost:8080")
BOT_VERIFY_SECRET = os.getenv("BOT_VERIFY_SECRET", "")
MP_WEBHOOK_SECRET = os.getenv("MEMBERPRESS_WEBHOOK_SECRET", "")

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
        await self.tree.sync()
        log.info("Slash commands synced.")
        self.cleanup_tokens_task.start()

    async def on_ready(self):
        log.info(f"Logged in as {self.user} (ID: {self.user.id})")
        await self._post_verify_embed()

    async def _post_verify_embed(self):
        channel = self.get_channel(VERIFY_CHANNEL_ID)
        if not channel:
            return
        # Only post if no recent bot message with the button exists
        async for msg in channel.history(limit=10):
            if msg.author == self.user and msg.components:
                return  # already posted
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

    @tasks.loop(hours=1)
    async def cleanup_tokens_task(self):
        db.cleanup_expired_tokens()


bot = CougConnectBot()


# ── Helpers ────────────────────────────────────────────────────────────────────

def get_guild() -> discord.Guild | None:
    return bot.get_guild(GUILD_ID)


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
        discord_id = str(interaction.user.id)
        token = db.create_token(discord_id)
        url = f"{os.getenv('MEMBERPRESS_BASE_URL', 'https://cougconnect.com')}/discord-verify?token={token}&discord_id={discord_id}"
        embed = discord.Embed(
            title="Verify Your Membership",
            description=(
                f"[Click here to verify your CougConnect subscription]({url})\n\n"
                "You'll be asked to log in to your CougConnect account. "
                "This link expires in **15 minutes**."
            ),
            color=discord.Color.blue(),
        )
        await interaction.response.send_message(embed=embed, ephemeral=True)


bot.add_view(VerifyView())


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
    active_ids = await mp.get_active_membership_ids(mp_id)
    tier = mp.resolve_tier(active_ids)

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

    active_ids = await mp.get_active_membership_ids(existing["mp_member_id"])
    tier = mp.resolve_tier(active_ids)
    db.upsert_member(str(user.id), existing["mp_member_id"], existing["mp_email"], tier)
    await assign_role(user.id, tier)
    await interaction.followup.send(
        f"✅ Synced **{user.display_name}** — current tier: **{tier_label(tier)}**", ephemeral=True
    )


@bot.tree.command(name="get-info", description="Show the email address tied to a Discord user")
@app_commands.describe(user="Discord member to look up")
@app_commands.default_permissions(manage_roles=True)
async def get_info(interaction: discord.Interaction, user: discord.Member):
    record = db.get_member_by_discord(str(user.id))
    if not record:
        await interaction.response.send_message(f"❌ **{user.display_name}** has not verified their membership.", ephemeral=True)
        return
    embed = discord.Embed(title="Member Info", color=discord.Color.blue())
    embed.add_field(name="Discord", value=f"{user.mention} (`{user.id}`)", inline=False)
    embed.add_field(name="Email", value=record["mp_email"], inline=False)
    embed.add_field(name="Tier", value=tier_label(record["tier"]), inline=True)
    embed.add_field(name="Linked", value=record["linked_at"][:10] if record["linked_at"] else "—", inline=True)
    await interaction.response.send_message(embed=embed, ephemeral=True)


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
    embed.set_footer(text=f"As of {datetime.utcnow().strftime('%m/%d/%Y %H:%M')} UTC")
    await interaction.response.send_message(embed=embed, ephemeral=True)


# ── aiohttp web server ─────────────────────────────────────────────────────────

async def handle_verify(request: web.Request) -> web.Response:
    """
    Called by cougconnect.com after a user logs in and their MemberPress tier is known.
    Expected JSON body:
      { token, discord_id, tier, mp_member_id, mp_email, secret }
    """
    try:
        data = await request.json()
    except Exception:
        return web.json_response({"error": "Invalid JSON"}, status=400)

    if data.get("secret") != BOT_VERIFY_SECRET:
        return web.json_response({"error": "Unauthorized"}, status=403)

    token = data.get("token")
    discord_id = data.get("discord_id")
    tier = data.get("tier", "").lower()
    mp_member_id = data.get("mp_member_id")
    mp_email = data.get("mp_email", "")

    if not all([token, discord_id, tier, mp_member_id]):
        return web.json_response({"error": "Missing required fields"}, status=400)

    if tier not in ("gold", "silver", "insider", "unsubscribed"):
        return web.json_response({"error": f"Unknown tier: {tier}"}, status=400)

    stored_discord_id = db.consume_token(token)
    if not stored_discord_id:
        return web.json_response({"error": "Invalid or expired token"}, status=400)
    if stored_discord_id != discord_id:
        return web.json_response({"error": "Token/Discord ID mismatch"}, status=400)

    db.upsert_member(discord_id, mp_member_id, mp_email, tier)

    success = await assign_role(int(discord_id), tier)
    if not success:
        log.error(f"Failed to assign role for discord_id={discord_id}")
        return web.json_response({"error": "Role assignment failed"}, status=500)

    log.info(f"Verified discord_id={discord_id} as tier={tier}")
    return web.json_response({"status": "ok", "tier": tier})


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
        "member-account-expired", "subscription-paused",
    }
    reactivate_events = {
        "subscription-resumed", "subscription-renewed", "subscription-upgraded",
        "member-signup-completed",
    }

    if event in inactive_events:
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
        # Re-fetch tier from MemberPress
        active_ids = await mp.get_active_membership_ids(mp_member_id)
        tier = mp.resolve_tier(active_ids)
        db.upsert_member(record["discord_id"], mp_member_id, record["mp_email"], tier)
        await assign_role(discord_id, tier)
        log.info(f"Re-activated discord_id={discord_id} as tier={tier} via event={event}")

    return web.json_response({"status": "ok"})


async def start_web_server():
    app = web.Application()
    app.router.add_post("/verify", handle_verify)
    app.router.add_post("/webhook", handle_webhook)
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
