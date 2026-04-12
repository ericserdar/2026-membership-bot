"""
Reads cougconnect_import.db and pushes all records to the live Railway bot.
Usage: python3 push_import.py
"""
import os
import sqlite3
import urllib.request
import json

BOT_URL = os.environ.get("BOT_PUBLIC_URL", "").rstrip("/")
SECRET  = os.environ.get("BOT_VERIFY_SECRET", "")
DB_PATH = os.environ.get("IMPORT_DB", "./cougconnect_import.db")

if not BOT_URL or not SECRET:
    raise SystemExit("Set BOT_PUBLIC_URL and BOT_VERIFY_SECRET env vars first.")

with sqlite3.connect(DB_PATH) as conn:
    rows = conn.execute(
        "SELECT discord_id, mp_member_id, mp_email, tier FROM member_links"
    ).fetchall()

members = [
    {"discord_id": r[0], "mp_member_id": r[1], "mp_email": r[2], "tier": r[3]}
    for r in rows
]

print(f"Pushing {len(members)} records to {BOT_URL}/admin/import ...")

payload = json.dumps({"secret": SECRET, "members": members}).encode()
req = urllib.request.Request(
    f"{BOT_URL}/admin/import",
    data=payload,
    headers={"Content-Type": "application/json"},
    method="POST",
)
with urllib.request.urlopen(req) as resp:
    result = json.loads(resp.read())

print(f"Done — imported: {result['imported']}, skipped: {result['skipped']}")
