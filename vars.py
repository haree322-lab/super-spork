import os
from os import environ


def _load_env_file():
    env_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), ".env")
    if os.path.exists(env_path):
        with open(env_path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith("#"):
                    continue
                if "=" in line:
                    key, value = line.split("=", 1)
                    os.environ.setdefault(key.strip(), value.strip())


_load_env_file()

# API Configuration
API_ID = int(os.environ.get("API_ID", "22484497"))
API_HASH = os.environ.get("API_HASH", "c38cb053916c47a97590c244663cbaef")
BOT_TOKEN = os.environ.get("BOT_TOKEN",
                           "8079475622:AAFRasQmKozum_FAVXo4VM-KdpPm8KYETvw")

CREDIT = os.environ.get("CREDIT", "𝕞𝕚𝕘𝕙𝕥𝕪 𝕒𝕥𝕠𝕞")

# Owner and Admin Configuration
OWNER_ID = int(os.environ.get("OWNER_ID", "6252997817"))
ADMINS = [int(x) for x in os.environ.get("ADMINS", "6252997817").split()
          ]  # Default to owner ID

# Channel Configuration
PREMIUM_CHANNEL = "https://t.me/+KqoscIcGJhQ1MjE1"
# Thumbnail Configuration
THUMBNAILS = list(
    map(
        str,
        os.environ.get("THUMBNAILS", "https://files.catbox.moe/4lwnko.jpg").
        split()))  # Image Link For Default Thumbnail

# Web Server Configuration
WEB_SERVER = os.environ.get("WEB_SERVER", "False").lower() == "true"
WEBHOOK = True  # Don't change this
PORT = int(os.environ.get("PORT", 8000))

# Message Formats
AUTH_MESSAGES = {
    "subscription_active":
    """<b>🎉 Subscription Activated!</b>

<blockquote>Your subscription has been activated and will expire on {expiry_date}.
You can now use the bot!</blockquote>\n\n Type /start to start uploading """,
    "subscription_expired":
    """<b>⚠️ Your Subscription Has Ended</b>

<blockquote>Your access to the bot has been revoked as your subscription period has expired.
Please contact the admin to renew your subscription.</blockquote>""",
    "user_added":
    """<b>✅ User Added Successfully!</b>

<blockquote>👤 Name: {name}
🆔 User ID: {user_id}
📅 Expiry: {expiry_date}</blockquote>""",
    "user_removed":
    """<b>✅ User Removed Successfully!</b>

<blockquote>User ID {user_id} has been removed from authorized users.</blockquote>""",
    "access_denied":
    """<b>⚠️ Access Denied!</b>

<blockquote>You are not authorized to use this bot.
Please contact the admin @ItsUGBot to get access.</blockquote>""",
    "not_admin":
    "⚠️ You are not authorized to use this command!",
    "invalid_format":
    """❌ <b>Invalid Format!</b>

<blockquote>Use format: {format}</blockquote>"""
}
