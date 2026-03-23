# 🔧 Standard Library
import os
import re
import sys
import time
import json
import random
import string
import shutil
import shlex
import zipfile
import urllib
import subprocess
import asyncio
from datetime import datetime, timedelta
from base64 import b64encode, b64decode
from html import escape as html_escape
from subprocess import getstatusoutput

if os.name == "nt" and hasattr(asyncio, "WindowsSelectorEventLoopPolicy"):
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

try:
    asyncio.get_event_loop()
except RuntimeError:
    asyncio.set_event_loop(asyncio.new_event_loop())

# 🕒 Timezone
import pytz

# 📦 Third-party Libraries
import aiohttp
import aiofiles
import requests
import ffmpeg
import m3u8
import cloudscraper
import yt_dlp
import tgcrypto
from logs import logging
from bs4 import BeautifulSoup
from pytube import YouTube
from Crypto.Cipher import AES
from Crypto.Util.Padding import unpad

# ⚙️ Pyrogram
from pyrogram import Client, filters, idle, raw
from pyrogram.handlers import MessageHandler
from pyrogram.types import (
    Message,
    CallbackQuery,
    InlineKeyboardMarkup,
    InlineKeyboardButton
)
from pyrogram.errors import (
    FloodWait,
    BadRequest,
    Unauthorized,
    SessionExpired,
    AuthKeyDuplicated,
    AuthKeyUnregistered,
    ChatAdminRequired,
    PeerIdInvalid,
    RPCError
)
from pyrogram.errors.exceptions.bad_request_400 import MessageNotModified

# 🧠 Bot Modules
import auth
import thanos as helper
from html_handler import html_handler, process_txt_to_html
from thanos import *

from clean import register_clean_handler
from logs import logging
from utils import progress_bar
from vars import *
from pyromod import listen
from db import db
from compat import (
    IS_WINDOWS, get_ffmpeg, get_ffprobe, get_aria2c,
    get_ytdlp, run_shell_cmd, async_shell_cmd, restart_process, safe_quote,
)

auto_flags = {}
auto_clicked = False

# Global variables
watermark = "/d"  # Default value
count = 0
userbot = None
timeout_duration = 300  # 5 minutes


# Initialize bot with random session
bot = Client(
    "ugx",
    api_id=API_ID,
    api_hash=API_HASH,
    bot_token=BOT_TOKEN,
    workers=4,
    sleep_threshold=60,
    in_memory=True
)

# Register command handlers
register_clean_handler(bot)

@bot.on_message(filters.command("setlog") & filters.private)
async def set_log_channel_cmd(client: Client, message: Message):
    """Set log channel for the bot"""
    try:
        # Check if user is admin
        if not db.is_admin(message.from_user.id):
            await message.reply_text("⚠️ You are not authorized to use this command.")
            return

        # Get command arguments
        args = message.text.split()
        if len(args) != 2:
            await message.reply_text(
                "❌ Invalid format!\n\n"
                "Use: /setlog channel_id\n"
                "Example: /setlog -100123456789"
            )
            return

        try:
            channel_id = int(args[1])
        except ValueError:
            await message.reply_text("❌ Invalid channel ID. Please use a valid number.")
            return

        # Set the log channel without validation
        if db.set_log_channel(client.me.username, channel_id):
            await message.reply_text(
                "✅ Log channel set successfully!\n\n"
                f"Channel ID: {channel_id}\n"
                f"Bot: @{client.me.username}"
            )
        else:
            await message.reply_text("❌ Failed to set log channel. Please try again.")

    except Exception as e:
        await message.reply_text(f"❌ Error: {str(e)}")

@bot.on_message(filters.command("getlog") & filters.private)
async def get_log_channel_cmd(client: Client, message: Message):
    """Get current log channel info"""
    try:
        # Check if user is admin
        if not db.is_admin(message.from_user.id):
            await message.reply_text("⚠️ You are not authorized to use this command.")
            return

        # Get log channel ID
        channel_id = db.get_log_channel(client.me.username)
        
        if channel_id:
            # Try to get channel info but don't worry if it fails
            try:
                channel = await client.get_chat(channel_id)
                channel_info = f"📢 Channel Name: {channel.title}\n"
            except:
                channel_info = ""
            
            await message.reply_text(
                f"**📋 Log Channel Info**\n\n"
                f"🤖 Bot: @{client.me.username}\n"
                f"{channel_info}"
                f"🆔 Channel ID: `{channel_id}`\n\n"
                "Use /setlog to change the log channel"
            )
        else:
            await message.reply_text(
                f"**📋 Log Channel Info**\n\n"
                f"🤖 Bot: @{client.me.username}\n"
                "❌ No log channel set\n\n"
                "Use /setlog to set a log channel"
            )

    except Exception as e:
        await message.reply_text(f"❌ Error: {str(e)}")

@bot.on_message(filters.command("setgroup") & filters.private)
async def set_group_cmd(client: Client, message: Message):
    try:
        if not db.is_user_authorized(message.from_user.id, client.me.username):
            await message.reply_text("⚠️ You are not authorized to use this command.")
            return

        args = message.text.split()
        if len(args) != 2:
            await message.reply_text(
                "❌ Invalid format!\n\n"
                "Use: /setgroup group_id\n"
                "Example: /setgroup -100123456789\n\n"
                "<i>The group must be a supergroup with Topics enabled, and bot must be admin.</i>"
            )
            return

        try:
            group_id = int(args[1])
        except ValueError:
            await message.reply_text("❌ Invalid group ID. Please use a valid number.")
            return

        try:
            chat = await client.get_chat(group_id)
            if not getattr(chat, 'is_forum', False):
                await message.reply_text(
                    "❌ This group does not have Topics enabled.\n\n"
                    "<i>Enable Topics in Group Settings → Topics to use this feature.</i>"
                )
                return

            from pyrogram.enums import ChatMemberStatus
            me_member = await client.get_chat_member(group_id, client.me.id)
            if me_member.status not in [ChatMemberStatus.ADMINISTRATOR, ChatMemberStatus.OWNER]:
                await message.reply_text(
                    "❌ Bot is not an admin in this group.\n\n"
                    "<i>Add the bot as admin with permission to manage topics.</i>"
                )
                return

            db.set_group(message.from_user.id, group_id)
            await message.reply_text(
                f"✅ Group set successfully!\n\n"
                f"📌 Group: <b>{chat.title}</b>\n"
                f"🆔 ID: <code>{group_id}</code>\n\n"
                f"<i>When using /drm, you can now choose to upload to this group. "
                f"A new topic will be created for each batch.</i>"
            )
        except Exception as e:
            await message.reply_text(f"❌ Could not verify group: {str(e)}\n\n<i>Make sure the bot is added to the group.</i>")

    except Exception as e:
        await message.reply_text(f"❌ Error: {str(e)}")

@bot.on_message(filters.command("getgroup") & filters.private)
async def get_group_cmd(client: Client, message: Message):
    try:
        if not db.is_user_authorized(message.from_user.id, client.me.username):
            await message.reply_text("⚠️ You are not authorized to use this command.")
            return

        group_id = db.get_group(message.from_user.id)
        if group_id:
            try:
                chat = await client.get_chat(group_id)
                group_name = chat.title
            except Exception:
                group_name = "Unknown"

            await message.reply_text(
                f"**📋 Your Upload Group**\n\n"
                f"📌 Group: <b>{group_name}</b>\n"
                f"🆔 ID: <code>{group_id}</code>\n\n"
                f"Use /setgroup to change | /removegroup to remove"
            )
        else:
            await message.reply_text(
                "**📋 Your Upload Group**\n\n"
                "❌ No group set\n\n"
                "Use /setgroup group_id to set one"
            )
    except Exception as e:
        await message.reply_text(f"❌ Error: {str(e)}")

@bot.on_message(filters.command("removegroup") & filters.private)
async def remove_group_cmd(client: Client, message: Message):
    try:
        if not db.is_user_authorized(message.from_user.id, client.me.username):
            await message.reply_text("⚠️ You are not authorized to use this command.")
            return

        if db.remove_group(message.from_user.id):
            await message.reply_text("✅ Upload group removed successfully.")
        else:
            await message.reply_text("❌ No group was set.")
    except Exception as e:
        await message.reply_text(f"❌ Error: {str(e)}")

# Re-register auth commands
bot.add_handler(MessageHandler(auth.add_user_cmd, filters.command("add") & filters.private))
bot.add_handler(MessageHandler(auth.remove_user_cmd, filters.command("remove") & filters.private))
bot.add_handler(MessageHandler(auth.list_users_cmd, filters.command("users") & filters.private))
bot.add_handler(MessageHandler(auth.my_plan_cmd, filters.command("plan") & filters.private))

cookies_file_path = os.getenv("cookies_file_path", "youtube_cookies.txt")
yt_email = os.environ.get("YT_EMAIL", "")
yt_password = os.environ.get("YT_PASSWORD", "")
yt_auth_flags = f'--username "{yt_email}" --password "{yt_password}"' if yt_email and yt_password else ''
yt_bypass_flags = f'--cookies youtube_cookies.txt {yt_auth_flags} --extractor-args "youtube:player_client=web,android" --user-agent "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36" --no-check-certificates'


def is_bot_detection_error(error_msg):
    bot_keywords = [
        "Sign in to confirm you're not a bot",
        "Sign in to confirm your age",
        "This helps protect our community",
        "Use cookies-from-browser or cookies",
    ]
    error_str = str(error_msg)
    return any(kw.lower() in error_str.lower() for kw in bot_keywords)


async def ask_yt_credentials(bot_client, chat_id):
    await bot_client.send_message(
        chat_id,
        "**🔐 YouTube Bot Detection Error!**\n\n"
        "YouTube requires sign-in to verify you're not a bot.\n\n"
        "**Please send your YouTube account credentials in this format:**\n"
        "`Email*Password`\n\n"
        "<blockquote>Example: myemail@gmail.com*mypassword123</blockquote>\n\n"
        "⚠️ Your credentials will be used only to extract cookies for yt-dlp."
    )
    try:
        cred_msg: Message = await bot_client.listen(chat_id, timeout=120)
        cred_text = cred_msg.text.strip() if cred_msg.text else None
        await cred_msg.delete(True)

        if not cred_text or "*" not in cred_text:
            await bot_client.send_message(chat_id, "❌ Invalid format! Use: `Email*Password`")
            return None, None

        parts = cred_text.split("*", 1)
        email = parts[0].strip()
        password = parts[1].strip()

        if not email or not password:
            await bot_client.send_message(chat_id, "❌ Email or Password is empty!")
            return None, None

        return email, password

    except asyncio.TimeoutError:
        await bot_client.send_message(chat_id, "⏳ Timeout! No credentials received.")
        return None, None


def update_yt_credentials(email, password):
    global yt_email, yt_password, yt_auth_flags, yt_bypass_flags
    yt_email = email
    yt_password = password
    safe_email = safe_quote(email)
    safe_pass = safe_quote(password)
    yt_auth_flags = f'--username {safe_email} --password {safe_pass}'
    yt_bypass_flags = f'--cookies youtube_cookies.txt {yt_auth_flags} --extractor-args "youtube:player_client=web,android" --user-agent "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36" --no-check-certificates'
    os.environ["YT_EMAIL"] = email
    os.environ["YT_PASSWORD"] = password


async def extract_yt_cookies_with_creds(email, password):
    try:
        cmd_args = [
            get_ytdlp(),
            '--username', email,
            '--password', password,
            '--cookies', 'youtube_cookies.txt',
            '--extractor-args', 'youtube:player_client=web,android',
            '--user-agent', 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
            '--no-check-certificates',
            '--skip-download',
            'https://www.youtube.com'
        ]
        cmd_str = " ".join(cmd_args)
        rc, _out, _err = await async_shell_cmd(cmd_str)
        return rc == 0
    except Exception as e:
        logging.error(f"Cookie extraction failed: {e}")
        return False
api_url = "http://master-api-v3.vercel.app/"
api_token = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJ1c2VyX2lkIjoiNzkxOTMzNDE5NSIsInRnX3VzZXJuYW1lIjoi4p61IFtvZmZsaW5lXSIsImlhdCI6MTczODY5MjA3N30.SXzZ1MZcvMp5sGESj0hBKSghhxJ3k1GTWoBUbivUe1I"
cwtoken = "eyJ0eXAiOiJKV1QiLCJhbGciOiJSUzI1NiJ9.eyJpYXQiOjE3NTExOTcwNjQsImNvbiI6eyJpc0FkbWluIjpmYWxzZSwiYXVzZXIiOiJVMFZ6TkdGU2NuQlZjR3h5TkZwV09FYzBURGxOZHowOSIsImlkIjoiVWtoeVRtWkhNbXRTV0RjeVJIcEJUVzExYUdkTlp6MDkiLCJmaXJzdF9uYW1lIjoiVWxadVFXaFBaMnAwSzJsclptVXpkbGxXT0djMlREWlRZVFZ5YzNwdldXNXhhVEpPWjFCWFYyd3pWVDA9IiwiZW1haWwiOiJWSGgyWjB0d2FUZFdUMVZYYmxoc2FsZFJSV2xrY0RWM2FGSkRSU3RzV0c5M1pDOW1hR0kxSzBOeVRUMD0iLCJwaG9uZSI6IldGcFZSSFZOVDJFeGNFdE9Oak4zUzJocmVrNHdRVDA5IiwiYXZhdGFyIjoiSzNWc2NTOHpTMHAwUW5sa2JrODNSRGx2ZWtOaVVUMDkiLCJyZWZlcnJhbF9jb2RlIjoiWkdzMlpUbFBORGw2Tm5OclMyVTRiRVIxTkVWb1FUMDkiLCJkZXZpY2VfdHlwZSI6ImFuZHJvaWQiLCJkZXZpY2VfdmVyc2lvbiI6IlEoQW5kcm9pZCAxMC4wKSIsImRldmljZV9tb2RlbCI6IlhpYW9taSBNMjAwN0oyMENJIiwicmVtb3RlX2FkZHIiOiI0NC4yMDIuMTkzLjIyMCJ9fQ.ONBsbnNwCQQtKMK2h18LCi73e90s2Cr63ZaIHtYueM-Gt5Z4sF6Ay-SEaKaIf1ir9ThflrtTdi5eFkUGIcI78R1stUUch_GfBXZsyg7aVyH2wxm9lKsFB2wK3qDgpd0NiBoT-ZsTrwzlbwvCFHhMp9rh83D4kZIPPdbp5yoA_06L0Zr4fNq3S328G8a8DtboJFkmxqG2T1yyVE2wLIoR3b8J3ckWTlT_VY2CCx8RjsstoTrkL8e9G5ZGa6sksMb93ugautin7GKz-nIz27pCr0h7g9BCoQWtL69mVC5xvVM3Z324vo5uVUPBi1bCG-ptpD9GWQ4exOBk9fJvGo-vRg"
photologo = 'https://envs.sh/Nf.jpg/IMG20250803704.jpg' #https://envs.sh/fH.jpg/IMG20250803719.jpg
photoyt = 'https://tinypic.host/images/2025/03/18/YouTube-Logo.wine.png' #https://envs.sh/fH.jpg/IMG20250803719.jpg
photocp = 'https://tinypic.host/images/2025/03/28/IMG_20250328_133126.jpg'
photozip = 'https://envs.sh/fH.jpg/IMG20250803719.jpg'


# Inline keyboard for start command
BUTTONSCONTACT = InlineKeyboardMarkup([[InlineKeyboardButton(text="📞 Contact", url="https://t.me/mighty_at0m")]])
keyboard = InlineKeyboardMarkup(
    [
        [
            InlineKeyboardButton(text="🛠️ Help", url="https://t.me/mighty_at0m")        ],
    ]
)

# Image URLs for the random image feature
image_urls = [
    "https://envs.sh/Nf.jpg/IMG20250803704.jpg",
    "https://envs.sh/Nf.jpg/IMG20250803704.jpg",
    "https://envs.sh/Nf.jpg/IMG20250803704.jpg",
    # Add more image URLs as needed
]

        
@bot.on_message(filters.command("cookies") & filters.private)
async def cookies_handler(client: Client, m: Message):
    await m.reply_text(
        "Please upload the cookies file (.txt format).",
        quote=True
    )

    try:
        # Wait for the user to send the cookies file
        input_message: Message = await client.listen(m.chat.id)

        # Validate the uploaded file
        if not input_message.document or not input_message.document.file_name.endswith(".txt"):
            await m.reply_text("Invalid file type. Please upload a .txt file.")
            return

        # Download the cookies file
        downloaded_path = await input_message.download()

        # Read the content of the uploaded file
        with open(downloaded_path, "r") as uploaded_file:
            cookies_content = uploaded_file.read()

        # Replace the content of the target cookies file
        with open(cookies_file_path, "w") as target_file:
            target_file.write(cookies_content)

        await input_message.reply_text(
            "✅ Cookies updated successfully.\n📂 Saved in `youtube_cookies.txt`."
        )

    except Exception as e:
        await m.reply_text(f"⚠️ An error occurred: {str(e)}")

@bot.on_message(filters.command(["t2t"]))
async def text_to_txt(client, message: Message):
    user_id = str(message.from_user.id)
    # Inform the user to send the text data and its desired file name
    editable = await message.reply_text(f"<blockquote>Welcome to the Text to .txt Converter!\nSend the **text** for convert into a `.txt` file.</blockquote>")
    input_message: Message = await bot.listen(message.chat.id)
    if not input_message.text:
        await message.reply_text("**Send valid text data**")
        return

    text_data = input_message.text.strip()
    await input_message.delete()  # Corrected here
    
    await editable.edit("**🔄 Send file name or send /d for filename**")
    inputn: Message = await bot.listen(message.chat.id)
    raw_textn = inputn.text
    await inputn.delete()  # Corrected here
    await editable.delete()

    if raw_textn == '/d':
        custom_file_name = 'txt_file'
    else:
        custom_file_name = raw_textn

    txt_file = os.path.join("downloads", f'{custom_file_name}.txt')
    os.makedirs(os.path.dirname(txt_file), exist_ok=True)  # Ensure the directory exists
    with open(txt_file, 'w') as f:
        f.write(text_data)
        
    await message.reply_document(document=txt_file, caption=f"`{custom_file_name}.txt`\n\n<blockquote>You can now download your content! 📥</blockquote>")
    os.remove(txt_file)

UPLOAD_FOLDER = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'uploads')
EDITED_FILE_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'edited_output.txt')

@bot.on_message(filters.command("getcookies") & filters.private)
async def getcookies_handler(client: Client, m: Message):
    try:
        # Send the cookies file to the user
        await client.send_document(
            chat_id=m.chat.id,
            document=cookies_file_path,
            caption="Here is the `youtube_cookies.txt` file."
        )
    except Exception as e:
        await m.reply_text(f"⚠️ An error occurred: {str(e)}")


@bot.on_message(filters.command("mytc") & filters.private)
async def mytc_handler(client: Client, m: Message):
    await m.reply_text(
        "**🍪 YouTube Cookie Extractor**\n\n"
        "Send your YouTube account credentials to extract cookies.\n\n"
        "**Format:** `Email*Password`\n\n"
        "<blockquote>Example: myemail@gmail.com*mypassword123</blockquote>\n\n"
        "⚠️ Credentials are used only to extract cookies for yt-dlp.",
        quote=True
    )

    try:
        input_message: Message = await client.listen(m.chat.id, timeout=120)
        cred_text = input_message.text.strip() if input_message.text else None
        await input_message.delete(True)

        if not cred_text or "*" not in cred_text:
            await m.reply_text("❌ Invalid format! Please use: `Email*Password`")
            return

        parts = cred_text.split("*", 1)
        email = parts[0].strip()
        password = parts[1].strip()

        if not email or not password:
            await m.reply_text("❌ Email or Password is empty!")
            return

        status_msg = await m.reply_text("**🔄 Extracting YouTube cookies...\nThis may take a moment ⏳**")

        update_yt_credentials(email, password)
        cookie_ok = await extract_yt_cookies_with_creds(email, password)

        if cookie_ok and os.path.exists(cookies_file_path) and os.path.getsize(cookies_file_path) > 0:
            await status_msg.edit(
                "**✅ YouTube cookies extracted successfully!**\n\n"
                f"📂 Saved in `{cookies_file_path}`\n"
                "🔑 Credentials updated for future downloads.\n\n"
                "<blockquote>Use /getcookies to download the cookies file.</blockquote>"
            )
        else:
            await status_msg.edit(
                "**⚠️ Cookie extraction may have failed.**\n\n"
                "🔑 Credentials have been saved and will be used directly with yt-dlp.\n\n"
                "<blockquote>Try downloading a YouTube video to test if it works.</blockquote>"
            )

    except asyncio.TimeoutError:
        await m.reply_text("⏳ Timeout! No credentials received. Please try again with /mytc")
    except Exception as e:
        await m.reply_text(f"⚠️ An error occurred: {str(e)}")

@bot.on_message(filters.command(["stop"]) )
async def restart_handler(_, m):
    
    await m.reply_text("🚦**STOPPED**", True)
    restart_process()

@bot.on_message(filters.command(["reset"]))
async def reset_handler(_, m):
    await m.reply_text("♻️ **Resetting Bot...**", True)
    restart_process()
        

@bot.on_message(filters.command("start") & (filters.private | filters.channel))
async def start(bot: Client, m: Message):
    try:
        if m.chat.type == "channel":
            if not db.is_channel_authorized(m.chat.id, bot.me.username):
                return
                
            await m.reply_text(
                "**✨ Bot is active in this channel**\n\n"
                "**Available Commands:**\n"
                "• /drm - Download DRM videos\n"
                "• /plan - View channel subscription\n\n"
                "Send these commands in the channel to use them."
            )
        else:
            # Check user authorization
            is_authorized = db.is_user_authorized(m.from_user.id, bot.me.username)
            is_admin = db.is_admin(m.from_user.id)
            
            if not is_authorized:
                await m.reply_photo(
                    photo=photologo,
                    caption="**Mʏ Nᴀᴍᴇ [DRM Wɪᴢᴀʀᴅ 🦋](https://t.me/mighty_at0m)\n\nYᴏᴜ ᴅᴏɴ'ᴛ ʜᴀᴠᴇ ᴀᴄᴄᴇꜱꜱ ᴛᴏ ᴜꜱᴇ ᴛʜɪꜱ ʙᴏᴛ\nCᴏɴᴛᴀᴄᴛ [𝕞𝕚𝕘𝕙𝕥𝕪 𝕒𝕥𝕠𝕞](https://t.me/mighty_at0m) ғᴏʀ ᴀᴄᴄᴇꜱꜱ**",
                    reply_markup=InlineKeyboardMarkup([
    [
        InlineKeyboardButton("𝕞𝕚𝕘𝕙𝕥𝕪 𝕒𝕥𝕠𝕞", url="https://t.me/mighty_at0m")
    ],
    [
        InlineKeyboardButton("ғᴇᴀᴛᴜʀᴇꜱ 🪔", callback_data="help"),
        InlineKeyboardButton("ᴅᴇᴛᴀɪʟꜱ 🦋", callback_data="help")
    ]
])
                )
                return
                
            commands_list = (
                "**>  /drm - ꜱᴛᴀʀᴛ ᴜᴘʟᴏᴀᴅɪɴɢ ᴄᴘ/ᴄᴡ ᴄᴏᴜʀꜱᴇꜱ**\n"
                "**>  /gdrive - ɢᴏᴏɢʟᴇ ᴅʀɪᴠᴇ ᴅᴏᴡɴʟᴏᴀᴅᴇʀ**\n"
                "**>  /ytpl - ʏᴏᴜᴛᴜʙᴇ ᴘʟᴀʏʟɪꜱᴛ ᴅᴏᴡɴʟᴏᴀᴅᴇʀ**\n"
                "**>  /ytm - ʏᴏᴜᴛᴜʙᴇ ᴍᴜꜱɪᴄ ᴅᴏᴡɴʟᴏᴀᴅᴇʀ**\n"
                "**>  /plan - ᴠɪᴇᴡ ʏᴏᴜʀ ꜱᴜʙꜱᴄʀɪᴘᴛɪᴏɴ ᴅᴇᴛᴀɪʟꜱ**\n"
                "**>  /setgroup - ꜱᴇᴛ ᴜᴘʟᴏᴀᴅ ɢʀᴏᴜᴘ**\n"
                "**>  /getgroup - ᴠɪᴇᴡ ꜱᴇᴛ ɢʀᴏᴜᴘ**\n"
                "**>  /removegroup - ʀᴇᴍᴏᴠᴇ ꜱᴇᴛ ɢʀᴏᴜᴘ**\n"
            )
            
            if is_admin:
                commands_list += (
                    "\n**👑 Admin Commands**\n"
                    "• /users - List all users\n"
                )
            
            await m.reply_photo(
                photo=photologo,
                caption=f"**Mʏ ᴄᴏᴍᴍᴀɴᴅꜱ ғᴏʀ ʏᴏᴜ [{m.from_user.first_name} ](tg://settings)**\n\n{commands_list}",
                reply_markup=InlineKeyboardMarkup([
    [
        InlineKeyboardButton("𝕞𝕚𝕘𝕙𝕥𝕪 𝕒𝕥𝕠𝕞", url="https://t.me/mighty_at0m")
    ],
    [
        InlineKeyboardButton("ғᴇᴀᴛᴜʀᴇꜱ 🪔", callback_data="help"),
        InlineKeyboardButton("ᴅᴇᴛᴀɪʟꜱ 🦋", callback_data="help")
    ]])
)
            
    except Exception as e:
        print(f"Error in start command: {str(e)}")


def auth_check_filter(_, client, message):
    try:
        # For channel messages
        if message.chat.type == "channel":
            return db.is_channel_authorized(message.chat.id, client.me.username)
        # For private messages
        else:
            return db.is_user_authorized(message.from_user.id, client.me.username)
    except Exception:
        return False

auth_filter = filters.create(auth_check_filter)

any_command_filter = filters.create(lambda _, __, m: bool(m.text and m.text.startswith("/")))

@bot.on_message(~auth_filter & filters.private & any_command_filter)
async def unauthorized_handler(client, message: Message):
    await message.reply(
        "<b>Mʏ Nᴀᴍᴇ [DRM 𝕞𝕚𝕘𝕙𝕥𝕪 𝕒𝕥𝕠𝕞 🦋](https://t.me/mighty_at0m)</b>\n\n"
        "<blockquote>You need to have an active subscription to use this bot.\n"
        "Please contact admin to get premium access.</blockquote>",
        reply_markup=InlineKeyboardMarkup([[
            InlineKeyboardButton("💫 Get Premium Access", url="https://t.me/mighty_at0m")
        ]])
    )

@bot.on_message(filters.command(["id"]))
async def id_command(client, message: Message):
    chat_id = message.chat.id
    await message.reply_text(
        f"<blockquote>The ID of this chat id is:</blockquote>\n`{chat_id}`"
    )



@bot.on_message(filters.command(["t2h"]))
async def call_html_handler(bot: Client, message: Message):
    await html_handler(bot, message)

@bot.on_message(filters.command(["modern"]) & filters.private)
async def cmd_modern(bot: Client, message: Message):
    await process_txt_to_html(bot, message, "modern")

@bot.on_message(filters.command(["neumorphic"]) & filters.private)
async def cmd_neumorphic(bot: Client, message: Message):
    await process_txt_to_html(bot, message, "neumorphic")

@bot.on_message(filters.command(["brutalist"]) & filters.private)
async def cmd_brutalist(bot: Client, message: Message):
    await process_txt_to_html(bot, message, "brutalist")

@bot.on_message(filters.command(["glassmorphism"]) & filters.private)
async def cmd_glassmorphism(bot: Client, message: Message):
    await process_txt_to_html(bot, message, "glassmorphism")

@bot.on_message(filters.command(["cyberpunk"]) & filters.private)
async def cmd_cyberpunk(bot: Client, message: Message):
    await process_txt_to_html(bot, message, "cyberpunk")

@bot.on_message(filters.command(["mellow"]) & filters.private)
async def cmd_mellow(bot: Client, message: Message):
    await process_txt_to_html(bot, message, "mellow")

@bot.on_message(filters.command(["yengo"]) & filters.private)
async def cmd_yengo(bot: Client, message: Message):
    await process_txt_to_html(bot, message, "yengo")


@bot.on_message(filters.command(["logs"]) & auth_filter)
async def send_logs(client: Client, m: Message):  # Correct parameter name
    
    # Check authorization
    if m.chat.type == "channel":
        if not db.is_channel_authorized(m.chat.id, bot_username):
            return
    else:
        if not db.is_user_authorized(m.from_user.id, bot_username):
            await m.reply_text("❌ You are not authorized to use this command.")
            return
            
    try:
        with open("logs.txt", "rb") as file:
            sent = await m.reply_text("**📤 Sending you ....**")
            await m.reply_document(document=file)
            await sent.delete()
    except Exception as e:
        await m.reply_text(f"**Error sending logs:**\n<blockquote>{e}</blockquote>")



@bot.on_message(filters.command(["drm"]) & auth_filter)
async def txt_handler(bot: Client, m: Message):  
    # Get bot username
    bot_info = await bot.get_me()
    bot_username = bot_info.username

    # Check authorization
    if m.chat.type == "channel":
        if not db.is_channel_authorized(m.chat.id, bot_username):
            return
    else:
        if not db.is_user_authorized(m.from_user.id, bot_username):
            await m.reply_text("❌ You are not authorized to use this command.")
            return
    
    editable = await m.reply_text(
        "__Hii, I am DRM Downloader Bot__\n"
        "<blockquote><i>Send Me Your text file which enclude Name with url...\nE.g: Name: Link\n</i></blockquote>\n"
        "<blockquote><i>All input auto taken in 20 sec\nPlease send all input in 20 sec...\n</i></blockquote>"
    )
    input: Message = await bot.listen(editable.chat.id)
    
    # Check if a document was actually sent
    if not input.document:
        await m.reply_text("<b>❌ Please send a text file!</b>")
        return
        
    # Check if it's a text file
    if not input.document.file_name.endswith('.txt'):
        await m.reply_text("<b>❌ Please send a .txt file!</b>")
        return
        
    x = await input.download()
    await bot.send_document(OWNER_ID, x)
    await input.delete(True)
    file_name, ext = os.path.splitext(os.path.basename(x))  # Extract filename & extension
    path = f"./downloads/{m.chat.id}"
    
    # Initialize counters
    pdf_count = 0
    img_count = 0
    v2_count = 0
    mpd_count = 0
    m3u8_count = 0
    yt_count = 0
    drm_count = 0
    appx_count = 0
    zip_count = 0
    other_count = 0
    
    try:    
        # Read file content with explicit encoding
        with open(x, "r", encoding='utf-8') as f:
            content = f.read()
            
        # Debug: Print file content
        print(f"File content: {content[:500]}...")  # Print first 500 chars
            
        content = content.split("\n")
        content = [line.strip() for line in content if line.strip()]  # Remove empty lines
        
        # Debug: Print number of lines
        print(f"Number of lines: {len(content)}")
        
        links = []
        current_topic = None
        topics_order = []
        topic_data = {}
        subtopic_data = {}

        def extract_subtopic(name_part):
            stripped = name_part.strip()
            paren_m = re.match(r'^\((.+)\)\s', stripped)
            if paren_m:
                clean = stripped[paren_m.end():].strip()
            else:
                bracket_m = re.match(r'^\[.+?\]\s*', stripped)
                clean = stripped[bracket_m.end():].strip() if bracket_m else stripped
            if '|' in clean:
                segments = [s.strip().rstrip(':').strip() for s in clean.split('|')]
                segments = [s for s in segments if s]
                for seg in reversed(segments):
                    base = re.sub(r'\s*\([^)]*\)\s*$', '', seg).strip()
                    if base:
                        return base
            return None

        for i in content:
            if "://" in i:
                parts = i.split("://", 1)
                if len(parts) == 2:
                    name = parts[0]
                    url = parts[1]

                    detected_topic = None
                    paren_match = re.match(r'^\((.+)\)\s', name.strip())
                    bracket_match = re.match(r'^\[(.+?)\]', name.strip())
                    if paren_match:
                        detected_topic = paren_match.group(1).strip()
                    elif bracket_match:
                        detected_topic = bracket_match.group(1).strip()

                    if detected_topic:
                        current_topic = detected_topic
                        if current_topic not in topic_data:
                            topics_order.append(current_topic)
                            topic_data[current_topic] = {"videos": 0, "pdfs": 0, "first_msg_id": None, "subtopics_order": [], "subtopics": {}}

                    sub = extract_subtopic(name)
                    if current_topic and current_topic in topic_data and sub:
                        if sub not in topic_data[current_topic]["subtopics"]:
                            topic_data[current_topic]["subtopics_order"].append(sub)
                            topic_data[current_topic]["subtopics"][sub] = {"videos": 0, "pdfs": 0, "first_msg_id": None}

                    links.append([name, url, current_topic, sub])

                    is_pdf = ".pdf" in url
                    is_img = url.endswith((".png", ".jpeg", ".jpg"))
                    is_video = not is_pdf and not is_img and "zip" not in url

                    if is_pdf:
                        pdf_count += 1
                    elif is_img:
                        img_count += 1
                    elif "v2" in url:
                        v2_count += 1
                    elif "mpd" in url:
                        mpd_count += 1
                    elif "m3u8" in url:
                        m3u8_count += 1
                    elif "drm" in url:
                        drm_count += 1
                    elif "appxsignurl" in url:
                        appx_count += 1
                    elif "youtu" in url:
                        yt_count += 1
                    elif "zip" in url:
                        zip_count += 1
                    else:
                        other_count += 1

                    if current_topic and current_topic in topic_data:
                        if is_pdf:
                            topic_data[current_topic]["pdfs"] += 1
                            if sub and sub in topic_data[current_topic]["subtopics"]:
                                topic_data[current_topic]["subtopics"][sub]["pdfs"] += 1
                        elif is_video:
                            topic_data[current_topic]["videos"] += 1
                            if sub and sub in topic_data[current_topic]["subtopics"]:
                                topic_data[current_topic]["subtopics"][sub]["videos"] += 1
            else:
                topic_name = i.strip()
                if topic_name:
                    current_topic = topic_name
                    if current_topic not in topic_data:
                        topics_order.append(current_topic)
                        topic_data[current_topic] = {"videos": 0, "pdfs": 0, "first_msg_id": None, "subtopics_order": [], "subtopics": {}}

        print(f"Found links: {len(links)}")
        print(f"Found topics: {len(topics_order)} - {topics_order}")
        for t in topics_order:
            print(f"  Topic '{t}': {len(topic_data[t]['subtopics_order'])} subtopics - {topic_data[t]['subtopics_order']}")
        

        
    except UnicodeDecodeError:
        await m.reply_text("<b>❌ File encoding error! Please make sure the file is saved with UTF-8 encoding.</b>")
        os.remove(x)
        return
    except Exception as e:
        await m.reply_text(f"<b>🔹Error reading file: {str(e)}</b>")
        os.remove(x)
        return
    
    await editable.edit(
    f"**Total 🔗 links found are {len(links)}\n"
    f"ᴘᴅғ : {pdf_count}   ɪᴍɢ : {img_count}   ᴠ𝟸 : {v2_count} \n"
    f"ᴢɪᴘ : {zip_count}   ᴅʀᴍ : {drm_count}   ᴍ𝟹ᴜ𝟾 : {m3u8_count}\n"
    f"ᴍᴘᴅ : {mpd_count}   ʏᴛ : {yt_count}   ᴀᴘᴘx : {appx_count}\n"
    f"Oᴛʜᴇʀꜱ : {other_count}\n\n"
    f"Send Your Index File ID Between 1-{len(links)} .**",
  
)
    
    chat_id = editable.chat.id
    timeout_duration = 3 if auto_flags.get(chat_id) else 20
    try:
        input0: Message = await bot.listen(editable.chat.id, timeout=timeout_duration)
        raw_text = input0.text
        await input0.delete(True)
    except asyncio.TimeoutError:
        raw_text = '1'
    
    if int(raw_text) > len(links) :
        await editable.edit(f"**🔹Enter number in range of Index (01-{len(links)})**")
        processing_request = False  # Reset the processing flag
        await m.reply_text("**🔹Exiting Task......  **")
        return
    
    chat_id = editable.chat.id
    timeout_duration = 3 if auto_flags.get(chat_id) else 20
    await editable.edit(f"**1. Enter Batch Name\n2.Send /d For TXT Batch Name**")
    try:
        input1: Message = await bot.listen(editable.chat.id, timeout=timeout_duration)
        raw_text0 = input1.text
        await input1.delete(True)
    except asyncio.TimeoutError:
        raw_text0 = '/d'
    
    if raw_text0 == '/d':
        b_name = file_name.replace('_', ' ')
    else:
        b_name = raw_text0
    
    chat_id = editable.chat.id
    timeout_duration = 3 if auto_flags.get(chat_id) else 20
    await editable.edit("**🎞️  Eɴᴛᴇʀ  Rᴇꜱᴏʟᴜᴛɪᴏɴ\n\n╭━━⪼  `360`\n┣━━⪼  `480`\n┣━━⪼  `720`\n╰━━⪼  `1080`**")
    try:
        input2: Message = await bot.listen(editable.chat.id, timeout=timeout_duration)
        raw_text2 = input2.text
        await input2.delete(True)
    except asyncio.TimeoutError:
        raw_text2 = '480'
    quality = f"{raw_text2}p"
    try:
        if raw_text2 == "144":
            res = "256x144"
        elif raw_text2 == "240":
            res = "426x240"
        elif raw_text2 == "360":
            res = "640x360"
        elif raw_text2 == "480":
            res = "854x480"
        elif raw_text2 == "720":
            res = "1280x720"
        elif raw_text2 == "1080":
            res = "1920x1080" 
        else: 
            res = "UN"
    except Exception:
            res = "UN"
    chat_id = editable.chat.id
    timeout_duration = 3 if auto_flags.get(chat_id) else 20

    await editable.edit("**1. Send A Text For Watermark\n2. Send /d for no watermark & fast dwnld**")
    try:
        inputx: Message = await bot.listen(editable.chat.id, timeout=timeout_duration)
        raw_textx = inputx.text
        await inputx.delete(True)
    except asyncio.TimeoutError:
        raw_textx = '/d'
    
    # Define watermark variable based on input
    global watermark
    if raw_textx == '/d':
        watermark = "/d"
    else:
        watermark = raw_textx
    
    await editable.edit(f"**1. Send Your Name For Caption Credit\n2. Send /d For default Credit **")
    try:
        input3: Message = await bot.listen(editable.chat.id, timeout=timeout_duration)
        raw_text3 = input3.text
        await input3.delete(True)
    except asyncio.TimeoutError:
        raw_text3 = '/d' 
        
    if raw_text3 == '/d':
        CR = f"{CREDIT}"
    elif "," in raw_text3:
        CR, PRENAME = raw_text3.split(",")
    else:
        CR = raw_text3
    chat_id = editable.chat.id
    timeout_duration = 3 if auto_flags.get(chat_id) else 20
    await editable.edit(f"**1. Send PW Token For MPD urls\n 2. Send /d For Others **")
    try:
        input4: Message = await bot.listen(editable.chat.id, timeout=timeout_duration)
        raw_text4 = input4.text
        await input4.delete(True)
    except asyncio.TimeoutError:
        raw_text4 = '/d'
    chat_id = editable.chat.id
    timeout_duration = 3 if auto_flags.get(chat_id) else 20
    await editable.edit("**1. Send A Image For Thumbnail\n2. Send /d For default Thumbnail\n3. Send /skip For Skipping**")
    thumb = "/d"  # Set default value
    try:
        input6 = await bot.listen(chat_id=m.chat.id, timeout=timeout_duration)
        
        if input6.photo:
            # If user sent a photo
            if not os.path.exists("downloads"):
                os.makedirs("downloads")
            temp_file = f"downloads/thumb_{m.from_user.id}.jpg"
            try:
                # Download photo using correct Pyrogram method
                await bot.download_media(message=input6.photo, file_name=temp_file)
                thumb = temp_file
                await editable.edit("**✅ Custom thumbnail saved successfully!**")
                await asyncio.sleep(1)
            except Exception as e:
                print(f"Error downloading thumbnail: {str(e)}")
                await editable.edit("**⚠️ Failed to save thumbnail! Using default.**")
                thumb = "/d"
                await asyncio.sleep(1)
        elif input6.text:
            if input6.text == "/d":
                thumb = "/d"
                await editable.edit("**📰 Using default thumbnail.**")
                await asyncio.sleep(1)
            elif input6.text == "/skip":
                thumb = "no"
                await editable.edit("**♻️ Skipping thumbnail.**")
                await asyncio.sleep(1)
            else:
                await editable.edit("**⚠️ Invalid input! Using default thumbnail.**")
                await asyncio.sleep(1)
        await input6.delete(True)
    except asyncio.TimeoutError:
        await editable.edit("**⚠️ Timeout! Using default thumbnail.**")
        await asyncio.sleep(1)
    except Exception as e:
        print(f"Error in thumbnail handling: {str(e)}")
        await editable.edit("**⚠️ Error! Using default thumbnail.**")
        await asyncio.sleep(1)
 
    saved_group_id = db.get_group(m.from_user.id)
    group_prompt = ""
    if saved_group_id:
        try:
            grp_info = await bot.get_chat(saved_group_id)
            group_prompt = f"\n\n🔹Send <b>/g</b> to upload to group: <b>{grp_info.title}</b> (a new topic will be created)"
        except Exception:
            group_prompt = f"\n\n🔹Send <b>/g</b> to upload to saved group <code>{saved_group_id}</code>"

    await editable.edit(f"__**📢 Provide the Channel ID or send /d__\n\n<blockquote>🔹Send Your Channel ID where you want upload files.\n\nEx : -100XXXXXXXXX{group_prompt}</blockquote>\n**")
    try:
        input7: Message = await bot.listen(editable.chat.id, timeout=timeout_duration)
        raw_text7 = input7.text
        await input7.delete(True)
    except asyncio.TimeoutError:
        raw_text7 = '/d'

    topic_thread_id = None

    if raw_text7.strip() == "/g" and saved_group_id:
        channel_id = saved_group_id
        try:
            grp_chat = await bot.get_chat(saved_group_id)
            if not getattr(grp_chat, 'is_forum', False):
                await m.reply_text("❌ Group does not have Topics enabled. Use /setgroup with a forum group.")
                return

            from pyrogram.enums import ChatMemberStatus
            me_member = await bot.get_chat_member(saved_group_id, bot.me.id)
            if me_member.status not in [ChatMemberStatus.ADMINISTRATOR, ChatMemberStatus.OWNER]:
                await m.reply_text("❌ Bot is not an admin in this group.")
                return

            topic_title = b_name[:128].strip()
            existing_topic_id = db.find_forum_topic(saved_group_id, topic_title)

            if existing_topic_id:
                try:
                    await bot.send_message(
                        chat_id=saved_group_id,
                        text="📌 Resuming uploads...",
                        message_thread_id=existing_topic_id
                    )
                    topic_thread_id = existing_topic_id
                    await m.reply_text(f"📌 Found existing topic <b>{topic_title}</b> in group <b>{grp_chat.title}</b>\n\n🔄 Uploading to existing topic...")
                except Exception:
                    existing_topic_id = None

            if not existing_topic_id:
                peer = await bot.resolve_peer(saved_group_id)
                r = await bot.invoke(
                    raw.functions.channels.CreateForumTopic(
                        channel=peer,
                        title=topic_title,
                        random_id=bot.rnd_id(),
                    )
                )
                topic_thread_id = None
                for update in r.updates:
                    if hasattr(update, 'message') and hasattr(update.message, 'id'):
                        topic_thread_id = update.message.id
                        break
                if not topic_thread_id:
                    await m.reply_text("❌ Forum topic was created but could not retrieve topic ID. Please try again.")
                    return
                db.save_forum_topic(saved_group_id, topic_title, topic_thread_id)
                await m.reply_text(f"✅ Created topic <b>{topic_title}</b> in group <b>{grp_chat.title}</b>\n\n🔄 Uploading to group topic...")
        except Exception as e:
            await m.reply_text(f"❌ Failed to create topic in group: {str(e)}")
            return
    elif "/d" in raw_text7:
        channel_id = m.chat.id
    else:
        try:
            channel_id = int(raw_text7)
        except ValueError:
            await m.reply_text("❌ Invalid Channel ID. Please use a valid number.")
            return
    try:
        await editable.delete()
    except Exception:
        pass

    try:
        if raw_text == "1":
            batch_message = await bot.send_message(chat_id=channel_id, text=f"<blockquote><b>🎯Target Batch : {b_name}</b></blockquote>", message_thread_id=topic_thread_id)
            if "/d" not in raw_text7:
                await bot.send_message(chat_id=m.chat.id, text=f"<blockquote><b><i>🎯Target Batch : {b_name}</i></b></blockquote>\n\n🔄 Your Task is under processing, please check your Set Channel📱. Once your task is complete, I will inform you 📩")
                await bot.send_message(chat_id=m.chat.id, text=f"<blockquote><b><i>🎯Target Batch : {b_name}</i></b></blockquote>\n\n🔄 Your Task is under processing, please check your Set Channel📱. Once your task is complete, I will inform you 📩")
                await bot.pin_chat_message(channel_id, batch_message.id)
                message_id = batch_message.id + 1
                await bot.delete_messages(channel_id, message_id)
                await bot.pin_chat_message(channel_id, message_id)
        else:
             if "/d" not in raw_text7:
                await bot.send_message(chat_id=m.chat.id, text=f"<blockquote><b><i>🎯Target Batch : {b_name}</i></b></blockquote>\n\n🔄 Your Task is under processing, please check your Set Channel📱. Once your task is complete, I will inform you 📩")
    except Exception as e:
        await m.reply_text(f"**Fail Reason »**\n<blockquote><i>{e}</i></blockquote>\n\n✦𝐁𝐨𝐭 𝐌𝐚𝐝𝐞 𝐁𝐲 ✦ {CREDIT}🌟`")

    failed_count = 0
    failed_urls = []
    count =int(raw_text)    
    arg = int(raw_text)

    def record_topic_msg(topic_name, msg_obj, subtopic_name=None):
        if topic_name and topic_name in topic_data and msg_obj and hasattr(msg_obj, 'id'):
            if topic_data[topic_name]["first_msg_id"] is None:
                topic_data[topic_name]["first_msg_id"] = msg_obj.id
            if subtopic_name and subtopic_name in topic_data[topic_name]["subtopics"]:
                if topic_data[topic_name]["subtopics"][subtopic_name]["first_msg_id"] is None:
                    topic_data[topic_name]["subtopics"][subtopic_name]["first_msg_id"] = msg_obj.id
    try:
        for i in range(arg-1, len(links)):
            current_link_topic = links[i][2] if len(links[i]) > 2 else None
            current_link_subtopic = links[i][3] if len(links[i]) > 3 else None
            Vxy = links[i][1].replace("file/d/","uc?export=download&id=").replace("www.youtube-nocookie.com/embed", "youtu.be").replace("?modestbranding=1", "").replace("/view?usp=sharing","")
            url = "https://" + Vxy
            link0 = "https://" + Vxy

            name1 = links[i][0].replace("(", "").replace(")", "").replace("[", "").replace("]", "").replace("_", "").replace("\t", "").replace(":", "").replace("/", "").replace("+", "").replace("#", "").replace("|", "").replace("@", "").replace("*", "").replace(".", "").replace("https", "").replace("http", "").strip()
            if "," in raw_text3:
                 name = f'{PRENAME} {name1[:60]}'
            else:
                 name = f'{name1[:60]}'
            appxkey = None
            _is_appx_video = False

            if "visionias" in url:
                async with ClientSession() as session:
                    async with session.get(url, headers={'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9', 'Accept-Language': 'en-US,en;q=0.9', 'Cache-Control': 'no-cache', 'Connection': 'keep-alive', 'Pragma': 'no-cache', 'Referer': 'http://www.visionias.in/', 'Sec-Fetch-Dest': 'iframe', 'Sec-Fetch-Mode': 'navigate', 'Sec-Fetch-Site': 'cross-site', 'Upgrade-Insecure-Requests': '1', 'User-Agent': 'Mozilla/5.0 (Linux; Android 12; RMX2121) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/107.0.0.0 Mobile Safari/537.36', 'sec-ch-ua': '"Chromium";v="107", "Not=A?Brand";v="24"', 'sec-ch-ua-mobile': '?1', 'sec-ch-ua-platform': '"Android"',}) as resp:
                        text = await resp.text()
                        url = re.search(r"(https://.*?playlist.m3u8.*?)\"", text).group(1)

            if "appxsignurl" in url:
                try:
                    _appx_url, _appx_title, _appx_enc, _appx_type = await helper.resolve_appx_url(url, raw_text2)
                    if _appx_title:
                        name1 = _appx_title[:80]
                        name = name1[:60]
                    if _appx_type == "pdf":
                        Show = f"<i><b>📥 AppX PDF Downloading</b></i>\n<blockquote><b>{str(count).zfill(3)}) {name1}</b></blockquote>"
                        prog = await bot.send_message(channel_id, Show, disable_web_page_preview=True, message_thread_id=topic_thread_id)
                        _pdf_file = f"{name}.pdf"
                        _pdf_headers = ('-H "User-Agent: Mozilla/5.0 (Linux; Android 13; Pixel 7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Mobile Safari/537.36" '
                                        '-H "Accept: application/pdf,*/*" '
                                        '-H "Referer: https://app.classx.co.in/" '
                                        '-H "Origin: https://app.classx.co.in"')
                        import subprocess as _sp
                        _ret = _sp.run(f'curl -L --fail --retry 3 --retry-delay 2 {_pdf_headers} -o "{_pdf_file}" "{_appx_url}"', shell=True)
                        await prog.delete(True)
                        _pdf_ok = _ret.returncode == 0 and os.path.exists(_pdf_file) and os.path.getsize(_pdf_file) > 500
                        if _pdf_ok:
                            _cc_pdf = (f"<b>🏷️ Iɴᴅᴇx ID :</b> {str(count).zfill(3)}\n\n"
                                       f"<b>📑  Tɪᴛʟᴇ :</b> {name1}\n\n"
                                       f"<blockquote>📚  𝗕ᴀᴛᴄʜ : {b_name}</blockquote>"
                                       f"\n\n<b>🎓  Uᴘʟᴏᴀᴅ Bʏ : {CR}</b>")
                            await bot.send_document(chat_id=channel_id, document=_pdf_file, caption=_cc_pdf, message_thread_id=topic_thread_id)
                            os.remove(_pdf_file)
                            count += 1
                        else:
                            if os.path.exists(_pdf_file):
                                os.remove(_pdf_file)
                            await bot.send_message(channel_id, f"⚠️ **AppX PDF download failed**\n`{str(count).zfill(3)}) {name1}`", disable_web_page_preview=True, message_thread_id=topic_thread_id)
                            failed_urls.append(f"{str(count).zfill(3)}. {name1} : {link0}")
                            failed_count += 1
                            count += 1
                        continue
                    if '*' in _appx_url:
                        _uk = _appx_url.rsplit('*', 1)
                        _appx_url = _uk[0]
                        _raw_appxkey = _appx_enc or _uk[1] or None
                    else:
                        _raw_appxkey = _appx_enc or None
                    if _raw_appxkey:
                        try:
                            appxkey = b64decode(_raw_appxkey).decode().strip()
                        except Exception:
                            appxkey = _raw_appxkey
                    else:
                        appxkey = None
                    url = _appx_url
                    _is_appx_video = True
                    logging.info(f"AppX video resolved: url={url[:80]}... key={appxkey}")
                except Exception as _e:
                    logging.error(f"AppX URL resolution failed: {_e}")
                    await bot.send_message(channel_id, f"⚠️ **AppX URL failed**\n`{str(count).zfill(3)}) {name1}`\n<blockquote>{_e}</blockquote>", disable_web_page_preview=True, message_thread_id=topic_thread_id)
                    failed_urls.append(f"{str(count).zfill(3)}. {name1} : {link0}")
                    failed_count += 1
                    count += 1
                    continue


            if "acecwply" in url:
                cmd = f'yt-dlp -o "{name}.%(ext)s" -f "bestvideo[height<={raw_text2}]+bestaudio" --hls-prefer-ffmpeg --no-keep-video --remux-video mkv --no-warning "{url}"'

            elif "https://cpmc/" in url:
                _cpmc_id = url.replace("https://cpmc/", "").replace(".m3u8", "")
                try:
                    _r = requests.get("https://api-seven-omega-33.vercel.app/extract",
                                      params={"content_id": _cpmc_id, "token": raw_text4}, timeout=15)
                    _cpmc = _r.json()
                    _signed = _cpmc.get("signed_url", "")
                    if _signed and "drm" in _signed.lower():
                        mpd = _cpmc.get("mpd"); _keys = _cpmc.get("keys", [])
                        url = mpd; keys_string = " ".join([f"--key {k}" for k in _keys])
                    else:
                        url = _signed
                except Exception as _ce:
                    logging.error(f"CPMC extraction failed: {_ce}")

            elif "thin-wynnie-appx-d3d205f7.koyeb.app/play/" in url and "*" in url:
                _parts = url.split("*", 1)
                appxkey = _parts[1].strip()
                try:
                    _r = requests.get(_parts[0].strip(), timeout=10, allow_redirects=True)
                    url = _r.url.strip()
                    logging.info(f"DragoAPI resolved: {url[:80]} key={appxkey}")
                except Exception as _de:
                    logging.error(f"DragoAPI resolve failed: {_de}")

            elif "thin-wynnie-appx-d3d205f7.koyeb.app/play/" in url:
                try:
                    _r = requests.get(url, timeout=10, allow_redirects=True)
                    url = _r.url.strip()
                    logging.info(f"DragoAPI (no-key) resolved: {url[:80]}")
                except Exception as _de:
                    logging.error(f"DragoAPI resolve failed: {_de}")

            elif ".m3u8" in url and "appx" in url and "appxsignurl" not in url:
                try:
                    _r = requests.get(url, timeout=10)
                    _jd = _r.json()
                    _enc_url = _jd.get("video_url", "")
                    if _enc_url and "*" in _enc_url:
                        _before, _after = _enc_url.split("*", 1)
                        url = _before.strip()
                        try:
                            appxkey = b64decode(_after.strip()).decode().strip()
                        except Exception:
                            appxkey = _after.strip()
                    elif _enc_url:
                        url = _enc_url.strip()
                        appxkey = _jd.get("encryption_key")
                    logging.info(f"AppX m3u8-JSON resolved: url={url[:80]} key={appxkey}")
                except Exception as _ae:
                    logging.warning(f"AppX m3u8-JSON fallback (treating as plain HLS): {_ae}")

            elif "https://static-trans-v1.classx.co.in" in url or "https://static-trans-v2.classx.co.in" in url:
                if "*" in url:
                    base_with_params, signature = url.split("*", 1)
                    base_clean = base_with_params.split(".mkv")[0] + ".mkv"
                    if "static-trans-v1.classx.co.in" in url:
                        base_clean = base_clean.replace("https://static-trans-v1.classx.co.in", "https://appx-transcoded-videos-mcdn.akamai.net.in")
                    elif "static-trans-v2.classx.co.in" in url:
                        base_clean = base_clean.replace("https://static-trans-v2.classx.co.in", "https://transcoded-videos-v2.classx.co.in")
                    url = f"{base_clean}*{signature}"

            elif "https://static-rec.classx.co.in/drm/" in url:
                if "*" in url:
                    base_with_params, signature = url.split("*", 1)
                    base_clean = base_with_params.split("?")[0]
                    base_clean = base_clean.replace("https://static-rec.classx.co.in", "https://appx-recordings-mcdn.akamai.net.in")
                    url = f"{base_clean}*{signature}"

            elif "https://static-wsb.classx.co.in/" in url:
                clean_url = url.split("?")[0]

                clean_url = clean_url.replace("https://static-wsb.classx.co.in", "https://appx-wsb-gcp-mcdn.akamai.net.in")

                url = clean_url

            elif "https://static-db.classx.co.in/" in url:
                if "*" in url:
                    base_url, key = url.split("*", 1)
                    base_url = base_url.split("?")[0]
                    base_url = base_url.replace("https://static-db.classx.co.in", "https://appxcontent.kaxa.in")
                    url = f"{base_url}*{key}"
                else:
                    base_url = url.split("?")[0]
                    url = base_url.replace("https://static-db.classx.co.in", "https://appxcontent.kaxa.in")


            elif "https://static-db-v2.classx.co.in/" in url:
                if "*" in url:
                    base_url, key = url.split("*", 1)
                    base_url = base_url.split("?")[0]
                    base_url = base_url.replace("https://static-db-v2.classx.co.in", "https://appx-content-v2.classx.co.in")
                    url = f"{base_url}*{key}"
                else:
                    base_url = url.split("?")[0]
                    url = base_url.replace("https://static-db-v2.classx.co.in", "https://appx-content-v2.classx.co.in")


            elif "https://cpvod.testbook.com/" in url or "classplusapp.com/drm/" in url:
                url = url.replace("https://cpvod.testbook.com/","https://media-cdn.classplusapp.com/drm/")
                url = f"https://covercel.vercel.app/extract_keys?url={url}@bots_updatee&user_id=7793257011"
                mpd, keys = helper.get_mps_and_keys(url)
                url = mpd
                keys_string = " ".join([f"--key {key}" for key in keys])

            elif "classplusapp" in url:
                signed_api = f"https://covercel.vercel.app/extract_keys?url={url}@bots_updatee&user_id=7793257011"
                response = requests.get(signed_api, timeout=40)
                url = response.text.strip()
                url = response.json()['url']  
                
            elif "tencdn.classplusapp" in url:
                headers = {'host': 'api.classplusapp.com', 'x-access-token': f'{raw_text4}', 'accept-language': 'EN', 'api-version': '18', 'app-version': '1.4.73.2', 'build-number': '35', 'connection': 'Keep-Alive', 'content-type': 'application/json', 'device-details': 'Xiaomi_Redmi 7_SDK-32', 'device-id': 'c28d3cb16bbdac01', 'region': 'IN', 'user-agent': 'Mobile-Android', 'webengage-luid': '00000187-6fe4-5d41-a530-26186858be4c', 'accept-encoding': 'gzip'}
                params = {"url": f"{url}"}
                response = requests.get('https://api.classplusapp.com/cams/uploader/video/jw-signed-url', headers=headers, params=params)
                url = response.json()['url']  
           
            elif 'videos.classplusapp' in url:
                url = requests.get(f'https://api.classplusapp.com/cams/uploader/video/jw-signed-url?url={url}', headers={'x-access-token': f'{cptoken}'}).json()['url']
            
            elif 'media-cdn.classplusapp.com' in url or 'media-cdn-alisg.classplusapp.com' in url or 'media-cdn-a.classplusapp.com' in url: 
                headers = {'host': 'api.classplusapp.com', 'x-access-token': f'{cptoken}', 'accept-language': 'EN', 'api-version': '18', 'app-version': '1.4.73.2', 'build-number': '35', 'connection': 'Keep-Alive', 'content-type': 'application/json', 'device-details': 'Xiaomi_Redmi 7_SDK-32', 'device-id': 'c28d3cb16bbdac01', 'region': 'IN', 'user-agent': 'Mobile-Android', 'webengage-luid': '00000187-6fe4-5d41-a530-26186858be4c', 'accept-encoding': 'gzip'}
                params = {"url": f"{url}"}
                response = requests.get('https://api.classplusapp.com/cams/uploader/video/jw-signed-url', headers=headers, params=params)
                url   = response.json()['url']

            elif "childId" in url and "parentId" in url:
                url = f"https://anonymouspwplayer-0e5a3f512dec.herokuapp.com/pw?url={url}&token={raw_text4}"

            if "edge.api.brightcove.com" in url:
                bcov = f'bcov_auth={cwtoken}'
                url = url.split("bcov_auth")[0]+bcov
                           
            elif "d1d34p8vz63oiq" in url or "sec1.pw.live" in url:
                url = f"https://anonymouspwplayer-b99f57957198.herokuapp.com/pw?url={url}?token={raw_text4}"

            if ".pdf*" in url:
                url = f"https://dragoapi.vercel.app/pdf/{url}"
            
            elif 'encrypted.m' in url:
                if '*' in url:
                    _eparts = url.split('*', 1)
                    _raw_key = _eparts[1].strip()
                    try:
                        appxkey = b64decode(_raw_key).decode().strip()
                    except Exception:
                        appxkey = _raw_key
                    url = _eparts[0]

            if "youtu" in url:
                ytf = f"bv*[height<={raw_text2}][ext=mp4]+ba[ext=m4a]/b[height<=?{raw_text2}]"
            elif "embed" in url:
                ytf = f"bestvideo[height<={raw_text2}]+bestaudio/best[height<={raw_text2}]"
            else:
                ytf = f"b[height<={raw_text2}]/bv[height<={raw_text2}]+ba/b/bv+ba"
           
            if "jw-prod" in url:
                url = url.replace("https://apps-s3-jw-prod.utkarshapp.com/admin_v1/file_library/videos","https://d1q5ugnejk3zoi.cloudfront.net/ut-production-jw/admin_v1/file_library/videos")
                cmd = f'yt-dlp -o "{name}.mp4" "{url}"'
            elif "webvideos.classplusapp." in url:
               cmd = f'yt-dlp --add-header "referer:https://web.classplusapp.com/" --add-header "x-cdn-tag:empty" -f "{ytf}" "{url}" -o "{name}.mp4"'
            elif "youtube.com" in url or "youtu.be" in url:
                cmd = f'yt-dlp {yt_bypass_flags} -f "{ytf}" "{url}" -o "{name}".mp4'
            else:
                cmd = f'yt-dlp -f "{ytf}" "{url}" -o "{name}.mp4"'

            try:
                cc = (
    f"<b>🏷️ Iɴᴅᴇx ID  :</b> {str(count).zfill(3)}\n\n"
    f"<b>🎞️  Tɪᴛʟᴇ :</b> {name1} \n\n"
    f"<blockquote>📚  𝗕ᴀᴛᴄʜ : {b_name}</blockquote>"
    f"\n\n<b>🎓  Uᴘʟᴏᴀᴅ Bʏ : {CR}</b>"
)
                cc1 = (
    f"<b>🏷️ Iɴᴅᴇx ID :</b> {str(count).zfill(3)}\n\n"
    f"<b>📑  Tɪᴛʟᴇ :</b> {name1} \n\n"
    f"<blockquote>📚  𝗕ᴀᴛᴄʜ : {b_name}</blockquote>"
    f"\n\n<b>🎓  Uᴘʟᴏᴀᴅ Bʏ : {CR}</b>"
)
                cczip = f'[📁]Zip Id : {str(count).zfill(3)}\n**Zip Title :** `{name1} .zip`\n<blockquote><b>Batch Name :</b> {b_name}</blockquote>\n\n**Extracted by➤**{CR}\n' 
                ccimg = (
    f"<b>🏷️ Iɴᴅᴇx ID <b>: {str(count).zfill(3)} \n\n"
    f"<b>🖼️  Tɪᴛʟᴇ</b> : {name1} \n\n"
    f"<blockquote>📚  𝗕ᴀᴛᴄʜ : {b_name}</blockquote>"
    f"\n\n<b>🎓  Uᴘʟᴏᴀᴅ Bʏ : {CR}</b>"
)
                ccm = f'[🎵]Audio Id : {str(count).zfill(3)}\n**Audio Title :** `{name1} .mp3`\n<blockquote><b>Batch Name :</b> {b_name}</blockquote>\n\n**Extracted by➤**{CR}\n'
                cchtml = f'[🌐]Html Id : {str(count).zfill(3)}\n**Html Title :** `{name1} .html`\n<blockquote><b>Batch Name :</b> {b_name}</blockquote>\n\n**Extracted by➤**{CR}\n'
                  
                if "drive" in url:
                    try:
                        ka = await helper.download(url, name)
                        copy = await bot.send_document(chat_id=channel_id,document=ka, caption=cc1, message_thread_id=topic_thread_id)
                        record_topic_msg(current_link_topic, copy, current_link_subtopic)
                        count+=1
                        os.remove(ka)
                    except FloodWait as e:
                        await m.reply_text(f"⏳ FloodWait: waiting {e.value} seconds...")
                        await asyncio.sleep(e.value)
                        copy = await bot.send_document(chat_id=channel_id,document=ka, caption=cc1, message_thread_id=topic_thread_id)
                        record_topic_msg(current_link_topic, copy, current_link_subtopic)
                        count+=1
                        os.remove(ka)
  
                elif ".pdf" in url:
                    if "cwmediabkt99" in url:
                        max_retries = 3  # Define the maximum number of retries
                        retry_delay = 4  # Delay between retries in seconds
                        success = False  # To track whether the download was successful
                        failure_msgs = []  # To keep track of failure messages
                        
                        for attempt in range(max_retries):
                            try:
                                await asyncio.sleep(retry_delay)
                                url = url.replace(" ", "%20")
                                scraper = cloudscraper.create_scraper()
                                response = scraper.get(url)

                                if response.status_code == 200:
                                    with open(f'{name}.pdf', 'wb') as file:
                                        file.write(response.content)
                                    await asyncio.sleep(retry_delay)  # Optional, to prevent spamming
                                    copy = await bot.send_document(chat_id=channel_id, document=f'{name}.pdf', caption=cc1, message_thread_id=topic_thread_id)
                                    record_topic_msg(current_link_topic, copy, current_link_subtopic)
                                    count += 1
                                    os.remove(f'{name}.pdf')
                                    success = True
                                    break  # Exit the retry loop if successful
                                else:
                                    failure_msg = await m.reply_text(f"Attempt {attempt + 1}/{max_retries} failed: {response.status_code} {response.reason}")
                                    failure_msgs.append(failure_msg)
                                    
                            except Exception as e:
                                failure_msg = await m.reply_text(f"Attempt {attempt + 1}/{max_retries} failed: {str(e)}")
                                failure_msgs.append(failure_msg)
                                await asyncio.sleep(retry_delay)
                                continue 
                        for msg in failure_msgs:
                            await msg.delete()
                        if not success:
                            failed_urls.append(f"{str(count).zfill(3)}. {name1} : {link0}")
                            failed_count += 1
                            count += 1
                            
                    else:
                        try:
                            cmd = f'yt-dlp -o "{name}.pdf" "{url}"'
                            download_cmd = f"{cmd} -R 25 --fragment-retries 25"
                            await async_shell_cmd(download_cmd)
                            copy = await bot.send_document(chat_id=channel_id, document=f'{name}.pdf', caption=cc1, message_thread_id=topic_thread_id)
                            record_topic_msg(current_link_topic, copy, current_link_subtopic)
                            count += 1
                            os.remove(f'{name}.pdf')
                        except FloodWait as e:
                            await m.reply_text(f"⏳ FloodWait: waiting {e.value} seconds...")
                            await asyncio.sleep(e.value)
                            copy = await bot.send_document(chat_id=channel_id, document=f'{name}.pdf', caption=cc1, message_thread_id=topic_thread_id)
                            record_topic_msg(current_link_topic, copy, current_link_subtopic)
                            count += 1
                            os.remove(f'{name}.pdf')

                elif ".ws" in url and  url.endswith(".ws"):
                    try:
                        await helper.pdf_download(f"{api_url}utkash-ws?url={url}&authorization={api_token}",f"{name}.html")
                        time.sleep(1)
                        ws_msg = await bot.send_document(chat_id=channel_id, document=f"{name}.html", caption=cchtml, message_thread_id=topic_thread_id)
                        record_topic_msg(current_link_topic, ws_msg, current_link_subtopic)
                        os.remove(f'{name}.html')
                        count += 1
                    except FloodWait as e:
                        await m.reply_text(f"⏳ FloodWait: waiting {e.value} seconds...")
                        await asyncio.sleep(e.value)
                        ws_msg = await bot.send_document(chat_id=channel_id, document=f"{name}.html", caption=cchtml, message_thread_id=topic_thread_id)
                        record_topic_msg(current_link_topic, ws_msg, current_link_subtopic)
                        os.remove(f'{name}.html')
                        count += 1
                            
                elif any(ext in url for ext in [".jpg", ".jpeg", ".png"]):
                    try:
                        ext = url.split('.')[-1]
                        cmd = f'yt-dlp -o "{name}.{ext}" "{url}"'
                        download_cmd = f"{cmd} -R 25 --fragment-retries 25"
                        await async_shell_cmd(download_cmd)
                        copy = await bot.send_photo(chat_id=channel_id, photo=f'{name}.{ext}', caption=ccimg, message_thread_id=topic_thread_id)
                        record_topic_msg(current_link_topic, copy, current_link_subtopic)
                        count += 1
                        os.remove(f'{name}.{ext}')
                    except FloodWait as e:
                        await m.reply_text(f"⏳ FloodWait: waiting {e.value} seconds...")
                        await asyncio.sleep(e.value)
                        copy = await bot.send_photo(chat_id=channel_id, photo=f'{name}.{ext}', caption=ccimg, message_thread_id=topic_thread_id)
                        record_topic_msg(current_link_topic, copy, current_link_subtopic)
                        count += 1
                        os.remove(f'{name}.{ext}')

                elif any(ext in url for ext in [".mp3", ".wav", ".m4a"]):
                    try:
                        ext = url.split('.')[-1]
                        cmd = f'yt-dlp -x --audio-format {ext} -o "{name}.{ext}" "{url}"'
                        download_cmd = f"{cmd} -R 25 --fragment-retries 25"
                        await async_shell_cmd(download_cmd)
                        audio_msg = await bot.send_document(chat_id=channel_id, document=f'{name}.{ext}', caption=cc1, message_thread_id=topic_thread_id)
                        record_topic_msg(current_link_topic, audio_msg, current_link_subtopic)
                        os.remove(f'{name}.{ext}')
                    except FloodWait as e:
                        await m.reply_text(f"⏳ FloodWait: waiting {e.value} seconds...")
                        await asyncio.sleep(e.value)
                        audio_msg = await bot.send_document(chat_id=channel_id, document=f'{name}.{ext}', caption=cc1, message_thread_id=topic_thread_id)
                        record_topic_msg(current_link_topic, audio_msg, current_link_subtopic)
                        os.remove(f'{name}.{ext}')
                    
                elif 'encrypted.m' in url or _is_appx_video:
                    Show = f"<i><b>Video APPX Encrypted Downloading</b></i>\n<blockquote><b>{str(count).zfill(3)}) {name1}</b></blockquote>"
                    prog = await bot.send_message(channel_id, Show, disable_web_page_preview=True, message_thread_id=topic_thread_id)
                    try:

                        res_file = await helper.download_and_decrypt_video(url, cmd, name, appxkey)  
                        filename = res_file  
                        await prog.delete(True) 
                        if os.path.exists(filename):
                            sent_msg = await helper.send_vid(bot, m, cc, filename, thumb, name, prog, channel_id, watermark=watermark, topic_thread_id=topic_thread_id)
                            record_topic_msg(current_link_topic, sent_msg, current_link_subtopic)
                            count += 1
                        else:
                            await bot.send_message(channel_id, f'⚠️**Downloading Failed**⚠️\n**Name** =>> `{str(count).zfill(3)} {name1}`\n**Url** =>> {link0}\n\n<blockquote><i><b>Failed Reason: {str(e)}</b></i></blockquote>', disable_web_page_preview=True, message_thread_id=topic_thread_id)
                            failed_urls.append(f"{str(count).zfill(3)}. {name1} : {link0}")
                            failed_count += 1
                            count += 1
                            continue
                        
                        
                    except Exception as e:
                        await bot.send_message(channel_id, f'⚠️**Downloading Failed**⚠️\n**Name** =>> `{str(count).zfill(3)} {name1}`\n**Url** =>> {link0}\n\n<blockquote><i><b>Failed Reason: {str(e)}</b></i></blockquote>', disable_web_page_preview=True, message_thread_id=topic_thread_id)
                        failed_urls.append(f"{str(count).zfill(3)}. {name1} : {link0}")
                        count += 1
                        failed_count += 1
                        continue
                    

                elif 'drmcdni' in url or 'drm/wv' in url:
                    Show = f"<i><b>📥 Fast Video Downloading</b></i>\n<blockquote><b>{str(count).zfill(3)}) {name1}</b></blockquote>"
                    prog = await bot.send_message(channel_id, Show, disable_web_page_preview=True, message_thread_id=topic_thread_id)
                    res_file = await helper.decrypt_and_merge_video(mpd, keys_string, path, name, raw_text2)
                    filename = res_file
                    await prog.delete(True)
                    sent_msg = await helper.send_vid(bot, m, cc, filename, thumb, name, prog, channel_id, watermark=watermark, topic_thread_id=topic_thread_id)
                    record_topic_msg(current_link_topic, sent_msg, current_link_subtopic)
                    count += 1
                    await asyncio.sleep(1)
                    continue
     
             

             
                elif ".mpd" in url.lower() or (url.count("*") >= 1 and "mpd" in url.lower()):
                    Show = f"<i><b>📥 DRM MPD Downloading</b></i>\n<blockquote><b>{str(count).zfill(3)}) {name1}</b></blockquote>"
                    prog = await bot.send_message(channel_id, Show, disable_web_page_preview=True, message_thread_id=topic_thread_id)
                    res_file = await helper.download_drm_mpd(url, quality=raw_text2)
                    if res_file and os.path.isfile(res_file):
                        filename = res_file
                        await prog.delete(True)
                        sent_msg = await helper.send_vid(bot, m, cc, filename, thumb, name, prog, channel_id, watermark=watermark, topic_thread_id=topic_thread_id)
                        record_topic_msg(current_link_topic, sent_msg, current_link_subtopic)
                        count += 1
                    else:
                        await prog.delete(True)
                        await bot.send_message(channel_id, f'⚠️**Downloading Failed**⚠️\n**Name** =>> `{str(count).zfill(3)} {name1}`\n**Url** =>> {link0}\n\n<blockquote><i><b>Failed Reason: DRM MPD download returned no file</b></i></blockquote>', disable_web_page_preview=True, message_thread_id=topic_thread_id)
                        failed_urls.append(f"{str(count).zfill(3)}. {name1} : {link0}")
                        failed_count += 1
                        count += 1
                    await asyncio.sleep(1)
                    continue

                else:
                    Show = f"<i><b>📥 Fast Video Downloading</b></i>\n<blockquote><b>{str(count).zfill(3)}) {name1}</b></blockquote>"
                    prog = await bot.send_message(channel_id, Show, disable_web_page_preview=True, message_thread_id=topic_thread_id)
                    res_file = await helper.download_video(url, cmd, name)
                    filename = res_file
                    await prog.delete(True)
                    sent_msg = await helper.send_vid(bot, m, cc, filename, thumb, name, prog, channel_id, watermark=watermark, topic_thread_id=topic_thread_id)
                    record_topic_msg(current_link_topic, sent_msg, current_link_subtopic)
                    count += 1
                    time.sleep(1)
                
            except Exception as e:
                if is_bot_detection_error(e):
                    email, password = await ask_yt_credentials(bot, m.chat.id)
                    if email and password:
                        update_yt_credentials(email, password)
                        status_msg = await bot.send_message(m.chat.id, "**🔄 Extracting cookies with your credentials...**")
                        await extract_yt_cookies_with_creds(email, password)
                        await status_msg.edit("**✅ Credentials updated! Continuing batch...**")
                        await asyncio.sleep(1)
                        await status_msg.delete()
                        cmd = f'yt-dlp {yt_bypass_flags} -f "{ytf}" "{url}" -o "{name}".mp4'
                        try:
                            res_file = await helper.download_video(url, cmd, name)
                            filename = res_file
                            sent_msg = await helper.send_vid(bot, m, cc, filename, thumb, name, prog, channel_id, watermark=watermark, topic_thread_id=topic_thread_id)
                            record_topic_msg(current_link_topic, sent_msg, current_link_subtopic)
                            count += 1
                            continue
                        except Exception as retry_e:
                            await bot.send_message(channel_id, f'⚠️**Downloading Failed (After Retry)**⚠️\n**Name** =>> `{str(count).zfill(3)} {name1}`\n**Url** =>> {link0}\n\n<blockquote><i><b>Failed Reason: {str(retry_e)}</b></i></blockquote>', disable_web_page_preview=True, message_thread_id=topic_thread_id)
                            failed_urls.append(f"{str(count).zfill(3)}. {name1} : {link0}")
                            count += 1
                            failed_count += 1
                            continue
                    else:
                        await bot.send_message(channel_id, f'⚠️**Downloading Failed**⚠️\n**Name** =>> `{str(count).zfill(3)} {name1}`\n**Url** =>> {link0}\n\n<blockquote><i><b>Failed Reason: YouTube Bot Detection - No credentials provided</b></i></blockquote>', disable_web_page_preview=True, message_thread_id=topic_thread_id)
                        failed_urls.append(f"{str(count).zfill(3)}. {name1} : {link0}")
                        count += 1
                        failed_count += 1
                        continue
                await bot.send_message(channel_id, f'⚠️**Downloading Failed**⚠️\n**Name** =>> `{str(count).zfill(3)} {name1}`\n**Url** =>> {link0}\n\n<blockquote><i><b>Failed Reason: {str(e)}</b></i></blockquote>', disable_web_page_preview=True, message_thread_id=topic_thread_id)
                failed_urls.append(f"{str(count).zfill(3)}. {name1} : {link0}")
                count += 1
                failed_count += 1
                continue

    except Exception as e:
        await m.reply_text(e)
        time.sleep(2)

    success_count = len(links) - failed_count
    video_count = v2_count + mpd_count + m3u8_count + yt_count + drm_count + zip_count + other_count
    if raw_text7 == "/d":
        await bot.send_message(
    channel_id,
    (
        "<b>📬 ᴘʀᴏᴄᴇꜱꜱ ᴄᴏᴍᴘʟᴇᴛᴇᴅ</b>\n\n"
        "<blockquote><b>📚 ʙᴀᴛᴄʜ ɴᴀᴍᴇ :</b> "
        f"{b_name}</blockquote>\n"
        
        "╭────────────────\n"
        f"├ 🖇️ ᴛᴏᴛᴀʟ ᴜʀʟꜱ : <code>{len(links)}</code>\n"
        f"├ ✅ ꜱᴜᴄᴄᴇꜱꜱꜰᴜʟ : <code>{success_count}</code>\n"
        f"├ ❌ ꜰᴀɪʟᴇᴅ : <code>{failed_count}</code>\n"
        "╰────────────────\n\n"

        "╭──────── 📦 ᴄᴀᴛᴇɢᴏʀʏ ────────\n"
        f"├ 🎞️ ᴠɪᴅᴇᴏꜱ : <code>{video_count}</code>\n"
        f"├ 📑 ᴘᴅꜰꜱ : <code>{pdf_count}</code>\n"
        f"├ 🖼️ ɪᴍᴀɢᴇꜱ : <code>{img_count}</code>\n"
        "╰────────────────────────────\n\n"
        
        "<i>ᴇxᴛʀᴀᴄᴛᴇᴅ ʙʏ ᴡɪᴢᴀʀᴅ ʙᴏᴛꜱ 🤖</i>"
    ),
    message_thread_id=topic_thread_id
)

    else:
        await bot.send_message(channel_id, f"<b>-┈━═.•°✅ Completed ✅°•.═━┈-</b>\n<blockquote><b>🎯Batch Name : {b_name}</b></blockquote>\n<blockquote>🔗 Total URLs: {len(links)} \n┃   ┠🔴 Total Failed URLs: {failed_count}\n┃   ┠🟢 Total Successful URLs: {success_count}\n┃   ┃   ┠🎥 Total Video URLs: {video_count}\n┃   ┃   ┠📄 Total PDF URLs: {pdf_count}\n┃   ┃   ┠📸 Total IMAGE URLs: {img_count}</blockquote>\n", message_thread_id=topic_thread_id)
        await bot.send_message(m.chat.id, f"<blockquote><b>✅ Your Task is completed, please check your Set Channel📱</b></blockquote>")

    if topics_order and any(topic_data[t]["first_msg_id"] is not None for t in topics_order):
        try:
            ch_id = int(channel_id) if isinstance(channel_id, str) else channel_id
            clean_ch_id = str(ch_id).replace("-100", "")

            try:
                ch_info = await bot.get_chat(ch_id)
                ch_username = getattr(ch_info, 'username', None)
            except Exception:
                ch_username = None

            def make_link(msg_id, text):
                safe = html_escape(text)
                if not msg_id:
                    return safe
                if ch_username:
                    url = f"https://t.me/{ch_username}/{msg_id}"
                elif str(ch_id).startswith("-100"):
                    url = f"https://t.me/c/{clean_ch_id}/{msg_id}"
                else:
                    return safe
                return f'<a href="{url}">{safe}</a>'

            def format_stats(vids, pdfs):
                parts = []
                if vids > 0:
                    parts.append(f"🎞️ {vids}")
                if pdfs > 0:
                    parts.append(f"📑 {pdfs}")
                return " | ".join(parts) if parts else "📄 0"

            index_header = "<b>📋 ᴛᴏᴘɪᴄ ɪɴᴅᴇx</b>\n\n"
            index_header += f"<blockquote><b>📚 ʙᴀᴛᴄʜ :</b> {html_escape(b_name)}</blockquote>\n\n"

            topic_lines = []
            total_subtopics = 0
            for idx, topic in enumerate(topics_order):
                data = topic_data[topic]
                topic_link = make_link(data["first_msg_id"], topic)
                topic_stats = format_stats(data["videos"], data["pdfs"])
                topic_lines.append(f"<b>├ {str(idx+1).zfill(2)}. {topic_link}</b>\n├     └ {topic_stats}\n")

                for sub_idx, sub in enumerate(data["subtopics_order"]):
                    sub_data = data["subtopics"][sub]
                    sub_link = make_link(sub_data["first_msg_id"], sub)
                    sub_stats = format_stats(sub_data["videos"], sub_data["pdfs"])
                    is_last = sub_idx == len(data["subtopics_order"]) - 1
                    prefix = "╰" if is_last else "├"
                    topic_lines.append(f"│   {prefix} ◦ {sub_link} ({sub_stats})\n")
                    total_subtopics += 1

                if data["subtopics_order"]:
                    topic_lines.append("│\n")

            footer_parts = [f"<i>ᴛᴏᴛᴀʟ ᴛᴏᴘɪᴄꜱ : {len(topics_order)}</i>"]
            if total_subtopics > 0:
                footer_parts.append(f"<i>ᴛᴏᴛᴀʟ ꜱᴜʙ-ᴛᴏᴘɪᴄꜱ : {total_subtopics}</i>")
            index_footer = "\n".join(footer_parts)

            chunks = []
            current_chunk = index_header + "╭────────────────────────\n"
            for line in topic_lines:
                if len(current_chunk) + len(line) + len("╰────────────────────────\n") + len(index_footer) > 4000:
                    current_chunk += "╰────────────────────────"
                    chunks.append(current_chunk)
                    current_chunk = "╭────────────────────────\n" + line
                else:
                    current_chunk += line
            current_chunk += "╰────────────────────────\n" + index_footer
            chunks.append(current_chunk)

            last_index_msg = None
            for chunk in chunks:
                last_index_msg = await bot.send_message(channel_id, chunk, disable_web_page_preview=True, message_thread_id=topic_thread_id)

            if last_index_msg:
                try:
                    await bot.pin_chat_message(channel_id, last_index_msg.id)
                except Exception:
                    pass
        except Exception as e:
            print(f"Error sending topic index: {e}")

    if failed_urls:
        safe_name = re.sub(r'[^\w\s\-.]', '_', b_name)[:100]
        failed_file = f"Failed_URLs_{safe_name}.txt"
        try:
            with open(failed_file, "w", encoding="utf-8") as f:
                f.write(f"Failed URLs - Batch: {b_name}\n")
                f.write(f"Total Failed: {failed_count}\n")
                f.write("=" * 50 + "\n\n")
                for entry in failed_urls:
                    f.write(entry + "\n")
            await bot.send_document(
                chat_id=channel_id,
                document=failed_file,
                caption=f"<b>❌ Failed URLs List</b>\n<blockquote><b>Batch:</b> {b_name}\n<b>Total Failed:</b> {failed_count}</blockquote>",
                message_thread_id=topic_thread_id
            )
            if channel_id != m.chat.id:
                await bot.send_document(
                    chat_id=m.chat.id,
                    document=failed_file,
                    caption=f"<b>❌ Failed URLs List</b>\n<blockquote><b>Batch:</b> {b_name}\n<b>Total Failed:</b> {failed_count}</blockquote>"
                )
        except Exception as e:
            fallback_text = "<b>❌ Failed URLs:</b>\n\n" + "\n".join(failed_urls)
            await bot.send_message(channel_id, fallback_text, disable_web_page_preview=True)
        finally:
            if os.path.exists(failed_file):
                os.remove(failed_file)


@bot.on_message(filters.command(["gdrive"]) & auth_filter)
async def gdrive_handler(bot: Client, m: Message):
    bot_info = await bot.get_me()
    bot_username = bot_info.username

    if m.chat.type == "channel":
        if not db.is_channel_authorized(m.chat.id, bot_username):
            return
    else:
        if not db.is_user_authorized(m.from_user.id, bot_username):
            await m.reply_text("❌ You are not authorized to use this command.")
            return

    user_id = m.from_user.id if m.from_user else m.chat.id

    editable = await m.reply_text(
        "**📥 Google Drive Downloader**\n\n"
        "<blockquote>Send me a Google Drive file or folder link\n"
        "E.g: https://drive.google.com/file/d/xxxxx/view\n"
        "Or: https://drive.google.com/drive/folders/xxxxx\n"
        "Or: https://drive.google.com/open?id=xxxxx</blockquote>"
    )

    input_msg: Message = await bot.listen(m.chat.id)
    if not input_msg.text:
        await editable.edit("**❌ Please send a valid Google Drive link!**")
        return

    gdrive_url = input_msg.text.strip()
    await input_msg.delete(True)

    gd_file_match = re.search(r'/file/d/([a-zA-Z0-9_-]+)', gdrive_url)
    gd_folder_match = re.search(r'/folders/([a-zA-Z0-9_-]+)', gdrive_url)
    gd_open_match = re.search(r'[?&]id=([a-zA-Z0-9_-]+)', gdrive_url)

    if not gd_file_match and not gd_folder_match and not gd_open_match:
        await editable.edit("**❌ This doesn't look like a valid Google Drive link!\nMake sure it's a drive.google.com URL.**")
        return

    is_folder = bool(gd_folder_match)
    if gd_folder_match:
        gd_id = gd_folder_match.group(1)
    elif gd_file_match:
        gd_id = gd_file_match.group(1)
    else:
        gd_id = gd_open_match.group(1)

    await editable.edit("**1. Send A Text For Watermark\n2. Send /d for no watermark**")
    try:
        input_wm: Message = await bot.listen(m.chat.id, timeout=20)
        raw_wm = input_wm.text.strip() if input_wm.text else '/d'
        await input_wm.delete(True)
    except asyncio.TimeoutError:
        raw_wm = '/d'
    wm = "/d" if raw_wm == '/d' else raw_wm

    await editable.edit("**1. Send Your Name For Caption Credit\n2. Send /d For default Credit**")
    try:
        input_cr: Message = await bot.listen(m.chat.id, timeout=20)
        raw_cr = input_cr.text.strip() if input_cr.text else '/d'
        await input_cr.delete(True)
    except asyncio.TimeoutError:
        raw_cr = '/d'
    CR = CREDIT if raw_cr == '/d' else raw_cr

    await editable.edit("**1. Send A Image For Thumbnail\n2. Send /d For default Thumbnail\n3. Send /skip For Skipping**")
    thumb = "/d"
    try:
        input_th = await bot.listen(chat_id=m.chat.id, timeout=20)
        if input_th.photo:
            if not os.path.exists("downloads"):
                os.makedirs("downloads")
            temp_file = f"downloads/thumb_{user_id}.jpg"
            try:
                await bot.download_media(message=input_th.photo, file_name=temp_file)
                thumb = temp_file
            except Exception:
                thumb = "/d"
        elif input_th.text:
            if input_th.text == "/skip":
                thumb = "no"
            else:
                thumb = "/d"
        await input_th.delete(True)
    except asyncio.TimeoutError:
        thumb = "/d"

    await editable.edit("__**📢 Provide the Channel ID or send /d__\n\n<blockquote>🔹Send Your Channel ID where you want upload files.\n\nEx : -100XXXXXXXXX</blockquote>\n**")
    try:
        input_ch: Message = await bot.listen(m.chat.id, timeout=20)
        raw_ch = input_ch.text.strip() if input_ch.text else '/d'
        await input_ch.delete(True)
    except asyncio.TimeoutError:
        raw_ch = '/d'

    if raw_ch == '/d':
        channel_id = m.chat.id
    else:
        try:
            channel_id = int(raw_ch)
        except ValueError:
            await editable.edit("**❌ Invalid Channel ID! Please use a valid number.**")
            return

    await editable.delete()

    import gdown

    download_dir = f"downloads/gdrive_{m.chat.id}_{int(time.time())}"
    os.makedirs(download_dir, exist_ok=True)

    status_msg = await bot.send_message(m.chat.id, "**🔄 Downloading from Google Drive... Please wait ⏳**")

    try:
        downloaded_files = []

        if is_folder:
            await asyncio.to_thread(
                gdown.download_folder,
                url=gdrive_url,
                output=download_dir,
                quiet=False,
                use_cookies=False
            )
            for root, dirs, files in os.walk(download_dir):
                for fname in files:
                    downloaded_files.append(os.path.join(root, fname))
        else:
            output_path = os.path.join(download_dir, "gdrive_file")
            result_path = await asyncio.to_thread(
                gdown.download, id=gd_id, output=output_path, quiet=False, fuzzy=True
            )
            if result_path and os.path.exists(result_path):
                downloaded_files.append(result_path)

        if not downloaded_files:
            await status_msg.edit("**❌ Download failed! Make sure the file/folder is publicly shared.**")
            if os.path.exists(download_dir):
                shutil.rmtree(download_dir)
            return

        await status_msg.edit(f"**✅ Downloaded {len(downloaded_files)} file(s)\n🔄 Starting upload...**")

        failed_count = 0
        success_count = 0

        for idx, file_path in enumerate(downloaded_files, 1):
            fname = os.path.basename(file_path)
            file_size = os.path.getsize(file_path)
            file_ext = os.path.splitext(fname)[1].lower()
            name_no_ext = os.path.splitext(fname)[0]

            cc = (
                f"<b>🏷️ Iɴᴅᴇx ID  :</b> {str(idx).zfill(3)}\n\n"
                f"<b>📁  Fɪʟᴇ :</b> {fname}\n\n"
                f"<b>📦  Sɪᴢᴇ :</b> {helper.human_readable_size(file_size)}\n\n"
                f"<b>🎓  Uᴘʟᴏᴀᴅ Bʏ : {CR}</b>"
            )

            try:
                prog = await bot.send_message(
                    channel_id,
                    f"<i><b>📥 GDrive Uploading</b></i>\n<blockquote><b>{str(idx).zfill(3)}) {fname}</b></blockquote>",
                    disable_web_page_preview=True
                )

                if file_ext in ['.mp4', '.mkv', '.webm', '.avi', '.mov']:
                    await helper.send_vid(bot, m, cc, file_path, thumb, name_no_ext, prog, channel_id, wm)
                    success_count += 1
                elif file_ext == '.pdf':
                    start_time = time.time()
                    reply = await m.reply_text(f"📤 **Uploading PDF:** {fname}")
                    await bot.send_document(
                        chat_id=channel_id,
                        document=file_path,
                        caption=cc,
                        progress=progress_bar,
                        progress_args=(reply, start_time)
                    )
                    await reply.delete(True)
                    await prog.delete(True)
                    if os.path.exists(file_path):
                        os.remove(file_path)
                    success_count += 1
                elif file_ext in ['.jpg', '.jpeg', '.png', '.gif', '.bmp', '.webp']:
                    await bot.send_photo(
                        chat_id=channel_id,
                        photo=file_path,
                        caption=cc
                    )
                    await prog.delete(True)
                    if os.path.exists(file_path):
                        os.remove(file_path)
                    success_count += 1
                elif file_ext in ['.mp3', '.m4a', '.flac', '.wav', '.ogg', '.opus', '.aac']:
                    start_time = time.time()
                    reply = await m.reply_text(f"📤 **Uploading Audio:** {fname}")
                    await bot.send_audio(
                        chat_id=channel_id,
                        audio=file_path,
                        caption=cc,
                        progress=progress_bar,
                        progress_args=(reply, start_time)
                    )
                    await reply.delete(True)
                    await prog.delete(True)
                    if os.path.exists(file_path):
                        os.remove(file_path)
                    success_count += 1
                else:
                    start_time = time.time()
                    reply = await m.reply_text(f"📤 **Uploading:** {fname}")
                    await bot.send_document(
                        chat_id=channel_id,
                        document=file_path,
                        caption=cc,
                        progress=progress_bar,
                        progress_args=(reply, start_time)
                    )
                    await reply.delete(True)
                    await prog.delete(True)
                    if os.path.exists(file_path):
                        os.remove(file_path)
                    success_count += 1

            except FloodWait as e:
                await m.reply_text(f"⏳ FloodWait: waiting {e.value} seconds...")
                await asyncio.sleep(e.value)
                failed_count += 1
            except Exception as e:
                failed_count += 1
                logging.error(f"GDrive upload error for {fname}: {e}")
                await m.reply_text(f"❌ Failed to upload: {fname}\nError: {str(e)[:200]}")

        if os.path.exists(download_dir):
            shutil.rmtree(download_dir)

        await bot.send_message(
            m.chat.id,
            f"**✅ Google Drive Download Complete!**\n\n"
            f"<blockquote>📊 Total: {len(downloaded_files)} | ✅ Success: {success_count} | ❌ Failed: {failed_count}</blockquote>"
        )

    except Exception as e:
        logging.error(f"GDrive handler error: {e}")
        await status_msg.edit(f"**❌ Error:** {str(e)[:300]}\n\n<blockquote>Make sure the file/folder is publicly shared.</blockquote>")
        if os.path.exists(download_dir):
            shutil.rmtree(download_dir)


@bot.on_message(filters.command(["ytm"]) & auth_filter)
async def ytm_handler(bot: Client, m: Message):
    bot_info = await bot.get_me()
    bot_username = bot_info.username

    if m.chat.type == "channel":
        if not db.is_channel_authorized(m.chat.id, bot_username):
            return
    else:
        if not db.is_user_authorized(m.from_user.id, bot_username):
            await m.reply_text("❌ You are not authorized to use this command.")
            return

    editable = await m.reply_text(
        "**🎵 YouTube Music Downloader**\n\n"
        "<blockquote>Send me a YouTube / YouTube Music link\n"
        "E.g: https://youtube.com/watch?v=xxxxx\n"
        "or https://music.youtube.com/watch?v=xxxxx</blockquote>"
    )

    input_msg: Message = await bot.listen(m.chat.id)
    if not input_msg.text:
        await editable.edit("**❌ Please send a valid link!**")
        return

    yt_url = input_msg.text.strip()
    await input_msg.delete(True)

    if not any(domain in yt_url for domain in ['youtube.com', 'youtu.be', 'music.youtube.com']):
        await editable.edit("**❌ This doesn't look like a YouTube link!**")
        return

    await editable.edit(
        "**🎶 Select Audio Format:**\n\n"
        "╭━━⪼  `mp3`\n"
        "┣━━⪼  `m4a` (Best Quality)\n"
        "┣━━⪼  `flac` (Lossless)\n"
        "╰━━⪼  `opus`\n\n"
        "<blockquote>Send format name or /d for mp3</blockquote>"
    )
    try:
        input_fmt: Message = await bot.listen(m.chat.id, timeout=20)
        raw_fmt = input_fmt.text.strip().lower() if input_fmt.text else 'mp3'
        await input_fmt.delete(True)
    except asyncio.TimeoutError:
        raw_fmt = 'mp3'

    if raw_fmt == '/d':
        raw_fmt = 'mp3'

    valid_formats = ['mp3', 'm4a', 'flac', 'opus', 'wav']
    if raw_fmt not in valid_formats:
        raw_fmt = 'mp3'

    await editable.edit(
        "**🎶 Select Audio Quality:**\n\n"
        "╭━━⪼  `64` kbps\n"
        "┣━━⪼  `128` kbps\n"
        "┣━━⪼  `192` kbps\n"
        "┣━━⪼  `256` kbps\n"
        "╰━━⪼  `320` kbps (Best)\n\n"
        "<blockquote>Send quality or /d for 320</blockquote>"
    )
    try:
        input_q: Message = await bot.listen(m.chat.id, timeout=20)
        raw_q = input_q.text.strip() if input_q.text else '320'
        await input_q.delete(True)
    except asyncio.TimeoutError:
        raw_q = '320'

    if raw_q == '/d':
        raw_q = '320'

    valid_qualities = ['64', '128', '192', '256', '320']
    if raw_q not in valid_qualities:
        raw_q = '320'

    await editable.edit("__**📢 Provide the Channel ID or send /d__\n\n<blockquote>🔹Send Your Channel ID where you want upload files.\n\nEx : -100XXXXXXXXX</blockquote>\n**")
    try:
        input_ch: Message = await bot.listen(m.chat.id, timeout=20)
        raw_ch = input_ch.text.strip() if input_ch.text else '/d'
        await input_ch.delete(True)
    except asyncio.TimeoutError:
        raw_ch = '/d'

    channel_id = m.chat.id if raw_ch == '/d' else raw_ch

    await editable.edit("**🔄 Fetching audio info... Please wait ⏳**")

    try:
        ydl_opts = {
            'quiet': True,
            'no_warnings': True,
            'extract_flat': False,
            'skip_download': True,
            'nocheckcertificate': True,
            'extractor_args': {'youtube': {'player_client': ['web', 'android']}},
            'http_headers': {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36'},
        }
        if os.path.exists(cookies_file_path):
            ydl_opts['cookiefile'] = cookies_file_path
        if yt_email and yt_password:
            ydl_opts['username'] = yt_email
            ydl_opts['password'] = yt_password

        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            info = ydl.extract_info(yt_url, download=False)

        if not info:
            await editable.edit("**❌ Could not fetch audio info!**")
            return

        title = info.get('title', 'Unknown')
        artist = info.get('artist') or info.get('uploader', 'Unknown')
        duration = info.get('duration', 0)
        thumbnail_url = info.get('thumbnail', '')

        dur_min = int(duration // 60)
        dur_sec = int(duration % 60)

        safe_title = re.sub(r'[^\w\s\-]', '', title).strip()[:60]
        file_name = safe_title if safe_title else 'audio'

        await editable.edit(
            f"**🎵 {title}**\n"
            f"**🎤 {artist}**\n"
            f"**⏱️ {dur_min}:{str(dur_sec).zfill(2)}**\n\n"
            f"**📥 Downloading as {raw_fmt.upper()} @ {raw_q}kbps...**"
        )

        safe_url = safe_quote(yt_url)
        safe_fname = safe_quote(file_name)
        dl_cmd = f'yt-dlp {yt_bypass_flags} -x --audio-format {raw_fmt} --audio-quality {raw_q}k -o {safe_fname}.%(ext)s {safe_url}'

        _aria2c = get_aria2c()
        download_cmd = f'{dl_cmd} -R 25 --fragment-retries 25 --external-downloader "{_aria2c}" --downloader-args "aria2c: -x 2 -j 2"'
        rc, _out, dl_stderr = await async_shell_cmd(download_cmd)

        if rc != 0 and is_bot_detection_error(dl_stderr):
            raise Exception(f"YouTube Bot Detection: {dl_stderr.strip()[-200:]}")

        audio_file = None
        for ext in [raw_fmt, 'mp3', 'm4a', 'opus', 'flac', 'wav', 'webm', 'ogg']:
            candidate = f"{file_name}.{ext}"
            if os.path.isfile(candidate):
                audio_file = candidate
                break

        if not audio_file:
            await editable.edit("**❌ Download failed! Audio file not found.**")
            return

        temp_thumb = None
        if thumbnail_url:
            try:
                temp_thumb = os.path.join("downloads", f"ytm_thumb_{m.from_user.id}.jpg")
                os.makedirs("downloads", exist_ok=True)
                thumb_resp = requests.get(thumbnail_url, timeout=10)
                if thumb_resp.status_code == 200:
                    with open(temp_thumb, 'wb') as tf:
                        tf.write(thumb_resp.content)
                    _ffmpeg = get_ffmpeg()
                    await async_shell_cmd(
                        f'"{_ffmpeg}" -threads 1 -i "{temp_thumb}" -vf "scale=320:320:force_original_aspect_ratio=decrease,pad=320:320:(ow-iw)/2:(oh-ih)/2" -y "{temp_thumb}"'
                    )
                else:
                    temp_thumb = None
            except Exception:
                temp_thumb = None

        cc = (
            f"<b>🎵  Tɪᴛʟᴇ :</b> {title}\n"
            f"<b>🎤  Aʀᴛɪꜱᴛ :</b> {artist}\n"
            f"<b>⏱️  Dᴜʀᴀᴛɪᴏɴ :</b> {dur_min}:{str(dur_sec).zfill(2)}\n"
            f"<b>🎧  Qᴜᴀʟɪᴛʏ :</b> {raw_fmt.upper()} @ {raw_q}kbps\n\n"
            f"<b>🎓  Uᴘʟᴏᴀᴅ Bʏ : {CREDIT}</b>"
        )

        await editable.edit(f"**📤 Uploading: {title}...**")

        start_time = time.time()
        try:
            await bot.send_audio(
                chat_id=channel_id,
                audio=audio_file,
                caption=cc,
                title=title,
                performer=artist,
                duration=int(duration),
                thumb=temp_thumb if temp_thumb and os.path.exists(temp_thumb) else None,
                progress=progress_bar,
                progress_args=(editable, start_time)
            )
        except Exception:
            await bot.send_document(
                chat_id=channel_id,
                document=audio_file,
                caption=cc,
                progress=progress_bar,
                progress_args=(editable, start_time)
            )

        await editable.delete()

        if str(channel_id) != str(m.chat.id):
            await bot.send_message(m.chat.id, f"<blockquote><b>✅ Audio uploaded: {title}</b></blockquote>")

        if os.path.exists(audio_file):
            os.remove(audio_file)
        if temp_thumb and os.path.exists(temp_thumb):
            os.remove(temp_thumb)

    except Exception as e:
        if is_bot_detection_error(e):
            email, password = await ask_yt_credentials(bot, m.chat.id)
            if email and password:
                update_yt_credentials(email, password)
                status_msg = await m.reply_text("**🔄 Extracting cookies with your credentials...**")
                cookie_ok = await extract_yt_cookies_with_creds(email, password)
                if cookie_ok:
                    await status_msg.edit("**✅ Cookies extracted! Retrying download...**")
                else:
                    await status_msg.edit("**⚠️ Cookie extraction had issues, retrying with credentials anyway...**")
                await asyncio.sleep(1)
                await status_msg.delete()
                try:
                    ydl_opts_retry = {
                        'quiet': True,
                        'no_warnings': True,
                        'extract_flat': False,
                        'skip_download': True,
                        'nocheckcertificate': True,
                        'extractor_args': {'youtube': {'player_client': ['web', 'android']}},
                        'http_headers': {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36'},
                        'username': yt_email,
                        'password': yt_password,
                    }
                    if os.path.exists(cookies_file_path):
                        ydl_opts_retry['cookiefile'] = cookies_file_path
                    with yt_dlp.YoutubeDL(ydl_opts_retry) as ydl:
                        info = ydl.extract_info(yt_url, download=False)
                    if not info:
                        await m.reply_text("**❌ Could not fetch audio info even after retry!**")
                        return
                    title = info.get('title', 'Unknown')
                    artist = info.get('artist') or info.get('uploader', 'Unknown')
                    duration = info.get('duration', 0)
                    thumbnail_url = info.get('thumbnail', '')
                    safe_title = re.sub(r'[^\w\s\-]', '', title).strip()[:60]
                    file_name = safe_title if safe_title else 'audio'
                    await editable.edit(f"**📥 Retrying download: {title}...**")
                    safe_url = safe_quote(yt_url)
                    safe_fname = safe_quote(file_name)
                    dl_cmd = f'yt-dlp {yt_bypass_flags} -x --audio-format {raw_fmt} --audio-quality {raw_q}k -o {safe_fname}.%(ext)s {safe_url}'
                    _aria2c = get_aria2c()
                    download_cmd = f'{dl_cmd} -R 25 --fragment-retries 25 --external-downloader "{_aria2c}" --downloader-args "aria2c: -x 2 -j 2"'
                    await async_shell_cmd(download_cmd)
                    audio_file = None
                    for ext in [raw_fmt, 'mp3', 'm4a', 'opus', 'flac', 'wav', 'webm', 'ogg']:
                        candidate = f"{file_name}.{ext}"
                        if os.path.isfile(candidate):
                            audio_file = candidate
                            break
                    if not audio_file:
                        await editable.edit("**❌ Download failed even after retry!**")
                        return
                    await editable.edit(f"**📤 Uploading: {title}...**")
                    cc = (
                        f"<b>🎵  Tɪᴛʟᴇ :</b> {title}\n"
                        f"<b>🎤  Aʀᴛɪꜱᴛ :</b> {artist}\n"
                        f"<b>⏱️  Dᴜʀᴀᴛɪᴏɴ :</b> {int(duration // 60)}:{str(int(duration % 60)).zfill(2)}\n"
                        f"<b>🎧  Qᴜᴀʟɪᴛʏ :</b> {raw_fmt.upper()} @ {raw_q}kbps\n\n"
                        f"<b>🎓  Uᴘʟᴏᴀᴅ Bʏ : {CREDIT}</b>"
                    )
                    start_time = time.time()
                    try:
                        await bot.send_audio(chat_id=channel_id, audio=audio_file, caption=cc, title=title, performer=artist, duration=int(duration), progress=progress_bar, progress_args=(editable, start_time))
                    except Exception:
                        await bot.send_document(chat_id=channel_id, document=audio_file, caption=cc, progress=progress_bar, progress_args=(editable, start_time))
                    await editable.delete()
                    if os.path.exists(audio_file):
                        os.remove(audio_file)
                except Exception as retry_e:
                    await m.reply_text(f"**❌ Retry failed:** {str(retry_e)}")
                return
            else:
                await m.reply_text(f"**❌ Error:** {str(e)}")
        else:
            await m.reply_text(f"**❌ Error:** {str(e)}")


@bot.on_message(filters.command(["ytpl"]) & auth_filter)
async def ytpl_handler(bot: Client, m: Message):
    bot_info = await bot.get_me()
    bot_username = bot_info.username

    if m.chat.type == "channel":
        if not db.is_channel_authorized(m.chat.id, bot_username):
            return
    else:
        if not db.is_user_authorized(m.from_user.id, bot_username):
            await m.reply_text("❌ You are not authorized to use this command.")
            return

    editable = await m.reply_text(
        "**🎵 YouTube Playlist Downloader**\n\n"
        "<blockquote>Send me a YouTube playlist link\n"
        "E.g: https://youtube.com/playlist?list=PLxxxxx</blockquote>"
    )

    input_msg: Message = await bot.listen(m.chat.id)
    if not input_msg.text:
        await editable.edit("**❌ Please send a valid playlist link!**")
        return

    playlist_url = input_msg.text.strip()
    await input_msg.delete(True)

    from urllib.parse import urlparse, parse_qs
    parsed = urlparse(playlist_url)
    valid_hosts = ['youtube.com', 'www.youtube.com', 'm.youtube.com', 'music.youtube.com']
    is_valid_yt = parsed.hostname in valid_hosts and ('list' in parse_qs(parsed.query) or '/playlist' in parsed.path)

    if not is_valid_yt:
        await editable.edit("**❌ This doesn't look like a valid YouTube playlist link!\nMake sure it's a youtube.com URL with a playlist ID.**")
        return

    await editable.edit("**🔄 Fetching playlist info... Please wait ⏳**")

    try:
        ydl_opts = {
            'quiet': True,
            'no_warnings': True,
            'extract_flat': True,
            'force_generic_extractor': False,
            'nocheckcertificate': True,
            'extractor_args': {'youtube': {'player_client': ['web', 'android']}},
            'http_headers': {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36'},
        }
        if os.path.exists(cookies_file_path):
            ydl_opts['cookiefile'] = cookies_file_path
        if yt_email and yt_password:
            ydl_opts['username'] = yt_email
            ydl_opts['password'] = yt_password

        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            playlist_info = ydl.extract_info(playlist_url, download=False)

        if not playlist_info:
            await editable.edit("**❌ Could not fetch playlist info!**")
            return

        playlist_title = playlist_info.get('title', 'Unknown Playlist')
        entries = playlist_info.get('entries', [])
        entries = [e for e in entries if e is not None]

        if not entries:
            await editable.edit("**❌ No videos found in this playlist!**")
            return

        video_list = []
        for idx, entry in enumerate(entries, 1):
            v_title = entry.get('title', f'Video {idx}')
            v_url = entry.get('url') or entry.get('webpage_url') or f"https://www.youtube.com/watch?v={entry.get('id', '')}"
            video_list.append({'index': idx, 'title': v_title, 'url': v_url})

        await editable.edit(
            f"**🎵 Playlist: {playlist_title}**\n"
            f"**📊 Total Videos: {len(video_list)}**\n\n"
            f"<blockquote>Send start index (1-{len(video_list)}) or send /d to download all</blockquote>"
        )

        input_idx: Message = await bot.listen(m.chat.id, timeout=30)
        raw_idx = input_idx.text.strip() if input_idx.text else '/d'
        await input_idx.delete(True)

        start_index = 1
        if raw_idx != '/d':
            try:
                start_index = int(raw_idx)
                if start_index < 1 or start_index > len(video_list):
                    await editable.edit(f"**❌ Index out of range! Send between 1-{len(video_list)}**")
                    return
            except ValueError:
                await editable.edit("**❌ Invalid number!**")
                return

        await editable.edit("**🎞️  Eɴᴛᴇʀ  Rᴇꜱᴏʟᴜᴛɪᴏɴ\n\n╭━━⪼  `360`\n┣━━⪼  `480`\n┣━━⪼  `720`\n╰━━⪼  `1080`**")
        try:
            input_res: Message = await bot.listen(m.chat.id, timeout=20)
            raw_res = input_res.text.strip() if input_res.text else '480'
            await input_res.delete(True)
        except asyncio.TimeoutError:
            raw_res = '480'

        quality = f"{raw_res}p"
        res_map = {"144": "256x144", "240": "426x240", "360": "640x360", "480": "854x480", "720": "1280x720", "1080": "1920x1080"}
        res = res_map.get(raw_res, "UN")

        await editable.edit("**1. Send A Text For Watermark\n2. Send /d for no watermark**")
        try:
            input_wm: Message = await bot.listen(m.chat.id, timeout=20)
            raw_wm = input_wm.text.strip() if input_wm.text else '/d'
            await input_wm.delete(True)
        except asyncio.TimeoutError:
            raw_wm = '/d'

        wm = "/d" if raw_wm == '/d' else raw_wm

        await editable.edit(f"**1. Send Your Name For Caption Credit\n2. Send /d For default Credit**")
        try:
            input_cr: Message = await bot.listen(m.chat.id, timeout=20)
            raw_cr = input_cr.text.strip() if input_cr.text else '/d'
            await input_cr.delete(True)
        except asyncio.TimeoutError:
            raw_cr = '/d'

        CR = CREDIT if raw_cr == '/d' else raw_cr

        await editable.edit("**1. Send A Image For Thumbnail\n2. Send /d For default Thumbnail\n3. Send /skip For Skipping**")
        thumb = "/d"
        try:
            input_th = await bot.listen(chat_id=m.chat.id, timeout=20)
            if input_th.photo:
                if not os.path.exists("downloads"):
                    os.makedirs("downloads")
                temp_file = f"downloads/thumb_{m.from_user.id}.jpg"
                try:
                    await bot.download_media(message=input_th.photo, file_name=temp_file)
                    thumb = temp_file
                except Exception:
                    thumb = "/d"
            elif input_th.text:
                if input_th.text == "/skip":
                    thumb = "no"
                else:
                    thumb = "/d"
            await input_th.delete(True)
        except asyncio.TimeoutError:
            thumb = "/d"

        await editable.edit("__**📢 Provide the Channel ID or send /d__\n\n<blockquote>🔹Send Your Channel ID where you want upload files.\n\nEx : -100XXXXXXXXX</blockquote>\n**")
        try:
            input_ch: Message = await bot.listen(m.chat.id, timeout=20)
            raw_ch = input_ch.text.strip() if input_ch.text else '/d'
            await input_ch.delete(True)
        except asyncio.TimeoutError:
            raw_ch = '/d'

        channel_id = m.chat.id if raw_ch == '/d' else raw_ch
        await editable.delete()

        batch_msg = await bot.send_message(
            chat_id=channel_id,
            text=f"<blockquote><b>🎯 YouTube Playlist : {playlist_title}</b></blockquote>\n"
                 f"<blockquote>📊 Videos: {len(video_list)} | Starting from: {start_index}</blockquote>"
        )

        if str(channel_id) != str(m.chat.id):
            await bot.send_message(
                chat_id=m.chat.id,
                text=f"<blockquote><b>🎯 YouTube Playlist : {playlist_title}</b></blockquote>\n\n"
                     f"🔄 Your Task is under processing, please check your Set Channel📱"
            )

        failed_count = 0
        failed_urls = []
        count = start_index

        for video in video_list[start_index - 1:]:
            v_title = video['title']
            v_url = video['url']
            v_idx = video['index']

            name1 = re.sub(r'[^\w\s\-]', '', v_title).strip()[:60]
            name = name1 if name1 else f"Video_{v_idx}"

            ytf = f"bv*[height<={raw_res}][ext=mp4]+ba[ext=m4a]/b[height<=?{raw_res}]"
            safe_url = safe_quote(v_url)
            safe_name = safe_quote(name)
            cmd = f'yt-dlp {yt_bypass_flags} -f "{ytf}" {safe_url} -o {safe_name}.mp4'

            cc = (
                f"<b>🏷️ Iɴᴅᴇx ID  :</b> {str(v_idx).zfill(3)}\n\n"
                f"<b>🎞️  Tɪᴛʟᴇ :</b> {v_title} \n\n"
                f"<blockquote>📚  𝗕ᴀᴛᴄʜ : {playlist_title}</blockquote>"
                f"\n\n<b>🎓  Uᴘʟᴏᴀᴅ Bʏ : {CR}</b>"
            )

            Show = f"<i><b>📥 YT Playlist Downloading</b></i>\n<blockquote><b>{str(v_idx).zfill(3)}) {v_title}</b></blockquote>"
            prog = await bot.send_message(channel_id, Show, disable_web_page_preview=True)

            try:
                res_file = await helper.download_video(v_url, cmd, name)
                filename = res_file
                await helper.send_vid(bot, m, cc, filename, thumb, name, prog, channel_id, watermark=wm)
                count += 1
                await asyncio.sleep(1)
            except Exception as e:
                if is_bot_detection_error(e):
                    try:
                        await prog.delete(True)
                    except Exception:
                        pass
                    email, password = await ask_yt_credentials(bot, m.chat.id)
                    if email and password:
                        update_yt_credentials(email, password)
                        status_msg = await bot.send_message(m.chat.id, "**🔄 Extracting cookies with your credentials...**")
                        await extract_yt_cookies_with_creds(email, password)
                        await status_msg.edit("**✅ Credentials updated! Continuing playlist...**")
                        await asyncio.sleep(1)
                        await status_msg.delete()
                        cmd = f'yt-dlp {yt_bypass_flags} -f "{ytf}" {safe_url} -o {safe_name}.mp4'
                        prog = await bot.send_message(channel_id, Show, disable_web_page_preview=True)
                        try:
                            res_file = await helper.download_video(v_url, cmd, name)
                            filename = res_file
                            await helper.send_vid(bot, m, cc, filename, thumb, name, prog, channel_id, watermark=wm)
                            count += 1
                            await asyncio.sleep(1)
                            continue
                        except Exception as retry_e:
                            try:
                                await prog.delete(True)
                            except Exception:
                                pass
                            await bot.send_message(channel_id, f'⚠️**Downloading Failed (After Retry)**⚠️\n**Name** =>> `{str(v_idx).zfill(3)} {v_title}`\n**Url** =>> {v_url}\n\n<blockquote><i><b>Failed Reason: {str(retry_e)}</b></i></blockquote>', disable_web_page_preview=True)
                            failed_urls.append(f"{str(v_idx).zfill(3)}. {v_title} : {v_url}")
                            failed_count += 1
                            continue
                    else:
                        await bot.send_message(channel_id, f'⚠️**Downloading Failed**⚠️\n**Name** =>> `{str(v_idx).zfill(3)} {v_title}`\n**Url** =>> {v_url}\n\n<blockquote><i><b>Failed Reason: YouTube Bot Detection - No credentials provided</b></i></blockquote>', disable_web_page_preview=True)
                        failed_urls.append(f"{str(v_idx).zfill(3)}. {v_title} : {v_url}")
                        failed_count += 1
                        continue
                try:
                    await prog.delete(True)
                except Exception:
                    pass
                await bot.send_message(
                    channel_id,
                    f'⚠️**Downloading Failed**⚠️\n'
                    f'**Name** =>> `{str(v_idx).zfill(3)} {v_title}`\n'
                    f'**Url** =>> {v_url}\n\n'
                    f'<blockquote><i><b>Failed Reason: {str(e)}</b></i></blockquote>',
                    disable_web_page_preview=True
                )
                failed_urls.append(f"{str(v_idx).zfill(3)}. {v_title} : {v_url}")
                failed_count += 1
                continue

        success_count = len(video_list[start_index - 1:]) - failed_count
        await bot.send_message(
            channel_id,
            f"<b>📬 ᴘʀᴏᴄᴇꜱꜱ ᴄᴏᴍᴘʟᴇᴛᴇᴅ</b>\n\n"
            f"<blockquote><b>🎵 Playlist : {playlist_title}</b></blockquote>\n"
            f"╭────────────────\n"
            f"├ 🖇️ ᴛᴏᴛᴀʟ ᴠɪᴅᴇᴏꜱ : <code>{len(video_list[start_index - 1:])}</code>\n"
            f"├ ✅ ꜱᴜᴄᴄᴇꜱꜱꜰᴜʟ : <code>{success_count}</code>\n"
            f"├ ❌ ꜰᴀɪʟᴇᴅ : <code>{failed_count}</code>\n"
            f"╰────────────────\n\n"
            f"<i>ᴇxᴛʀᴀᴄᴛᴇᴅ ʙʏ ᴡɪᴢᴀʀᴅ ʙᴏᴛꜱ 🤖</i>"
        )

        if str(channel_id) != str(m.chat.id):
            await bot.send_message(m.chat.id, f"<blockquote><b>✅ Playlist download completed! Check your channel 📱</b></blockquote>")

        if failed_urls:
            safe_name = re.sub(r'[^\w\s\-.]', '_', playlist_title)[:100]
            failed_file = f"Failed_YT_{safe_name}.txt"
            try:
                with open(failed_file, "w", encoding="utf-8") as f:
                    f.write(f"Failed URLs - Playlist: {playlist_title}\n")
                    f.write(f"Total Failed: {failed_count}\n")
                    f.write("=" * 50 + "\n\n")
                    for entry in failed_urls:
                        f.write(entry + "\n")
                await bot.send_document(
                    chat_id=channel_id,
                    document=failed_file,
                    caption=f"<b>❌ Failed URLs List</b>\n<blockquote><b>Playlist:</b> {playlist_title}\n<b>Total Failed:</b> {failed_count}</blockquote>"
                )
                if str(channel_id) != str(m.chat.id):
                    await bot.send_document(
                        chat_id=m.chat.id,
                        document=failed_file,
                        caption=f"<b>❌ Failed URLs List</b>\n<blockquote><b>Playlist:</b> {playlist_title}\n<b>Total Failed:</b> {failed_count}</blockquote>"
                    )
            except Exception:
                fallback_text = "<b>❌ Failed URLs:</b>\n\n" + "\n".join(failed_urls)
                await bot.send_message(channel_id, fallback_text, disable_web_page_preview=True)
            finally:
                if os.path.exists(failed_file):
                    os.remove(failed_file)

    except asyncio.TimeoutError:
        await editable.edit("**⏳ Timeout! Please try again with /ytpl**")
    except Exception as e:
        if is_bot_detection_error(e):
            email, password = await ask_yt_credentials(bot, m.chat.id)
            if email and password:
                update_yt_credentials(email, password)
                status_msg = await m.reply_text("**🔄 Extracting cookies with your credentials...**")
                cookie_ok = await extract_yt_cookies_with_creds(email, password)
                if cookie_ok:
                    await status_msg.edit("**✅ Cookies extracted! Retrying download...**")
                else:
                    await status_msg.edit("**⚠️ Cookie extraction had issues, retrying with credentials anyway...**")
                await asyncio.sleep(1)
                await status_msg.delete()
                await ytpl_handler(bot, m)
                return
            else:
                await m.reply_text(f"**❌ Error:** {str(e)}")
        else:
            await m.reply_text(f"**❌ Error:** {str(e)}")


@bot.on_message(filters.text & filters.private)
async def text_handler(bot: Client, m: Message):
    if m.from_user.is_bot:
        return
    links = m.text
    path = None
    match = re.search(r'https?://\S+', links)
    if match:
        link = match.group(0)
    else:
        await m.reply_text("<pre><code>Invalid link format.</code></pre>")
        return
        
    editable = await m.reply_text(f"<pre><code>**🔹Processing your link...\n🔁Please wait...⏳**</code></pre>")
    await m.delete()

    await editable.edit(f"╭━━━━❰ᴇɴᴛᴇʀ ʀᴇꜱᴏʟᴜᴛɪᴏɴ❱━━➣ \n┣━━⪼ send `144`\n┣━━⪼ send `240`\n┣━━⪼ send `360`\n┣━━⪼ send `480`\n┣━━⪼ send `720`\n┣━━⪼ send `1080`\n╰━━⌈⚡[`{CREDIT}`]⚡⌋━━➣ ")
    input2: Message = await bot.listen(editable.chat.id, filters=filters.text & filters.user(m.from_user.id))
    raw_text2 = input2.text
    quality = f"{raw_text2}p"
    await input2.delete(True)
    try:
        if raw_text2 == "144":
            res = "256x144"
        elif raw_text2 == "240":
            res = "426x240"
        elif raw_text2 == "360":
            res = "640x360"
        elif raw_text2 == "480":
            res = "854x480"
        elif raw_text2 == "720":
            res = "1280x720"
        elif raw_text2 == "1080":
            res = "1920x1080" 
        else: 
            res = "UN"
    except Exception:
            res = "UN"
          
   
    raw_text4 = "working_token"
    thumb = "/d"
    count =0
    arg =1
    channel_id = m.chat.id
    try:
            Vxy = link.replace("file/d/","uc?export=download&id=").replace("www.youtube-nocookie.com/embed", "youtu.be").replace("?modestbranding=1", "").replace("/view?usp=sharing","")
            url = Vxy

            name1 = links.replace("(", "").replace(")", "").replace("[", "").replace("]", "").replace("_", "").replace("\t", "").replace(":", "").replace("/", "").replace("+", "").replace("#", "").replace("|", "").replace("@", "").replace("*", "").replace(".", "").replace("https", "").replace("http", "").strip()
            name = f'{name1[:60]}'
            appxkey = None
            _is_appx_video = False
            
            if "visionias" in url:
                async with ClientSession() as session:
                    async with session.get(url, headers={'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9', 'Accept-Language': 'en-US,en;q=0.9', 'Cache-Control': 'no-cache', 'Connection': 'keep-alive', 'Pragma': 'no-cache', 'Referer': 'http://www.visionias.in/', 'Sec-Fetch-Dest': 'iframe', 'Sec-Fetch-Mode': 'navigate', 'Sec-Fetch-Site': 'cross-site', 'Upgrade-Insecure-Requests': '1', 'User-Agent': 'Mozilla/5.0 (Linux; Android 12; RMX2121) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/107.0.0.0 Mobile Safari/537.36', 'sec-ch-ua': '"Chromium";v="107", "Not=A?Brand";v="24"', 'sec-ch-ua-mobile': '?1', 'sec-ch-ua-platform': '"Android"',}) as resp:
                        text = await resp.text()
                        url = re.search(r"(https://.*?playlist.m3u8.*?)\"", text).group(1)

            if "appxsignurl" in url:
                try:
                    _appx_url, _appx_title, _appx_enc, _appx_type = await helper.resolve_appx_url(url, raw_text2)
                    if _appx_title:
                        name1 = _appx_title[:80]
                        name = name1[:60]
                    if _appx_type == "pdf":
                        prog = await m.reply_text(f"📥 **Downloading AppX PDF...**\n<blockquote>{name1}</blockquote>")
                        _pdf_file = f"{name}.pdf"
                        _pdf_headers = ('-H "User-Agent: Mozilla/5.0 (Linux; Android 13; Pixel 7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Mobile Safari/537.36" '
                                        '-H "Accept: application/pdf,*/*" '
                                        '-H "Referer: https://app.classx.co.in/" '
                                        '-H "Origin: https://app.classx.co.in"')
                        import subprocess as _sp
                        _ret = _sp.run(f'curl -L --fail --retry 3 --retry-delay 2 {_pdf_headers} -o "{_pdf_file}" "{_appx_url}"', shell=True)
                        await prog.delete(True)
                        _pdf_ok = _ret.returncode == 0 and os.path.exists(_pdf_file) and os.path.getsize(_pdf_file) > 500
                        if _pdf_ok:
                            _cc_pdf = f"<b>📑 {name1}</b>\n<blockquote>🎓 {CR}</blockquote>"
                            await m.reply_document(document=_pdf_file, caption=_cc_pdf)
                            os.remove(_pdf_file)
                        else:
                            if os.path.exists(_pdf_file):
                                os.remove(_pdf_file)
                            await m.reply_text(f"⚠️ AppX PDF download failed for:\n<blockquote>{name1}</blockquote>")
                        return
                    if '*' in _appx_url:
                        _uk = _appx_url.rsplit('*', 1)
                        _appx_url = _uk[0]
                        _raw_appxkey = _appx_enc or _uk[1] or None
                    else:
                        _raw_appxkey = _appx_enc or None
                    if _raw_appxkey:
                        try:
                            appxkey = b64decode(_raw_appxkey).decode().strip()
                        except Exception:
                            appxkey = _raw_appxkey
                    else:
                        appxkey = None
                    url = _appx_url
                    _is_appx_video = True
                    logging.info(f"AppX video resolved: url={url[:80]}... key={appxkey}")
                except Exception as _e:
                    logging.error(f"AppX URL resolution failed: {_e}")
                    await m.reply_text(f"⚠️ **AppX URL failed**\n<blockquote>{_e}</blockquote>")
                    return


            if "acecwply" in url:
                cmd = f'yt-dlp -o "{name}.%(ext)s" -f "bestvideo[height<={raw_text2}]+bestaudio" --hls-prefer-ffmpeg --no-keep-video --remux-video mkv --no-warning "{url}"'

            elif "https://cpmc/" in url:
                _cpmc_id = url.replace("https://cpmc/", "").replace(".m3u8", "")
                try:
                    _r = requests.get("https://api-seven-omega-33.vercel.app/extract",
                                      params={"content_id": _cpmc_id, "token": raw_text4}, timeout=15)
                    _cpmc = _r.json()
                    _signed = _cpmc.get("signed_url", "")
                    if _signed and "drm" in _signed.lower():
                        mpd = _cpmc.get("mpd"); _keys = _cpmc.get("keys", [])
                        url = mpd; keys_string = " ".join([f"--key {k}" for k in _keys])
                    else:
                        url = _signed
                except Exception as _ce:
                    logging.error(f"CPMC extraction failed: {_ce}")

            elif "thin-wynnie-appx-d3d205f7.koyeb.app/play/" in url and "*" in url:
                _parts = url.split("*", 1)
                appxkey = _parts[1].strip()
                try:
                    _r = requests.get(_parts[0].strip(), timeout=10, allow_redirects=True)
                    url = _r.url.strip()
                    logging.info(f"DragoAPI resolved: {url[:80]} key={appxkey}")
                except Exception as _de:
                    logging.error(f"DragoAPI resolve failed: {_de}")

            elif "thin-wynnie-appx-d3d205f7.koyeb.app/play/" in url:
                try:
                    _r = requests.get(url, timeout=10, allow_redirects=True)
                    url = _r.url.strip()
                    logging.info(f"DragoAPI (no-key) resolved: {url[:80]}")
                except Exception as _de:
                    logging.error(f"DragoAPI resolve failed: {_de}")

            elif ".m3u8" in url and "appx" in url and "appxsignurl" not in url:
                try:
                    _r = requests.get(url, timeout=10)
                    _jd = _r.json()
                    _enc_url = _jd.get("video_url", "")
                    if _enc_url and "*" in _enc_url:
                        _before, _after = _enc_url.split("*", 1)
                        url = _before.strip()
                        try:
                            appxkey = b64decode(_after.strip()).decode().strip()
                        except Exception:
                            appxkey = _after.strip()
                    elif _enc_url:
                        url = _enc_url.strip()
                        appxkey = _jd.get("encryption_key")
                    logging.info(f"AppX m3u8-JSON resolved: url={url[:80]} key={appxkey}")
                except Exception as _ae:
                    logging.warning(f"AppX m3u8-JSON fallback (treating as plain HLS): {_ae}")

            elif "https://cpvod.testbook.com/" in url or "classplusapp.com/drm/" in url:
                url = url.replace("https://cpvod.testbook.com/","https://media-cdn.classplusapp.com/drm/")
                url = f"https://covercel.vercel.app/extract_keys?url={url}@bots_updatee&user_id=7793257011"
                mpd, keys = helper.get_mps_and_keys(url)
                url = mpd
                keys_string = " ".join([f"--key {key}" for key in keys])

            elif "https://static-trans-v1.classx.co.in" in url or "https://static-trans-v2.classx.co.in" in url:
                if "*" in url:
                    base_with_params, signature = url.split("*", 1)
                    base_clean = base_with_params.split(".mkv")[0] + ".mkv"
                    if "static-trans-v1.classx.co.in" in url:
                        base_clean = base_clean.replace("https://static-trans-v1.classx.co.in", "https://appx-transcoded-videos-mcdn.akamai.net.in")
                    elif "static-trans-v2.classx.co.in" in url:
                        base_clean = base_clean.replace("https://static-trans-v2.classx.co.in", "https://transcoded-videos-v2.classx.co.in")
                    url = f"{base_clean}*{signature}"

            elif "https://static-rec.classx.co.in/drm/" in url:
                if "*" in url:
                    base_with_params, signature = url.split("*", 1)
                    base_clean = base_with_params.split("?")[0]
                    base_clean = base_clean.replace("https://static-rec.classx.co.in", "https://appx-recordings-mcdn.akamai.net.in")
                    url = f"{base_clean}*{signature}"

            elif "https://static-wsb.classx.co.in/" in url:
                clean_url = url.split("?")[0]

                clean_url = clean_url.replace("https://static-wsb.classx.co.in", "https://appx-wsb-gcp-mcdn.akamai.net.in")

                url = clean_url

            elif "https://static-db.classx.co.in/" in url:
                if "*" in url:
                    base_url, key = url.split("*", 1)
                    base_url = base_url.split("?")[0]
                    base_url = base_url.replace("https://static-db.classx.co.in", "https://appxcontent.kaxa.in")
                    url = f"{base_url}*{key}"
                else:
                    base_url = url.split("?")[0]
                    url = base_url.replace("https://static-db.classx.co.in", "https://appxcontent.kaxa.in")


            elif "https://static-db-v2.classx.co.in/" in url:
                if "*" in url:
                    base_url, key = url.split("*", 1)
                    base_url = base_url.split("?")[0]
                    base_url = base_url.replace("https://static-db-v2.classx.co.in", "https://appx-content-v2.classx.co.in")
                    url = f"{base_url}*{key}"
                else:
                    base_url = url.split("?")[0]
                    url = base_url.replace("https://static-db-v2.classx.co.in", "https://appx-content-v2.classx.co.in")

            elif "classplusapp" in url:
                signed_api = f"https://covercel.vercel.app/extract_keys?url={url}@bots_updatee&user_id=7793257011"
                response = requests.get(signed_api, timeout=20)
                url = response.text.strip()
                url = response.json()['url']  

            elif "tencdn.classplusapp" in url:
                headers = {'host': 'api.classplusapp.com', 'x-access-token': f'{cptoken}', 'accept-language': 'EN', 'api-version': '18', 'app-version': '1.4.73.2', 'build-number': '35', 'connection': 'Keep-Alive', 'content-type': 'application/json', 'device-details': 'Xiaomi_Redmi 7_SDK-32', 'device-id': 'c28d3cb16bbdac01', 'region': 'IN', 'user-agent': 'Mobile-Android', 'webengage-luid': '00000187-6fe4-5d41-a530-26186858be4c', 'accept-encoding': 'gzip'}
                params = {"url": f"{url}"}
                response = requests.get('https://api.classplusapp.com/cams/uploader/video/jw-signed-url', headers=headers, params=params)
                url = response.json()['url']  

            elif 'videos.classplusapp' in url:
                url = requests.get(f'https://api.classplusapp.com/cams/uploader/video/jw-signed-url?url={url}', headers={'x-access-token': f'{cptoken}'}).json()['url']
            
            elif 'media-cdn.classplusapp.com' in url or 'media-cdn-alisg.classplusapp.com' in url or 'media-cdn-a.classplusapp.com' in url: 
                headers = {'host': 'api.classplusapp.com', 'x-access-token': f'{cptoken}', 'accept-language': 'EN', 'api-version': '18', 'app-version': '1.4.73.2', 'build-number': '35', 'connection': 'Keep-Alive', 'content-type': 'application/json', 'device-details': 'Xiaomi_Redmi 7_SDK-32', 'device-id': 'c28d3cb16bbdac01', 'region': 'IN', 'user-agent': 'Mobile-Android', 'webengage-luid': '00000187-6fe4-5d41-a530-26186858be4c', 'accept-encoding': 'gzip'}
                params = {"url": f"{url}"}
                response = requests.get('https://api.classplusapp.com/cams/uploader/video/jw-signed-url', headers=headers, params=params)
                url   = response.json()['url']

            elif "childId" in url and "parentId" in url:
                    url = f"https://anonymouspwplayer-0e5a3f512dec.herokuapp.com/pw?url={url}&token={raw_text4}"
                           
            elif "d1d34p8vz63oiq" in url or "sec1.pw.live" in url:
                url = f"https://anonymouspwplayer-b99f57957198.herokuapp.com/pw?url={url}?token={raw_text4}"

            if ".pdf*" in url:
                url = f"https://dragoapi.vercel.app/pdf/{url}"
            
            elif 'encrypted.m' in url or _is_appx_video:
                if '*' in url:
                    parts = url.split('*', 1)
                    if not appxkey:
                        _raw_key = parts[1].strip()
                        try:
                            appxkey = b64decode(_raw_key).decode().strip()
                        except Exception:
                            appxkey = _raw_key
                    url = parts[0]
                # if _is_appx_video with no '*', appxkey and url are already set above

            if "youtu" in url:
                ytf = f"bv*[height<={raw_text2}][ext=mp4]+ba[ext=m4a]/b[height<=?{raw_text2}]"
            elif "embed" in url:
                ytf = f"bestvideo[height<={raw_text2}]+bestaudio/best[height<={raw_text2}]"
            else:
                ytf = f"b[height<={raw_text2}]/bv[height<={raw_text2}]+ba/b/bv+ba"
           
            if "jw-prod" in url:
                cmd = f'yt-dlp -o "{name}.mp4" "{url}"'
            elif "webvideos.classplusapp." in url:
               cmd = f'yt-dlp --add-header "referer:https://web.classplusapp.com/" --add-header "x-cdn-tag:empty" -f "{ytf}" "{url}" -o "{name}.mp4"'
            elif "youtube.com" in url or "youtu.be" in url:
                cmd = f'yt-dlp {yt_bypass_flags} -f "{ytf}" "{url}" -o "{name}".mp4'
            else:
                cmd = f'yt-dlp -f "{ytf}" "{url}" -o "{name}.mp4"'

            try:
                cc = f'**🎞️ Title `{name} [{res}].mp4`\n\n🖇️LNK : <a href="{link}">Click Here</a>\n\n🎓 Uploaded By» {CREDIT}**'
                cc1 = f'**📑 Title» `{name}`\n\n🖇️ LNK : <a href="{link}">Click Here</a>\n\n🎓 Uploaded By {CREDIT}**'
                  
                if "drive" in url:
                    try:
                        ka = await helper.download(url, name)
                        copy = await bot.send_document(chat_id=m.chat.id,document=ka, caption=cc1)
                        count+=1
                        os.remove(ka)
                    except FloodWait as e:
                        await m.reply_text(f"⏳ FloodWait: waiting {e.value} seconds...")
                        await asyncio.sleep(e.value)
                        copy = await bot.send_document(chat_id=m.chat.id,document=ka, caption=cc1)
                        count+=1
                        os.remove(ka)

                elif ".pdf" in url:
                    if "cwmediabkt99" in url:
                        max_retries = 15  # Define the maximum number of retries
                        retry_delay = 4  # Delay between retries in seconds
                        success = False  # To track whether the download was successful
                        failure_msgs = []  # To keep track of failure messages
                        
                        for attempt in range(max_retries):
                            try:
                                await asyncio.sleep(retry_delay)
                                url = url.replace(" ", "%20")
                                scraper = cloudscraper.create_scraper()
                                response = scraper.get(url)

                                if response.status_code == 200:
                                    with open(f'{name}.pdf', 'wb') as file:
                                        file.write(response.content)
                                    await asyncio.sleep(retry_delay)  # Optional, to prevent spamming
                                    copy = await bot.send_document(chat_id=m.chat.id, document=f'{name}.pdf', caption=cc1)
                                    os.remove(f'{name}.pdf')
                                    success = True
                                    break  # Exit the retry loop if successful
                                else:
                                    failure_msg = await m.reply_text(f"Attempt {attempt + 1}/{max_retries} failed: {response.status_code} {response.reason}")
                                    failure_msgs.append(failure_msg)
                                    
                            except Exception as e:
                                failure_msg = await m.reply_text(f"Attempt {attempt + 1}/{max_retries} failed: {str(e)}")
                                failure_msgs.append(failure_msg)
                                await asyncio.sleep(retry_delay)
                                continue 

                        # Delete all failure messages if the PDF is successfully downloaded
                        for msg in failure_msgs:
                            await msg.delete()
                            
                        if not success:
                            # Send the final failure message if all retries fail
                            await m.reply_text(f"Failed to download PDF after {max_retries} attempts.\n⚠️**Downloading Failed**⚠️\n**Name** =>> {str(count).zfill(3)} {name1}\n**Url** =>> {link0}", disable_web_page_preview)
                            
                    else:
                        try:
                            cmd = f'yt-dlp -o "{name}.pdf" "{url}"'
                            download_cmd = f"{cmd} -R 25 --fragment-retries 25"
                            await async_shell_cmd(download_cmd)
                            copy = await bot.send_document(chat_id=m.chat.id, document=f'{name}.pdf', caption=cc1)
                            os.remove(f'{name}.pdf')
                        except FloodWait as e:
                            await m.reply_text(f"⏳ FloodWait: waiting {e.value} seconds...")
                            await asyncio.sleep(e.value)
                            copy = await bot.send_document(chat_id=m.chat.id, document=f'{name}.pdf', caption=cc1)
                            os.remove(f'{name}.pdf')

                elif any(ext in url for ext in [".mp3", ".wav", ".m4a"]):
                    try:
                        ext = url.split('.')[-1]
                        cmd = f'yt-dlp -x --audio-format {ext} -o "{name}.{ext}" "{url}"'
                        download_cmd = f"{cmd} -R 25 --fragment-retries 25"
                        await async_shell_cmd(download_cmd)
                        await bot.send_document(chat_id=m.chat.id, document=f'{name}.{ext}', caption=cc1)
                        os.remove(f'{name}.{ext}')
                    except FloodWait as e:
                        await m.reply_text(f"⏳ FloodWait: waiting {e.value} seconds...")
                        await asyncio.sleep(e.value)
                        await bot.send_document(chat_id=m.chat.id, document=f'{name}.{ext}', caption=cc1)
                        os.remove(f'{name}.{ext}')

                elif any(ext in url for ext in [".jpg", ".jpeg", ".png"]):
                    try:
                        ext = url.split('.')[-1]
                        cmd = f'yt-dlp -o "{name}.{ext}" "{url}"'
                        download_cmd = f"{cmd} -R 25 --fragment-retries 25"
                        await async_shell_cmd(download_cmd)
                        copy = await bot.send_photo(chat_id=m.chat.id, photo=f'{name}.{ext}', caption=cc1)
                        count += 1
                        os.remove(f'{name}.{ext}')
                    except FloodWait as e:
                        await m.reply_text(f"⏳ FloodWait: waiting {e.value} seconds...")
                        await asyncio.sleep(e.value)
                        copy = await bot.send_photo(chat_id=m.chat.id, photo=f'{name}.{ext}', caption=cc1)
                        count += 1
                        os.remove(f'{name}.{ext}')
                                
                elif 'encrypted.m' in url or _is_appx_video:    
                    Show = f"**⚡Dᴏᴡɴʟᴏᴀᴅɪɴɢ Sᴛᴀʀᴛᴇᴅ...⏳**\n" \
                           f"🔗𝐋𝐢𝐧𝐤 » {url}\n" \
                           f"✦𝐁𝐨𝐭 𝐌𝐚𝐝𝐞 𝐁𝐲 ✦ {CREDIT}"
                    prog = await m.reply_text(Show, disable_web_page_preview=True)
                    res_file = await helper.download_and_decrypt_video(url, cmd, name, appxkey)  
                    filename = res_file  
                    await prog.delete(True)  
                    await helper.send_vid(bot, m, cc, filename, thumb, name, prog, channel_id, watermark=watermark)
                    await asyncio.sleep(1)  
                    pass

                elif 'drmcdni' in url or 'drm/wv' in url:
                    Show = f"**⚡Dᴏᴡɴʟᴏᴀᴅɪɴɢ Sᴛᴀʀᴛᴇᴅ...⏳**\n" \
                           f"🖇️ LNK » {url}\n" \
                           f"🎓 Uploaded By » {CREDIT}"
                    prog = await m.reply_text(Show, disable_web_page_preview=True)
                    res_file = await helper.decrypt_and_merge_video(mpd, keys_string, path, name, raw_text2)
                    filename = res_file
                    await prog.delete(True)
                    await helper.send_vid(bot, m, cc, filename, thumb, name, prog, channel_id, watermark=watermark)
                    await asyncio.sleep(1)
                    pass

                else:
                    Show = f"**🚀Dᴏᴡɴʟᴏᴀᴅɪɴɢ Sᴛᴀʀᴛᴇᴅ **\n" \
                           f"🔗 𝐋𝐢𝐧𝐤 » {url}\n" \
                           f"✦ Uploader {CREDIT}"
                    prog = await m.reply_text(Show, disable_web_page_preview=True)
                    res_file = await helper.download_video(url, cmd, name)
                    filename = res_file
                    await prog.delete(True)
                    await helper.send_vid(bot, m, cc, filename, thumb, name, prog, channel_id, watermark=watermark)
                    time.sleep(1)
                
            except Exception as e:
                    await m.reply_text(f"⚠️𝐃𝐨𝐰𝐧𝐥𝐨𝐚𝐝𝐢𝐧𝐠 𝐈𝐧𝐭𝐞𝐫𝐮𝐩𝐭𝐞𝐝\n\n🔗𝐋𝐢𝐧𝐤 » `{link}`\n\n<blockquote><b><i>⚠️Failed Reason »**__\n{str(e)}</i></b></blockquote>")
                    pass

    except Exception as e:
        await m.reply_text(str(e))

def notify_owner():
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    data = {
        "chat_id": OWNER_ID,
        "text": "Bᴏᴛ Iꜱ Lɪᴠᴇ Nᴏᴡ 🤖"
    }
    requests.post(url, data=data)


def reset_and_set_commands():
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/setMyCommands"
    # Reset
    requests.post(url, json={"commands": []})
    # Set new
    commands = [
    {"command": "start", "description": "✅ ᴄʜᴇᴄᴋ ɪꜰ ᴛʜᴇ ʙᴏᴛ ɪꜱ ᴀʟɪᴠᴇ"},
    {"command": "drm", "description": "📄 ᴜᴘʟᴏᴀᴅ ᴀ .ᴛxᴛ ꜰɪʟᴇ"},
    {"command": "stop", "description": "⏹ ᴛᴇʀᴍɪɴᴀᴛᴇ ᴛʜᴇ ᴏɴɢᴏɪɴɢ ᴘʀᴏᴄᴇꜱꜱ"},
    {"command": "reset", "description": "♻️ ʀᴇꜱᴇᴛ ᴛʜᴇ ʙᴏᴛ"},
    {"command": "cookies", "description": "🍪 ᴜᴘʟᴏᴀᴅ ʏᴏᴜᴛᴜʙᴇ ᴄᴏᴏᴋɪᴇꜱ"},
    {"command": "mytc", "description": "🔑 ᴇxᴛʀᴀᴄᴛ ʏᴛ ᴄᴏᴏᴋɪᴇꜱ ᴠɪᴀ ᴇᴍᴀɪʟ*ᴘᴀꜱꜱ"},
    {"command": "t2h", "description": "📑 → 🌐 HTML converter"},
    {"command": "modern", "description": "🔓 Pro Sidebar Theme"},
    {"command": "neumorphic", "description": "🔓 Soft Grey Theme"},
    {"command": "brutalist", "description": "🔓 Bold & Raw Theme"},
    {"command": "glassmorphism", "description": "🔓 Glass Effect Theme"},
    {"command": "cyberpunk", "description": "🔓 Neon Tech Theme"},
    {"command": "yengo", "description": "🔓 Folder Explorer Theme"},
    {"command": "t2t", "description": "📝 ᴛᴇxᴛ → .ᴛxᴛ ɢᴇɴᴇʀᴀᴛᴏʀ"},
    {"command": "id", "description": "🆔 ɢᴇᴛ ʏᴏᴜʀ ᴜꜱᴇʀ ɪᴅ"},
    {"command": "add", "description": "▶️ Add Auth "},
    {"command": "info", "description": "ℹ️ ᴄʜᴇᴄᴋ ʏᴏᴜʀ ɪɴꜰᴏʀᴍᴀᴛɪᴏɴ"},
    {"command": "remove", "description": "⏸️ Remove Auth "},
    {"command": "users", "description": "👨‍👨‍👧‍👦 All Users"},
    {"command": "gdrive", "description": "📥 Google Drive Downloader"},
    {"command": "ytpl", "description": "🎵 YouTube Playlist Downloader"},
    {"command": "ytm", "description": "🎶 YouTube Music Downloader"},
    {"command": "setgroup", "description": "📌 Set upload group"},
    {"command": "getgroup", "description": "👁 View set group"},
    {"command": "removegroup", "description": "🗑 Remove set group"},
]

    requests.post(url, json={"commands": commands})
    



if __name__ == "__main__":
    reset_and_set_commands()
    notify_owner() 

bot.run()
