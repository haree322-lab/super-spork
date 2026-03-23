# Deployment Guide

## Deploy on Render

1. Push this repo to GitHub (make sure all Python files are committed).

2. Go to https://render.com and sign in.

3. Click **New → Background Worker**.
   - Choose **Deploy from a Git repository** and connect your GitHub repo.
   - Runtime: **Docker** (Render will detect the `Dockerfile` automatically).

4. Set the following **Environment Variables** under the service settings:

   | Key | Value |
   |-----|-------|
   | `BOT_TOKEN` | Your Telegram bot token |
   | `API_ID` | `22484497` |
   | `API_HASH` | `c38cb053916c47a97590c244663cbaef` |
   | `OWNER_ID` | Your Telegram user ID |
   | `ADMINS` | Space-separated Telegram user IDs |

   > **Use "Background Worker"**, NOT "Web Service" — the bot has no HTTP port.

5. Click **Deploy**. Render will build the Docker image and start the bot.

> **Note:** The free plan on Render spins down workers after inactivity. Use the **Starter plan ($7/mo)** or higher to keep the bot always running.

---

## Run on Windows (Local)

### Step 1 — Install Python 3.11

Download from https://www.python.org/downloads/  
During install, check **"Add Python to PATH"**.

### Step 2 — Install ffmpeg

1. Download the latest build from https://www.gyan.dev/ffmpeg/builds/ → `ffmpeg-release-essentials.zip`
2. Extract it somewhere, e.g. `C:\ffmpeg`
3. Add `C:\ffmpeg\bin` to your system PATH:
   - Search "Environment Variables" → Edit System Variables → PATH → New → `C:\ffmpeg\bin`

### Step 3 — Install aria2

1. Download from https://github.com/aria2/aria2/releases/latest → `aria2-*-win-64bit-build1.zip`
2. Extract and place `aria2c.exe` in `C:\ffmpeg\bin` (same folder as ffmpeg) so it's already in PATH.

### Step 4 — Install mp4decrypt (Bento4)

1. Download from https://www.bok.net/Bento4/binaries/Bento4-SDK-1-6-0-641.x86_64-pc-windows.zip
2. Extract and copy `mp4decrypt.exe` into the **same folder as main.py**.

### Step 5 — Install Python packages

Open Command Prompt in the bot folder and run:

```
pip install -r requirements.txt
```

### Step 6 — Set environment variables

Create a file named `.env` in the same folder as `main.py` with this content:

```
BOT_TOKEN=your_bot_token_here
API_ID=22484497
API_HASH=c38cb053916c47a97590c244663cbaef
OWNER_ID=your_telegram_id
ADMINS=your_telegram_id
```

### Step 7 — Run the bot

```
python main.py
```

The bot will start and connect to Telegram. Keep the window open while the bot is running.
