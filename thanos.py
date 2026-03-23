import os
import re
import time
import mmap
import shutil
import datetime
import aiohttp
import aiofiles
import asyncio
import logging
import requests
import tgcrypto
import subprocess
import concurrent.futures
from math import ceil
from pyrogram.errors import FloodWait
from utils import progress_bar
from pyrogram import Client, filters
from pyrogram.types import Message
from io import BytesIO
from pathlib import Path  
from Crypto.Cipher import AES
from Crypto.Util.Padding import unpad
from base64 import b64decode
import math
import m3u8
from urllib.parse import urljoin
from vars import *
from db import Database
from compat import (
    IS_WINDOWS, CREATE_NO_WINDOW, find_binary, get_ffmpeg, get_ffprobe,
    get_mp4decrypt, get_aria2c, get_ytdlp,
    get_duration_ffprobe, run_shell_cmd, async_shell_cmd,
)

# Global semaphore: only 1 download/process at a time on low-resource deployments
_DOWNLOAD_SEM = asyncio.Semaphore(1)



def get_duration(filename):
    return get_duration_ffprobe(filename)


def get_free_disk_mb(path=".") -> float:
    """Return free disk space in MB for the given path."""
    stat = os.statvfs(path)
    return (stat.f_bavail * stat.f_frsize) / (1024 * 1024)


def check_disk_space(required_mb: float = 500) -> bool:
    """Return True if enough disk space is available, else False."""
    free = get_free_disk_mb()
    if free < required_mb:
        logging.warning(f"Low disk space: {free:.0f} MB free, need {required_mb} MB.")
        return False
    return True


def cleanup_downloads(keep_mb: float = 600):
    """Delete oldest files in downloads/ until free space >= keep_mb MB."""
    downloads_dir = "downloads"
    if not os.path.isdir(downloads_dir):
        return
    while get_free_disk_mb() < keep_mb:
        files = sorted(
            [
                os.path.join(downloads_dir, f)
                for f in os.listdir(downloads_dir)
                if os.path.isfile(os.path.join(downloads_dir, f))
            ],
            key=os.path.getmtime,
        )
        if not files:
            break
        try:
            os.remove(files[0])
            logging.info(f"Freed space by deleting: {files[0]}")
        except Exception as e:
            logging.warning(f"Could not delete {files[0]}: {e}")
            break

def split_large_video(file_path, max_size_mb=1900):
    size_bytes = os.path.getsize(file_path)
    max_bytes = max_size_mb * 1024 * 1024

    if size_bytes <= max_bytes:
        return [file_path]  # No splitting needed

    duration = get_duration(file_path)
    parts = ceil(size_bytes / max_bytes)
    part_duration = duration / parts
    base_name = file_path.rsplit(".", 1)[0]
    output_files = []

    for i in range(parts):
        output_file = f"{base_name}_part{i+1}.mp4"
        cmd = [
            get_ffmpeg(), "-y",
            "-threads", "2",
            "-i", file_path,
            "-ss", str(int(part_duration * i)),
            "-t", str(int(part_duration)),
            "-c", "copy",
            output_file
        ]
        kwargs = {}
        if IS_WINDOWS:
            kwargs["creationflags"] = CREATE_NO_WINDOW
        subprocess.run(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, **kwargs)
        if os.path.exists(output_file):
            output_files.append(output_file)

    return output_files


def duration(filename):
    return get_duration_ffprobe(filename)


def get_mps_and_keys(api_url):
    response = requests.get(api_url)
    response_json = response.json()
    mpd = response_json.get('mpd_url')
    keys = response_json.get('keys')
    return mpd, keys


   
def exec(cmd):
        process = subprocess.run(cmd, stdout=subprocess.PIPE,stderr=subprocess.PIPE)
        output = process.stdout.decode()
        print(output)
        return output
        #err = process.stdout.decode()
def pull_run(work, cmds):
    safe_workers = min(work, 2)  # cap at 2 threads on low-resource deployments
    with concurrent.futures.ThreadPoolExecutor(max_workers=safe_workers) as executor:
        print("Waiting for tasks to complete")
        fut = executor.map(exec, cmds)
async def aio(url, name):
    k = f'{name}.pdf'
    chunk_size = 512 * 1024  # 512 KB chunks — safe for 512 MB RAM
    async with aiohttp.ClientSession() as session:
        async with session.get(url) as resp:
            if resp.status == 200:
                async with aiofiles.open(k, mode='wb') as f:
                    async for chunk in resp.content.iter_chunked(chunk_size):
                        await f.write(chunk)
    return k


async def download(url, name):
    ka = f'{name}.pdf'
    chunk_size = 512 * 1024  # 512 KB chunks — safe for 512 MB RAM
    async with aiohttp.ClientSession() as session:
        async with session.get(url) as resp:
            if resp.status == 200:
                async with aiofiles.open(ka, mode='wb') as f:
                    async for chunk in resp.content.iter_chunked(chunk_size):
                        await f.write(chunk)
    return ka

async def pdf_download(url, file_name, chunk_size=1024 * 10):
    if os.path.exists(file_name):
        os.remove(file_name)
    r = requests.get(url, allow_redirects=True, stream=True)
    with open(file_name, 'wb') as fd:
        for chunk in r.iter_content(chunk_size=chunk_size):
            if chunk:
                fd.write(chunk)
    return file_name   
   

def parse_vid_info(info):
    info = info.strip()
    info = info.split("\n")
    new_info = []
    temp = []
    for i in info:
        i = str(i)
        if "[" not in i and '---' not in i:
            while "  " in i:
                i = i.replace("  ", " ")
            i.strip()
            i = i.split("|")[0].split(" ",2)
            try:
                if "RESOLUTION" not in i[2] and i[2] not in temp and "audio" not in i[2]:
                    temp.append(i[2])
                    new_info.append((i[0], i[2]))
            except:
                pass
    return new_info


def vid_info(info):
    info = info.strip()
    info = info.split("\n")
    new_info = dict()
    temp = []
    for i in info:
        i = str(i)
        if "[" not in i and '---' not in i:
            while "  " in i:
                i = i.replace("  ", " ")
            i.strip()
            i = i.split("|")[0].split(" ",3)
            try:
                if "RESOLUTION" not in i[2] and i[2] not in temp and "audio" not in i[2]:
                    temp.append(i[2])
                    
                    # temp.update(f'{i[2]}')
                    # new_info.append((i[2], i[0]))
                    #  mp4,mkv etc ==== f"({i[1]})" 
                    
                    new_info.update({f'{i[2]}':f'{i[0]}'})

            except:
                pass
    return new_info


async def download_and_decrypt_video(url, cmd, name, appxkey=None):
    """
    Download an AppX encrypted video and optionally decrypt with mp4decrypt.

    Args:
        url:     Direct CDN URL (.mkv / .m3u8)
        cmd:     yt-dlp fallback command string
        name:    Output base name (no extension)
        appxkey: "KID:KEY" hex string, or plain key, or None to skip decryption

    Returns: path to the final video file
    """
    output_mkv = f"{name}.mkv"
    output_mp4 = f"{name}.mp4"

    # Only normalise non-signed classx.co.in CDN URLs (Akamai mirrors).
    # Do NOT touch appx.co.in URLs — they use Google Cloud CDN URL signing
    # where the Signature is cryptographically bound to the original domain;
    # changing the domain invalidates the signature and causes 404.
    _CLASSX_CDN_FIXES = [
        ("static-trans-v1.classx.co.in", "appx-transcoded-videos-mcdn.akamai.net.in"),
        ("static-trans-v2.classx.co.in", "transcoded-videos-v2.classx.co.in"),
        ("static-rec.classx.co.in",      "appx-recordings-mcdn.akamai.net.in"),
    ]
    for _old_cdn, _new_cdn in _CLASSX_CDN_FIXES:
        if _old_cdn in url:
            logging.info(f"ClassX CDN normalised: {_old_cdn} → {_new_cdn}")
            url = url.replace(_old_cdn, _new_cdn)
            break

    # Derive origin/referer from the actual URL domain so CDN auth works for
    # all classx subdomains (parmaracademyapi, app, static-trans, etc.)
    try:
        from urllib.parse import urlparse as _urlparse
        _parsed = _urlparse(url)
        _origin = f"{_parsed.scheme}://{_parsed.netloc}"
    except Exception:
        _origin = "https://app.classx.co.in"

    logging.info(f"AppX download starting: url={url[:120]} key={appxkey}")

    # For m3u8 URLs: skip curl entirely — curl only fetches the playlist text
    # file, not the video.  Go straight to ffmpeg which handles HLS correctly.
    if '.m3u8' in url:
        logging.info("AppX m3u8 detected — using ffmpeg directly (skipping curl)")
        _ffmpeg = get_ffmpeg()
        _fb_cmd = (
            f'"{_ffmpeg}" -threads 1 -y -hide_banner -loglevel error '
            f'-headers "User-Agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36\\r\\n'
            f'Referer: {_origin}/\\r\\nOrigin: {_origin}\\r\\n" '
            f'-i "{url}" -c copy "{output_mkv}"'
        )
        await async_shell_cmd(_fb_cmd)
        # Check result; if ffmpeg failed, try yt-dlp as a last resort
        if not os.path.exists(output_mkv) or os.path.getsize(output_mkv) < 10_000:
            logging.warning("ffmpeg HLS download failed — trying yt-dlp fallback")
            _ytdlp = get_ytdlp()
            _ytdlp_cmd = (
                f'"{_ytdlp}" --no-check-certificate '
                f'--add-header "Referer:{_origin}/" '
                f'--add-header "Origin:{_origin}" '
                f'-R 5 --fragment-retries 5 --concurrent-fragments 2 '
                f'-o "{name}.%(ext)s" "{url}"'
            )
            await async_shell_cmd(_ytdlp_cmd)
            for ext in [".mkv", ".mp4", ".webm"]:
                candidate = f"{name}{ext}"
                if os.path.exists(candidate) and os.path.getsize(candidate) > 10_000:
                    output_mkv = candidate
                    break
    else:
        _curl_headers = (
            '-H "User-Agent: Mozilla/5.0 (Linux; Android 13; Pixel 7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Mobile Safari/537.36" '
            '-H "Accept: */*" '
            '-H "Accept-Language: en-US,en;q=0.9" '
            f'-H "Origin: {_origin}" '
            f'-H "Referer: {_origin}/" '
        )
        dl_cmd = f'curl -L --fail --retry 3 --retry-delay 2 {_curl_headers} -o "{output_mkv}" "{url}"'
        ret_rc, _, _ = await async_shell_cmd(dl_cmd)

        _file_ok = ret_rc == 0 and os.path.exists(output_mkv) and os.path.getsize(output_mkv) > 10_000
        if not _file_ok:
            if os.path.exists(output_mkv):
                _sz = os.path.getsize(output_mkv)
                logging.warning(f"curl produced suspicious file ({_sz} bytes), falling back to yt-dlp")
                os.remove(output_mkv)
            else:
                logging.warning("curl failed for AppX — falling back to yt-dlp")
            _fb_cmd = (
                f'{cmd} -R 5 --fragment-retries 5 '
                f'--add-header "Referer:{_origin}/" '
                f'--add-header "Origin:{_origin}"'
            )
            await async_shell_cmd(_fb_cmd)
            for ext in [".mkv", ".mp4", ".webm"]:
                candidate = f"{name}{ext}"
                if os.path.exists(candidate) and os.path.getsize(candidate) > 10_000:
                    output_mkv = candidate
                    break

    if not os.path.exists(output_mkv) or os.path.getsize(output_mkv) < 10_000:
        raise FileNotFoundError(f"AppX download produced no valid file for: {name}")

    if not appxkey or str(appxkey).strip() in ('', '/d', 'None'):
        ffmpeg_cmd = f'ffmpeg -threads 1 -y -hide_banner -loglevel error -i "{output_mkv}" -c copy "{output_mp4}"'
        ret2_rc, _, _ = await async_shell_cmd(ffmpeg_cmd)
        if ret2_rc == 0 and os.path.exists(output_mp4):
            if output_mkv != output_mp4 and os.path.exists(output_mkv):
                os.remove(output_mkv)
            return output_mp4
        return output_mkv

    mp4decrypt_bin = get_mp4decrypt()
    decrypted = f"{name}_dec.mp4"
    if ":" in str(appxkey):
        kid, key = appxkey.split(":", 1)
        decrypt_cmd = f'"{mp4decrypt_bin}" --key {kid}:{key} --show-progress "{output_mkv}" "{decrypted}"'
    else:
        decrypt_cmd = f'"{mp4decrypt_bin}" --key {appxkey} --show-progress "{output_mkv}" "{decrypted}"'

    await async_shell_cmd(decrypt_cmd)

    if os.path.exists(decrypted):
        if os.path.exists(output_mkv):
            os.remove(output_mkv)
        ffmpeg_cmd = f'ffmpeg -threads 1 -y -hide_banner -loglevel error -i "{decrypted}" -c copy "{output_mp4}"'
        await async_shell_cmd(ffmpeg_cmd)
        if os.path.exists(output_mp4):
            os.remove(decrypted)
            return output_mp4
        return decrypted

    raise FileNotFoundError(f"Decryption failed for: {name}")


async def decrypt_and_merge_video(mpd_url, keys_string, output_path, output_name, quality="720"):
    cleanup_downloads(keep_mb=600)  # free space before download
    async with _DOWNLOAD_SEM:
        try:
            output_path = Path(output_path)
            output_path.mkdir(parents=True, exist_ok=True)

            _ytdlp = get_ytdlp()
            _aria2c = get_aria2c()
            cmd1 = f'"{_ytdlp}" -f "bv[height<={quality}]+ba/b" -o "{output_path}/file.%(ext)s" --allow-unplayable-format --no-check-certificate --concurrent-fragments 2 --external-downloader "{_aria2c}" --downloader-args "aria2c: -x 2 -j 2" "{mpd_url}"'
            print(f"Running command: {cmd1}")
            await async_shell_cmd(cmd1)

            avDir = list(output_path.iterdir())
            print(f"Downloaded files: {avDir}")
            print("Decrypting")

            video_decrypted = False
            audio_decrypted = False

            _mp4decrypt = get_mp4decrypt()
            _ffmpeg = get_ffmpeg()
            for data in avDir:
                if data.suffix == ".mp4" and not video_decrypted:
                    cmd2 = f'"{_mp4decrypt}" {keys_string} --show-progress "{data}" "{output_path}/video.mp4"'
                    print(f"Running command: {cmd2}")
                    await async_shell_cmd(cmd2)
                    if (output_path / "video.mp4").exists():
                        video_decrypted = True
                    data.unlink()
                elif data.suffix == ".m4a" and not audio_decrypted:
                    cmd3 = f'"{_mp4decrypt}" {keys_string} --show-progress "{data}" "{output_path}/audio.m4a"'
                    print(f"Running command: {cmd3}")
                    await async_shell_cmd(cmd3)
                    if (output_path / "audio.m4a").exists():
                        audio_decrypted = True
                    data.unlink()

            if not video_decrypted or not audio_decrypted:
                raise FileNotFoundError("Decryption failed: video or audio file not found.")

            cmd4 = f'"{_ffmpeg}" -threads 1 -i "{output_path}/video.mp4" -i "{output_path}/audio.m4a" -c copy "{output_path}/{output_name}.mp4"'
            print(f"Running command: {cmd4}")
            await async_shell_cmd(cmd4)
            if (output_path / "video.mp4").exists():
                (output_path / "video.mp4").unlink()
            if (output_path / "audio.m4a").exists():
                (output_path / "audio.m4a").unlink()

            filename = output_path / f"{output_name}.mp4"

            if not filename.exists():
                raise FileNotFoundError("Merged video file not found.")

            dur_val = get_duration_ffprobe(str(filename))
            print(f"Duration info: {dur_val}s")

            return str(filename)

        except Exception as e:
            print(f"Error during decryption and merging: {str(e)}")
            raise

async def run(cmd):
    proc = await asyncio.create_subprocess_shell(
        cmd,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE)

    stdout, stderr = await proc.communicate()

    print(f'[{cmd!r} exited with {proc.returncode}]')
    if proc.returncode == 1:
        return False
    if stdout:
        return f'[stdout]\n{stdout.decode()}'
    if stderr:
        return f'[stderr]\n{stderr.decode()}'

    

def old_download(url, file_name, chunk_size = 1024 * 10 * 10):
    if os.path.exists(file_name):
        os.remove(file_name)
    r = requests.get(url, allow_redirects=True, stream=True)
    with open(file_name, 'wb') as fd:
        for chunk in r.iter_content(chunk_size=chunk_size):
            if chunk:
                fd.write(chunk)
    return file_name


def human_readable_size(size, decimal_places=2):
    for unit in ['B', 'KB', 'MB', 'GB', 'TB', 'PB']:
        if size < 1024.0 or unit == 'PB':
            break
        size /= 1024.0
    return f"{size:.{decimal_places}f} {unit}"


def time_name():
    date = datetime.date.today()
    now = datetime.datetime.now()
    current_time = now.strftime("%H%M%S")
    return f"{date} {current_time}.mp4"


async def _download_segment(session, url, semaphore, chunk_size=512 * 1024):
    """Download a single m3u8 segment and return its bytes, using a semaphore to cap concurrency."""
    async with semaphore:
        async with session.get(url) as response:
            data = b""
            async for chunk in response.content.iter_chunked(chunk_size):
                data += chunk
            return data


async def fast_download(url, name):
    """Fast direct download implementation without yt-dlp.
    Streams data to disk in chunks to avoid loading large files into RAM."""
    max_retries = 5
    retry_count = 0
    success = False
    CHUNK_SIZE = 512 * 1024  # 512 KB — memory-safe for 512 MB RAM

    while not success and retry_count < max_retries:
        try:
            if "m3u8" in url:
                async with aiohttp.ClientSession() as session:
                    async with session.get(url) as response:
                        m3u8_text = await response.text()

                    playlist = m3u8.loads(m3u8_text)
                    if playlist.is_endlist:
                        base_url = url.rsplit('/', 1)[0] + '/'
                        output_file = f"{name}.mp4"

                        # Limit concurrent segment fetches to avoid memory spike
                        semaphore = asyncio.Semaphore(4)
                        segment_urls = [
                            urljoin(base_url, seg.uri)
                            for seg in playlist.segments
                        ]

                        # Download segments in small batches and write immediately
                        async with aiofiles.open(output_file, 'wb') as out_f:
                            batch_size = 8
                            for i in range(0, len(segment_urls), batch_size):
                                batch = segment_urls[i:i + batch_size]
                                tasks = [
                                    _download_segment(session, seg_url, semaphore, CHUNK_SIZE)
                                    for seg_url in batch
                                ]
                                results = await asyncio.gather(*tasks)
                                for seg_data in results:
                                    await out_f.write(seg_data)

                        success = True
                        return [output_file]
                    else:
                        _ffmpeg = get_ffmpeg()
                        cmd = (
                            f'"{_ffmpeg}" -threads 1 -hide_banner -loglevel error -stats '
                            f'-i "{url}" -c copy -bsf:a aac_adtstoasc -movflags +faststart "{name}.mp4"'
                        )
                        await async_shell_cmd(cmd)
                        if os.path.exists(f"{name}.mp4"):
                            success = True
                            return [f"{name}.mp4"]
            else:
                # Direct video URL — stream to disk in chunks
                async with aiohttp.ClientSession() as session:
                    async with session.get(url) as response:
                        if response.status == 200:
                            output_file = f"{name}.mp4"
                            async with aiofiles.open(output_file, 'wb') as out_f:
                                async for chunk in response.content.iter_chunked(CHUNK_SIZE):
                                    await out_f.write(chunk)
                            success = True
                            return [output_file]

            if not success:
                print(f"\nAttempt {retry_count + 1} failed, retrying in 3 seconds...")
                retry_count += 1
                await asyncio.sleep(3)

        except Exception as e:
            print(f"\nError during attempt {retry_count + 1}: {str(e)}")
            retry_count += 1
            await asyncio.sleep(3)

    return None

async def download_video(url, cmd, name):
    cleanup_downloads(keep_mb=600)  # free space before download
    retry_count = 0
    max_retries = 2
    last_stderr = ""

    async with _DOWNLOAD_SEM:
      while retry_count < max_retries:
        _aria2c = get_aria2c()
        download_cmd = f'{cmd} -R 25 --fragment-retries 25 --external-downloader "{_aria2c}" --downloader-args "aria2c: -x 2 -j 2"'
        print(download_cmd)
        logging.info(download_cmd)

        rc, _out, last_stderr = await async_shell_cmd(download_cmd)

        if rc == 0:
            break  # success

        bot_keywords = ["Sign in to confirm you're not a bot", "Use cookies-from-browser or cookies"]
        if any(kw.lower() in last_stderr.lower() for kw in bot_keywords):
            raise Exception(f"YouTube Bot Detection: {last_stderr.strip()[-200:]}")

        retry_count += 1
        print(f"⚠️ Download failed (attempt {retry_count}/{max_retries}), retrying in 5s...")
        await asyncio.sleep(5)

    try:
        if os.path.isfile(name):
            return name
        elif os.path.isfile(f"{name}.webm"):
            return f"{name}.webm"
        name = name.split(".")[0]
        if os.path.isfile(f"{name}.mkv"):
            return f"{name}.mkv"
        elif os.path.isfile(f"{name}.mp4"):
            return f"{name}.mp4"
        elif os.path.isfile(f"{name}.mp4.webm"):
            return f"{name}.mp4.webm"

        return name + ".mp4"
    except Exception as exc:
        logging.error(f"Error checking file: {exc}")
        return name 





async def send_vid(bot: Client, m: Message, cc, filename, thumb, name, prog, channel_id, watermark="Thanos", topic_thread_id: int = None):
    try:
        temp_thumb = None  # ✅ Ensure this is always defined for later cleanup

        thumbnail = thumb
        if thumb in ["/d", "no"] or not os.path.exists(thumb):
            temp_thumb = os.path.join("downloads", f"thumb_{os.path.basename(filename)}.jpg")
            
            _ffmpeg = get_ffmpeg()
            _ffprobe = get_ffprobe()
            await async_shell_cmd(
                f'"{_ffmpeg}" -threads 1 -i "{filename}" -ss 00:00:10 -vframes 1 -q:v 2 -y "{temp_thumb}"'
            )

            if os.path.exists(temp_thumb) and (watermark and watermark.strip() != "/d"):
                text_to_draw = watermark.strip()
                try:
                    _probe_rc, probe_out, _ = await async_shell_cmd(
                        f'"{_ffprobe}" -v error -select_streams v:0 -show_entries stream=width -of csv=p=0:s=x "{temp_thumb}"'
                    )
                    probe_out = probe_out.strip()
                    img_width = int(probe_out.split('x')[0]) if 'x' in probe_out else int(probe_out)
                except Exception:
                    img_width = 1280

                # Base size relative to width, then adjust by text length
                base_size = max(28, int(img_width * 0.075))
                text_len = len(text_to_draw)
                if text_len <= 3:
                    font_size = int(base_size * 1.25)
                elif text_len <= 8:
                    font_size = int(base_size * 1.0)
                elif text_len <= 15:
                    font_size = int(base_size * 0.85)
                else:
                    font_size = int(base_size * 0.7)
                font_size = max(32, min(font_size, 120))

                box_h = max(60, int(font_size * 1.6))

                # Simple escaping for single quotes in text
                safe_text = text_to_draw.replace("'", "\\'")

                text_cmd = (
                    f'"{_ffmpeg}" -threads 2 -i "{temp_thumb}" -vf '
                    f'"drawbox=y=0:color=black@0.35:width=iw:height={box_h}:t=fill,'
                    f'drawtext=fontfile=font.ttf:text=\'{safe_text}\':fontcolor=white:'
                    f'fontsize={font_size}:x=(w-text_w)/2:y=(({box_h})-text_h)/2" '
                    f'-c:v mjpeg -q:v 2 -y "{temp_thumb}"'
                )
                await async_shell_cmd(text_cmd)
            
            thumbnail = temp_thumb if os.path.exists(temp_thumb) else None

        await prog.delete(True)  # ⏳ Remove previous progress message

        try:
            reply1 = await bot.send_message(channel_id, f" **Uploading Video:**\n<blockquote>{name}</blockquote>", message_thread_id=topic_thread_id)
        except FloodWait as e:
            await m.reply_text(f"⏳ FloodWait: waiting {e.value} seconds...")
            await asyncio.sleep(e.value)
            reply1 = await bot.send_message(channel_id, f" **Uploading Video:**\n<blockquote>{name}</blockquote>", message_thread_id=topic_thread_id)
        reply = await m.reply_text(f"🖼 **Generating Thumbnail:**\n<blockquote>{name}</blockquote>")

        file_size_mb = os.path.getsize(filename) / (1024 * 1024)
        notify_split = None
        sent_message = None

        if file_size_mb < 2000:
            # 📹 Upload as single video
            dur = int(duration(filename))
            start_time = time.time()

            try:
                sent_message = await bot.send_video(
                    chat_id=channel_id,
                    video=filename,
                    caption=cc,
                    supports_streaming=True,
                    height=720,
                    width=1280,
                    thumb=thumbnail,
                    duration=dur,
                    progress=progress_bar,
                    progress_args=(reply, start_time),
                    message_thread_id=topic_thread_id
                )
            except FloodWait as e:
                await m.reply_text(f"⏳ FloodWait: waiting {e.value} seconds...")
                await asyncio.sleep(e.value)
                sent_message = await bot.send_video(
                    chat_id=channel_id,
                    video=filename,
                    caption=cc,
                    supports_streaming=True,
                    height=720,
                    width=1280,
                    thumb=thumbnail,
                    duration=dur,
                    progress=progress_bar,
                    progress_args=(reply, time.time()),
                    message_thread_id=topic_thread_id
                )
            except Exception:
                try:
                    sent_message = await bot.send_document(
                        chat_id=channel_id,
                        document=filename,
                        caption=cc,
                        progress=progress_bar,
                        progress_args=(reply, start_time),
                        message_thread_id=topic_thread_id
                    )
                except FloodWait as e:
                    await m.reply_text(f"⏳ FloodWait: waiting {e.value} seconds...")
                    await asyncio.sleep(e.value)
                    sent_message = await bot.send_document(
                        chat_id=channel_id,
                        document=filename,
                        caption=cc,
                        progress=progress_bar,
                        progress_args=(reply, time.time()),
                        message_thread_id=topic_thread_id
                    )

            # ✅ Cleanup
            if os.path.exists(filename):
                os.remove(filename)
            await reply.delete(True)
            await reply1.delete(True)

        else:
            # ⚠️ Notify about splitting
            notify_split = await m.reply_text(
                f"⚠️ The video is larger than 2GB ({human_readable_size(os.path.getsize(filename))})\n"
                f"⏳ Splitting into parts before upload..."
            )

            parts = split_large_video(filename)

            try:
                first_part_message = None
                for idx, part in enumerate(parts):
                    part_dur = int(duration(part))
                    part_num = idx + 1
                    total_parts = len(parts)
                    part_caption = f"{cc}\n\n📦 Part {part_num} of {total_parts}"
                    part_filename = f"{name}_Part{part_num}.mp4"

                    upload_msg = await m.reply_text(f"📤 Uploading Part {part_num}/{total_parts}...")

                    try:
                        msg_obj = await bot.send_video(
                            chat_id=channel_id,
                            video=part,
                            caption=part_caption,
                            file_name=part_filename,
                            supports_streaming=True,
                            height=720,
                            width=1280,
                            thumb=thumbnail,
                            duration=part_dur,
                            progress=progress_bar,
                            progress_args=(upload_msg, time.time()),
                            message_thread_id=topic_thread_id
                        )
                        if first_part_message is None:
                            first_part_message = msg_obj
                    except FloodWait as e:
                        await m.reply_text(f"⏳ FloodWait: waiting {e.value} seconds...")
                        await asyncio.sleep(e.value)
                        msg_obj = await bot.send_video(
                            chat_id=channel_id,
                            video=part,
                            caption=part_caption,
                            file_name=part_filename,
                            supports_streaming=True,
                            height=720,
                            width=1280,
                            thumb=thumbnail,
                            duration=part_dur,
                            progress=progress_bar,
                            progress_args=(upload_msg, time.time()),
                            message_thread_id=topic_thread_id
                        )
                        if first_part_message is None:
                            first_part_message = msg_obj
                    except Exception:
                        try:
                            msg_obj = await bot.send_document(
                                chat_id=channel_id,
                                document=part,
                                caption=part_caption,
                                file_name=part_filename,
                                progress=progress_bar,
                                progress_args=(upload_msg, time.time()),
                                message_thread_id=topic_thread_id
                            )
                            if first_part_message is None:
                                first_part_message = msg_obj
                        except FloodWait as e:
                            await m.reply_text(f"⏳ FloodWait: waiting {e.value} seconds...")
                            await asyncio.sleep(e.value)
                            msg_obj = await bot.send_document(
                                chat_id=channel_id,
                                document=part,
                                caption=part_caption,
                                file_name=part_filename,
                                progress=progress_bar,
                                progress_args=(upload_msg, time.time()),
                                message_thread_id=topic_thread_id
                            )
                            if first_part_message is None:
                                first_part_message = msg_obj

                    await upload_msg.delete(True)
                    if os.path.exists(part):
                        os.remove(part)

            except Exception as e:
                raise Exception(f"Upload failed at part {idx + 1}: {str(e)}")

            # ✅ Final messages
            if len(parts) > 1:
                await m.reply_text("✅ Large video successfully uploaded in multiple parts!")

            # Cleanup after split
            await reply.delete(True)
            await reply1.delete(True)
            if notify_split:
                await notify_split.delete(True)
            if os.path.exists(filename):
                os.remove(filename)

            # Return first sent part message
            sent_message = first_part_message

        # 🧹 Cleanup generated thumbnail if applicable
        if thumb in ["/d", "no"] and temp_thumb and os.path.exists(temp_thumb):
            os.remove(temp_thumb)

        return sent_message

    except Exception as err:
        raise Exception(f"send_vid failed: {err}")



async def resolve_appx_url(url, quality="720"):
    """
    Resolve AppX signed URL to a direct CDN video URL.
    URL formats:
      Video: https://appxsignurl-omega.vercel.app/appx/<domain>/<path>.m3u8?usertoken=TOKEN&appxv=2
      PDF:   https://appxsignurl-omega.vercel.app/appx/<domain>/<path>.pdf?pdf=1&usertoken=TOKEN&appxv=2
    Returns: (resolved_url, title, encryption_key, content_type)
             where content_type is "pdf" or "video"
    """
    async with aiohttp.ClientSession() as session:
        async with session.get(url, timeout=aiohttp.ClientTimeout(total=30)) as resp:
            if resp.status != 200:
                raise Exception(f"AppX API returned HTTP {resp.status}")
            data = await resp.json(content_type=None)

    if not data.get("success"):
        raise Exception(f"AppX API error: {data}")

    title = data.get("title", "file")
    content_type = "pdf" if data.get("type") == "pdf" or not data.get("is_video", True) else "video"

    if content_type == "pdf":
        pdf_url = data.get("pdf_url", "")
        if not pdf_url:
            raise Exception("No pdf_url in AppX PDF response")
        logging.info(f"AppX PDF resolved: title={title!r} url={pdf_url[:80]}...")
        return pdf_url, title, "", "pdf"

    encryption_key = data.get("encryption_key", "")
    all_qualities = data.get("all_qualities", [])
    video_url = data.get("video_url", "")

    quality_str = f"{quality}p"

    # Build candidate list: preferred quality first, then all others in order
    preferred = None
    others = []
    for q in all_qualities:
        candidate = q.get("url", "")
        if not candidate:
            continue
        if q.get("quality") == quality_str:
            preferred = candidate
        else:
            others.append(candidate)
    candidates = ([preferred] if preferred else []) + others
    if not candidates and video_url:
        candidates = [video_url]
    if not candidates:
        raise Exception("No video URL found in AppX API response")

    # HEAD-check each candidate; pick the first one the CDN actually serves.
    # Google Cloud CDN signed URLs (appx.co.in) return 404 when the file is
    # missing at that CDN path — pre-checking avoids a slow failed download.
    matched_url = None
    async with aiohttp.ClientSession() as _sess:
        for _cand in candidates:
            # Strip the *KEY suffix before checking — it's not part of the URL
            _check_url = _cand.split("*")[0] if "*" in _cand else _cand
            try:
                async with _sess.head(
                    _check_url,
                    timeout=aiohttp.ClientTimeout(total=10),
                    allow_redirects=True,
                ) as _hr:
                    if _hr.status < 400:
                        matched_url = _cand
                        logging.info(f"AppX CDN OK ({_hr.status}): {_check_url[:80]}")
                        break
                    else:
                        logging.warning(f"AppX CDN {_hr.status} (skipping): {_check_url[:80]}")
            except Exception as _he:
                logging.warning(f"AppX HEAD check failed ({_he}): {_check_url[:80]}")

    if not matched_url:
        raise Exception(
            f"Video not available on CDN — all {len(candidates)} quality URL(s) returned 404. "
            "The content may have been removed or is not yet published by the platform."
        )

    logging.info(f"AppX video resolved: title={title!r} quality={quality_str} url={matched_url[:80]}...")
    return matched_url, title, encryption_key, "video"

async def download_drm_mpd(input_string, quality="720"):
    save_name = None
    output_path = None
    async with _DOWNLOAD_SEM:
        try:
            if "*" not in input_string or ":" not in input_string:
                logging.error(f"Invalid input format: {input_string}")
                return None

            url, remainder = input_string.split("*", 1)
            start_number, kid, key = remainder.split(":", 2)

            if not all([url, start_number, kid, key]):
                logging.error(f"One or more parsed fields are empty: url={url}, startNumber={start_number}")
                return None

            safe_start = re.sub(r'[^A-Za-z0-9_-]', '', start_number)
            if not safe_start:
                safe_start = "0"

            save_name = f"Output_Video_{safe_start}"
            output_path = Path(f"downloads/drm_{safe_start}")
            output_path.mkdir(parents=True, exist_ok=True)

            keys_string = f"--key {kid}:{key}"
            mp4decrypt_path = get_mp4decrypt()

            logging.info(f"Starting DRM MPD download: url={url}, startNumber={start_number}")

            _ytdlp = get_ytdlp()
            dl_cmd = (
                f'"{_ytdlp}" -f "bv[height<={quality}]+ba/b[ext=m4a]/bv+ba/b" '
                f'-o "{output_path}/file.%(ext)s" '
                f'--allow-unplayable-format --no-check-certificate '
                f'--no-part --concurrent-fragments 2 '
                f'"{url}"'
            )
            logging.info(f"Download cmd: {dl_cmd}")
            dl_process = await asyncio.create_subprocess_shell(
                dl_cmd,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
            stdout, stderr = await dl_process.communicate()
            if stdout:
                logging.info(f"[yt-dlp stdout]\n{stdout.decode(errors='replace')}")
            if stderr:
                logging.info(f"[yt-dlp stderr]\n{stderr.decode(errors='replace')}")

            av_files = list(output_path.iterdir())
            logging.info(f"Downloaded files: {av_files}")

            video_decrypted = False
            audio_decrypted = False

            for data in av_files:
                if data.suffix == ".mp4" and not video_decrypted:
                    dec_cmd = f'"{mp4decrypt_path}" {keys_string} --show-progress "{data}" "{output_path}/video.mp4"'
                    logging.info(f"Decrypt video: {dec_cmd}")
                    await async_shell_cmd(dec_cmd)
                    if (output_path / "video.mp4").exists():
                        video_decrypted = True
                    data.unlink()
                elif data.suffix == ".m4a" and not audio_decrypted:
                    dec_cmd = f'"{mp4decrypt_path}" {keys_string} --show-progress "{data}" "{output_path}/audio.m4a"'
                    logging.info(f"Decrypt audio: {dec_cmd}")
                    await async_shell_cmd(dec_cmd)
                    if (output_path / "audio.m4a").exists():
                        audio_decrypted = True
                    data.unlink()

            if not video_decrypted or not audio_decrypted:
                logging.error("Decryption failed: video or audio file not found.")
                _cleanup_temp_dir(output_path)
                return None

            final_file = f"{save_name}.mkv"
            _ffmpeg = get_ffmpeg()
            merge_cmd = f'"{_ffmpeg}" -threads 1 -y -i "{output_path}/video.mp4" -i "{output_path}/audio.m4a" -c copy "{final_file}"'
            logging.info(f"Merge cmd: {merge_cmd}")
            merge_process = await asyncio.create_subprocess_shell(
                merge_cmd,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
            await merge_process.communicate()

            _cleanup_temp_dir(output_path)

            if os.path.isfile(final_file):
                logging.info(f"Download successful: {final_file}")
                return final_file

            logging.error(f"Output file not found after merge.")
            return None

        except ValueError:
            logging.error(f"Failed to parse input string: {input_string}")
            _cleanup_temp_dir(output_path)
            return None
        except Exception as e:
            logging.error(f"download_drm_mpd error: {e}")
            _cleanup_temp_dir(output_path)
            return None


def _cleanup_temp_dir(dir_path):
    if dir_path and os.path.isdir(dir_path):
        shutil.rmtree(dir_path, ignore_errors=True)
        logging.info(f"Cleaned up temp directory: {dir_path}")
