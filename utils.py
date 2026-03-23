import random
import time
from pyrogram.errors import FloodWait
from vars import CREDIT

class Timer:
    def __init__(self, time_between=5):
        self.start_time = time.time()
        self.time_between = time_between

    def can_send(self):
        if time.time() > (self.start_time + self.time_between):
            self.start_time = time.time()
            return True
        return False

timer = Timer(time_between=10)

def hrb(value, digits=2, delim="", postfix=""):
    if value is None:
        return None
    chosen_unit = "B"
    for unit in ("KB", "MB", "GB", "TB"):
        if value > 1000:
            value /= 1024
            chosen_unit = unit
        else:
            break
    return f"{value:.{digits}f}" + delim + chosen_unit + postfix

def hrt(seconds, precision=0):
    pieces = []
    from datetime import timedelta
    value = timedelta(seconds=seconds)

    if value.days:
        pieces.append(f"{value.days}d")

    seconds = value.seconds
    if seconds >= 3600:
        hours = int(seconds / 3600)
        pieces.append(f"{hours}h")
        seconds -= hours * 3600

    if seconds >= 60:
        minutes = int(seconds / 60)
        pieces.append(f"{minutes}m")
        seconds -= minutes * 60

    if seconds > 0 or not pieces:
        pieces.append(f"{seconds}s")

    if not precision:
        return "".join(pieces)

    return "".join(pieces[:precision])


async def progress_bar(current, total, reply, start):
    if not timer.can_send():
        return

    now = time.time()
    elapsed = now - start
    if elapsed < 1:
        return

    base_speed = current / elapsed
    speed = base_speed + (9 * 1024 * 1024)  # +9 MB/s

    percent = (current / total) * 100
    eta_seconds = (total - current) / speed if speed > 0 else 0

    bar_length = 12

    # Calculate how many blocks filled (float for smoothness)
    progress_ratio = current / total
    filled_length = progress_ratio * bar_length

    progress_bar_list = []

    for i in range(bar_length):
        # Position index in bar (0-based)
        pos = i + 1

        if pos <= int(filled_length):
            # Fully filled block — decide green or orange
            # If in last 30% of progress, make green
            if progress_ratio > 0.7:
                # The left part turns green from 70% progress onwards
                progress_bar_list.append("🔳")
            else:
                # Between 0 and 70% progress filled blocks are orange
                progress_bar_list.append("🔲")
        elif pos - 1 < filled_length < pos:
            # Partial fill (between blocks), show orange as partial progress
            progress_bar_list.append("◻️")
        else:
            # Not filled yet, show white block
            progress_bar_list.append("◻️")

    # Extra tweak: if progress > 90%, all filled blocks green
    if progress_ratio >= 0.9:
        for i in range(int(filled_length)):
            progress_bar_list[i] = "◻️"

    progress_bar_str = "".join(progress_bar_list)

    msg = (
        f"╭───⌯═════ 𝐁𝐎𝐓 𝐏𝐑𝐎𝐆𝐑𝐄𝐒𝐒 ═════⌯\n"
        f"├  **{percent:.1f}%** `{progress_bar_str}`\n├\n"
        f"├ 🛜  𝗦𝗣𝗘𝗘𝗗 ➤ | {hrb(speed)}/s \n"
        f"├ ♻️  𝗣𝗥𝗢𝗖𝗘𝗦𝗦𝗘𝗗 ➤ | {hrb(current)} \n"
        f"├ 📦  𝗦𝗜𝗭𝗘 ➤ | {hrb(total)} \n"
        f"├ ⏰  𝗘𝗧𝗔 ➤ | {hrt(eta_seconds, 1)}\n\n"
        f"╰─═══ **𝕞𝕚𝕘𝕙𝕥𝕪 𝕒𝕥𝕠𝕞**═══─╯"
    )

    try:
        await reply.edit(msg)
    except FloodWait as e:
        time.sleep(e.x)







