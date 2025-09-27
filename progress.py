# progress.py
import time
import math
from pyrogram.errors import FloodWait

# --- Progress Callback for Pyrogram ---
async def progress_for_pyrogram(current, total, ud_type, message, start_time):
    """
    Updates a Telegram message to show download/upload progress.
    """
    now = time.time()
    diff = now - start_time
    if diff < 1:  # Update at most once per second
        return

    percentage = current * 100 / total
    speed = current / diff
    elapsed_time = round(diff)
    time_to_completion = round((total - current) / speed) if speed > 0 else 0
    
    progress_bar = "[{0}{1}]".format(
        ''.join(["⬢" for _ in range(math.floor(percentage / 10))]),
        ''.join(["⬡" for _ in range(10 - math.floor(percentage / 10))])
    )

    progress_text = (
        f"**{ud_type}**\n"
        f"{progress_bar} {percentage:.2f}%\n"
        f"**Total:** {humanbytes(total)}\n"
        f"**Done:** {humanbytes(current)}\n"
        f"**Speed:** {humanbytes(speed)}/s\n"
        f"**ETA:** {time_formatter(time_to_completion)}"
    )

    try:
        await message.edit_text(text=progress_text)
    except FloodWait as e:
        time.sleep(e.value)
    except Exception:
        pass

# --- Progress Callback Class for Boto3 (Wasabi) ---
class Boto3Progress:
    """
    A class to handle progress reporting for Boto3 uploads to Telegram.
    """
    def __init__(self, message, file_size, loop):
        self._message = message
        self._size = file_size
        self._seen_so_far = 0
        self._loop = loop
        self._start_time = time.time()
        self._last_update_time = 0

    def __call__(self, bytes_amount):
        now = time.time()
        if now - self._last_update_time < 2:  # Update at most every 2 seconds
            return
            
        self._seen_so_far += bytes_amount
        percentage = (self._seen_so_far / self._size) * 100
        
        diff = now - self._start_time
        speed = self._seen_so_far / diff if diff > 0 else 0
        
        progress_bar = "[{0}{1}]".format(
            ''.join(["⬢" for _ in range(math.floor(percentage / 10))]),
            ''.join(["⬡" for _ in range(10 - math.floor(percentage / 10))])
        )

        progress_text = (
            f"**Uploading to Wasabi...**\n"
            f"{progress_bar} {percentage:.2f}%\n"
            f"**Total:** {humanbytes(self._size)}\n"
            f"**Done:** {humanbytes(self._seen_so_far)}\n"
            f"**Speed:** {humanbytes(speed)}/s"
        )
        
        # Schedule the message edit on the main asyncio event loop
        asyncio.run_coroutine_threadsafe(
            self._message.edit_text(progress_text), self._loop
        )
        self._last_update_time = now

# --- Helper Functions ---
def humanbytes(size):
    if not size:
        return "0 B"
    power = 1024
    t_n = 0
    power_dict = {0: " ", 1: "K", 2: "M", 3: "G", 4: "T"}
    while size > power:
        size /= power
        t_n += 1
    return "{:.2f} {}B".format(size, power_dict[t_n])

def time_formatter(seconds: int) -> str:
    minutes, seconds = divmod(seconds, 60)
    hours, minutes = divmod(minutes, 60)
    return f"{hours:02d}:{minutes:02d}:{seconds:02d}"

