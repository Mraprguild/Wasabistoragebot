import os
import time
import asyncio
import logging
from datetime import datetime, timedelta
from collections import defaultdict
from threading import Thread

from pyrogram import Client, filters
from pyrogram.types import Message, InlineKeyboardButton, InlineKeyboardMarkup
from pyrogram.errors import FloodWait

from utils.s3_client import WasabiClient
from utils.security import SecurityManager
from utils.helpers import HelperFunctions

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Load environment
from dotenv import load_dotenv
load_dotenv()

# Configuration
class Config:
    API_ID = int(os.getenv('API_ID'))
    API_HASH = os.getenv('API_HASH')
    BOT_TOKEN = os.getenv('BOT_TOKEN')
    
    WASABI_ACCESS_KEY = os.getenv('WASABI_ACCESS_KEY')
    WASABI_SECRET_KEY = os.getenv('WASABI_SECRET_KEY')
    WASABI_BUCKET = os.getenv('WASABI_BUCKET')
    WASABI_REGION = os.getenv('WASABI_REGION', 'us-east-1')
    
    RENDER_URL = os.getenv('RENDER_URL', 'http://localhost:8000')
    MAX_FILE_SIZE = int(os.getenv('MAX_FILE_SIZE', 2147483648))
    MAX_USER_FILES = int(os.getenv('MAX_USER_FILES', 100))
    RATE_LIMIT = int(os.getenv('RATE_LIMIT_PER_MINUTE', 5))

# Initialize clients
app = Client("wasabi_bot", api_id=Config.API_ID, api_hash=Config.API_HASH, bot_token=Config.BOT_TOKEN)
s3_client = WasabiClient(Config.WASABI_ACCESS_KEY, Config.WASABI_SECRET_KEY, Config.WASABI_BUCKET, Config.WASABI_REGION)
security = SecurityManager(os.getenv('SECRET_KEY', 'default-secret'))
helpers = HelperFunctions()

# Rate limiting
user_requests = defaultdict(list)

def is_rate_limited(user_id: int, limit: int = Config.RATE_LIMIT, period: int = 60) -> bool:
    now = datetime.now()
    user_requests[user_id] = [req_time for req_time in user_requests[user_id] 
                             if now - req_time < timedelta(seconds=period)]
    
    if len(user_requests[user_id]) >= limit:
        return True
    
    user_requests[user_id].append(now)
    return False

def create_file_keyboard(presigned_url: str, player_url: str = None, token: str = None) -> InlineKeyboardMarkup:
    """Create inline keyboard for file actions"""
    keyboard = []
    
    if player_url and token:
        secure_player_url = f"{player_url}?token={token}"
        keyboard.append([InlineKeyboardButton("🎬 Secure Web Player", url=secure_player_url)])
    
    keyboard.append([InlineKeyboardButton("📥 Direct Download", url=presigned_url)])
    
    return InlineKeyboardMarkup(keyboard)

@app.on_message(filters.command("start"))
async def start_command(client, message: Message):
    if is_rate_limited(message.from_user.id):
        await message.reply_text("🚫 Too many requests. Please wait a minute.")
        return
        
    welcome_text = """
🚀 **Wasabi Cloud Storage Bot**

**Features:**
• 📁 Upload files up to 2GB
• 🎬 Web player for media files
• 🔒 Secure cloud storage
• 📊 Real-time progress tracking

**Commands:**
/upload - Upload a file
/download <filename> - Download file
/play <filename> - Get web player link
/list - List your files
/delete <filename> - Delete file
/stats - Account statistics

**💎 Owner:** @Sathishkumar33
    """
    
    await message.reply_text(welcome_text)

@app.on_message(filters.document | filters.video | filters.audio | filters.photo)
async def handle_upload(client, message: Message):
    user_id = message.from_user.id
    
    if is_rate_limited(user_id):
        await message.reply_text("🚫 Rate limited. Please wait.")
        return

    # Get file info
    media = message.document or message.video or message.audio or message.photo
    if not media:
        await message.reply_text("❌ Unsupported file type")
        return

    file_size = media.file_size if not message.photo else message.photo.sizes[-1].file_size
    
    if file_size > Config.MAX_FILE_SIZE:
        await message.reply_text(f"❌ File too large. Max: {helpers.humanbytes(Config.MAX_FILE_SIZE)}")
        return

    # Check user file count
    user_folder = helpers.get_user_folder(user_id)
    user_files = s3_client.list_user_files(user_folder)
    if len(user_files) >= Config.MAX_USER_FILES:
        await message.reply_text(f"❌ File limit reached. Max: {Config.MAX_USER_FILES} files")
        return

    # Start upload process
    status_msg = await message.reply_text("📥 Initializing download...")
    
    download_start = time.time()
    last_update = time.time()
    last_processed = 0
    
    async def progress_callback(current, total):
        nonlocal last_update, last_processed
        current_time = time.time()
        
        if current_time - last_update >= 1:  # Update every second
            percentage = (current / total) * 100
            elapsed = current_time - download_start
            speed = (current - last_processed) / (current_time - last_update)
            eta = (total - current) / speed if speed > 0 else 0
            
            progress_text = (
                f"📥 Downloading...\n"
                f"[{helpers.create_progress_bar(percentage)}] {percentage:.1f}%\n"
                f"📊 {helpers.humanbytes(current)} / {helpers.humanbytes(total)}\n"
                f"⚡ {helpers.humanbytes(speed)}/s | ⏱️ {helpers.format_duration(eta)}\n"
                f"🕒 Elapsed: {helpers.format_duration(elapsed)}"
            )
            
            try:
                await status_msg.edit_text(progress_text)
                last_update = current_time
                last_processed = current
            except FloodWait as e:
                await asyncio.sleep(e.value)
            except Exception:
                pass

    try:
        # Download from Telegram
        file_path = await message.download(progress=progress_callback)
        filename = security.sanitize_filename(media.file_name if hasattr(media, 'file_name') else f"file_{message.id}")
        s3_key = f"{user_folder}/{filename}"
        
        # Upload to Wasabi
        await status_msg.edit_text("📤 Uploading to Wasabi...")
        
        success = await asyncio.to_thread(s3_client.upload_file, file_path, s3_key)
        
        if not success:
            await status_msg.edit_text("❌ Upload failed")
            return

        # Generate URLs
        presigned_url = s3_client.generate_presigned_url(s3_key)
        file_type = helpers.get_file_type(filename)
        player_url = None
        
        if file_type in ['video', 'audio', 'image']:
            encoded_url = helpers.encode_url(presigned_url)
            player_url = f"{Config.RENDER_URL}/player/{file_type}/{encoded_url}"
        
        # Generate secure token
        token = security.generate_token(user_id, filename)
        
        # Send success message
        total_time = time.time() - download_start
        response_text = (
            f"✅ **Upload Complete!**\n\n"
            f"📁 **File:** `{filename}`\n"
            f"📦 **Size:** {helpers.humanbytes(file_size)}\n"
            f"⏱️ **Time:** {helpers.format_duration(total_time)}\n"
            f"🔗 **Expires:** 24 hours"
        )
        
        keyboard = create_file_keyboard(presigned_url, player_url, token)
        await status_msg.edit_text(response_text, reply_markup=keyboard)
        
    except Exception as e:
        logger.error(f"Upload error: {e}")
        await status_msg.edit_text(f"❌ Error: {str(e)}")
    finally:
        if 'file_path' in locals() and os.path.exists(file_path):
            os.remove(file_path)

@app.on_message(filters.command("download"))
async def download_file(client, message: Message):
    if is_rate_limited(message.from_user.id):
        await message.reply_text("🚫 Rate limited. Please wait.")
        return
        
    if len(message.command) < 2:
        await message.reply_text("📝 Usage: /download <filename>")
        return

    filename = " ".join(message.command[1:])
    user_folder = helpers.get_user_folder(message.from_user.id)
    s3_key = f"{user_folder}/{filename}"
    
    status_msg = await message.reply_text("🔍 Searching for file...")
    
    try:
        # Verify file exists
        file_info = s3_client.get_file_info(s3_key)
        if not file_info:
            await status_msg.edit_text("❌ File not found")
            return

        # Generate URLs
        presigned_url = s3_client.generate_presigned_url(s3_key)
        file_type = helpers.get_file_type(filename)
        player_url = None
        
        if file_type in ['video', 'audio', 'image']:
            encoded_url = helpers.encode_url(presigned_url)
            player_url = f"{Config.RENDER_URL}/player/{file_type}/{encoded_url}"
        
        token = security.generate_token(message.from_user.id, filename)
        
        response_text = (
            f"📥 **Download Ready**\n\n"
            f"📁 **File:** `{filename}`\n"
            f"📦 **Size:** {helpers.humanbytes(file_info['size'])}\n"
            f"🔗 **Expires:** 24 hours"
        )
        
        keyboard = create_file_keyboard(presigned_url, player_url, token)
        await status_msg.edit_text(response_text, reply_markup=keyboard)
        
    except Exception as e:
        logger.error(f"Download error: {e}")
        await status_msg.edit_text(f"❌ Error: {str(e)}")

# Add other handlers (list, delete, play, stats) similarly...

def run_flask():
    from flask_app import app
    port = int(os.getenv('PORT', 8000))
    app.run(host='0.0.0.0', port=port, debug=False)

if __name__ == "__main__":
    # Start Flask server in background
    flask_thread = Thread(target=run_flask, daemon=True)
    flask_thread.start()
    
    print("🚀 Starting Wasabi Storage Bot...")
    app.run()
