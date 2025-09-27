import os
import time
import asyncio
import logging
from collections import defaultdict
from datetime import datetime, timedelta
from pyrogram import Client, filters
from pyrogram.types import Message, ForceReply
from pyrogram.errors import MessageNotModified, FloodWait

from config import config
from wasabi_handler import wasabi
from progress import progress_for_pyrogram, Boto3Progress

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Initialize the Pyrogram client
app = Client(
    "WasabiUploadBot",
    api_id=config.API_ID,
    api_hash=config.API_HASH,
    bot_token=config.BOT_TOKEN
)

# State management
user_states = {}
user_requests = defaultdict(list)

def is_rate_limited(user_id, max_requests=5, time_window=60):
    """Check if user has exceeded rate limit"""
    now = datetime.now()
    user_requests[user_id] = [
        req_time for req_time in user_requests[user_id] 
        if now - req_time < timedelta(seconds=time_window)
    ]
    
    if len(user_requests[user_id]) >= max_requests:
        return True
    
    user_requests[user_id].append(now)
    return False

def validate_filename(filename):
    """Validate filename for security"""
    if not filename or len(filename) > 255:
        return False, "Filename must be between 1-255 characters"
    
    invalid_chars = ['/', '\\', ':', '*', '?', '"', '<', '>', '|']
    if any(char in filename for char in invalid_chars):
        return False, "Filename contains invalid characters"
    
    return True, "Valid"

def get_file_extension(filename):
    """Extract file extension safely"""
    return os.path.splitext(filename)[1].lower()

@app.on_message(filters.command("start"))
async def start_handler(client, message: Message):
    """Handler for the /start command"""
    await message.reply_text(
        "👋 **Wasabi Upload Bot**\n\n"
        "Send me any file, and I'll help you:\n"
        "• Rename it\n• Add custom thumbnails (for videos)\n• Upload to Wasabi storage\n\n"
        "**Supported formats:** Documents, Videos, Archives\n"
        "**Max size:** 2GB\n\n"
        "Just send a file to get started!"
    )

@app.on_message(filters.command("help"))
async def help_handler(client, message: Message):
    """Handler for the /help command"""
    await message.reply_text(
        "**How to use:**\n\n"
        "1. Send any file (document/video)\n"
        "2. Provide a new filename when asked\n"
        "3. For videos: Send a thumbnail photo or /skip\n"
        "4. Wait for upload to complete\n\n"
        "**Commands:**\n"
        "/start - Start the bot\n"
        "/help - Show this help\n"
        "/status - Check bot status\n"
    )

@app.on_message(filters.command("status"))
async def status_handler(client, message: Message):
    """Check bot and Wasabi connection status"""
    status_text = "🟢 **Bot Status:** Online\n"
    
    # Check Wasabi connection
    wasabi_status = await wasabi.check_bucket_access()
    status_text += f"🔗 **Wasabi:** {'Connected' if wasabi_status else 'Disconnected'}\n"
    status_text += f"📦 **Bucket:** {config.WASABI_BUCKET}\n"
    status_text += f"🌐 **Region:** {config.WASABI_REGION}"
    
    await message.reply_text(status_text)

@app.on_message(filters.media & ~filters.photo & filters.private)
async def file_handler(client, message: Message):
    """Handles files sent by users"""
    user_id = message.from_user.id
    
    # Rate limiting
    if is_rate_limited(user_id):
        await message.reply_text("⏳ Please wait a minute before sending another file.")
        return
    
    # Check file size
    if message.document and message.document.file_size > config.MAX_FILE_SIZE:
        await message.reply_text(
            f"📁 File too large! Max size: {config.MAX_FILE_SIZE / 1024 / 1024 / 1024:.1f}GB"
        )
        return
    
    if message.video and message.video.file_size > config.MAX_FILE_SIZE:
        await message.reply_text(
            f"🎥 Video too large! Max size: {config.MAX_FILE_SIZE / 1024 / 1024 / 1024:.1f}GB"
        )
        return
    
    # Check file extension
    if message.document:
        file_ext = get_file_extension(message.document.file_name)
        if file_ext not in config.ALLOWED_EXTENSIONS:
            await message.reply_text(
                f"❌ File type not supported. Allowed: {', '.join(config.ALLOWED_EXTENSIONS)}"
            )
            return
    
    # Store file and set state
    user_states[user_id] = {
        "state": "AWAITING_FILENAME",
        "file_message": message,
        "start_time": time.time()
    }
    
    await message.reply_text(
        "📁 File received! Please send me the new filename (without extension):",
        reply_markup=ForceReply(selective=True)
    )

@app.on_message(filters.photo & filters.private)
async def photo_handler(client, message: Message):
    """Handles photos as potential thumbnails"""
    user_id = message.from_user.id
    
    if user_id in user_states and user_states[user_id].get("state") == "AWAITING_THUMBNAIL":
        user_states[user_id]["state"] = "PROCESSING"
        user_states[user_id]["thumbnail_message"] = message
        await process_file(client, message.chat.id, user_id)
    else:
        await message.reply_text("Please send a file first to start the upload process.")

@app.on_message(filters.text & filters.private)
async def text_handler(client, message: Message):
    """Handles text messages (filenames, commands)"""
    user_id = message.from_user.id

    if user_id not in user_states:
        await message.reply_text("Please send a file first to start the upload process.")
        return

    state_data = user_states[user_id]
    current_state = state_data.get("state")

    if current_state == "AWAITING_FILENAME":
        # Validate filename
        is_valid, validation_msg = validate_filename(message.text.strip())
        if not is_valid:
            await message.reply_text(f"❌ {validation_msg}")
            return

        state_data["new_name"] = message.text.strip()
        original_message = state_data["file_message"]
        
        if original_message.video:
            state_data["state"] = "AWAITING_THUMBNAIL"
            await message.reply_text(
                "🎥 Video detected! Send a custom thumbnail (photo) or /skip to use default:",
                reply_markup=ForceReply(selective=True)
            )
        else:
            state_data["state"] = "PROCESSING"
            state_data["thumbnail_message"] = None
            await process_file(client, message.chat.id, user_id)
            
    elif current_state == "AWAITING_THUMBNAIL" and message.text.lower() == "/skip":
        state_data["state"] = "PROCESSING"
        state_data["thumbnail_message"] = None
        await process_file(client, message.chat.id, user_id)

async def process_file(client, chat_id, user_id):
    """Core function to download, rename, and upload the file"""
    state_data = user_states.get(user_id)
    if not state_data or state_data.get("state") != "PROCESSING":
        return

    status_message = await client.send_message(chat_id, "⏳ Processing your request...")

    try:
        # 1. Download from Telegram
        await status_message.edit("📥 Downloading from Telegram...")
        original_media_message = state_data["file_message"]
        
        start_time = state_data.get("start_time", time.time())
        download_path = await client.download_media(
            message=original_media_message,
            progress=progress_for_pyrogram,
            progress_args=("Downloading...", status_message, start_time)
        )

        if not download_path:
            await status_message.edit("❌ Failed to download file")
            return

        await status_message.edit("✅ Download complete!")

        # 2. Download thumbnail if provided
        thumb_path = None
        if state_data.get("thumbnail_message"):
            thumb_path = await client.download_media(
                message=state_data["thumbnail_message"]
            )

        # 3. Rename the file
        file_name = os.path.basename(download_path)
        file_ext = get_file_extension(file_name)
        new_file_name = f"{state_data['new_name']}{file_ext}"
        new_path = os.path.join(os.path.dirname(download_path), new_file_name)
        
        os.rename(download_path, new_path)
        logger.info(f"File renamed to: {new_path}")

        # 4. Upload to Wasabi
        await status_message.edit("☁️ Uploading to Wasabi...")
        file_size = os.path.getsize(new_path)
        loop = asyncio.get_event_loop()
        boto3_progress = Boto3Progress(status_message, file_size, loop)

        upload_success = await wasabi.upload_file(
            file_path=new_path,
            object_name=new_file_name,
            callback=boto3_progress
        )

        # 5. Finalize
        if upload_success:
            s3_url = f"https://s3.{config.WASABI_REGION}.wasabisys.com/{config.WASABI_BUCKET}/{new_file_name}"
            final_text = (
                f"✅ **Upload Successful!**\n\n"
                f"**File:** `{new_file_name}`\n"
                f"**Size:** {Boto3Progress.humanbytes(file_size)}\n"
                f"**URL:** `{s3_url}`\n"
                f"**Bucket:** `{config.WASABI_BUCKET}`"
            )
            await status_message.edit(final_text, disable_web_page_preview=True)
        else:
            await status_message.edit("❌ **Upload Failed!**\n\nPlease try again later.")
    
    except MessageNotModified:
        pass
    except FloodWait as e:
        await asyncio.sleep(e.value)
    except Exception as e:
        logger.error(f"Error processing file for user {user_id}: {e}", exc_info=True)
        try:
            await status_message.edit(f"❌ **Error:** {str(e)}")
        except Exception:
            pass
            
    finally:
        # Cleanup
        for path_var in ['download_path', 'new_path', 'thumb_path']:
            path = locals().get(path_var)
            if path and os.path.exists(path):
                try:
                    os.remove(path)
                except Exception as e:
                    logger.warning(f"Could not remove {path}: {e}")
        
        if user_id in user_states:
            del user_states[user_id]

async def main():
    """Main function to start the bot"""
    logger.info("🤖 Bot is starting...")
    
    # Test Wasabi connection
    try:
        if await wasabi.check_bucket_access():
            logger.info("✅ Wasabi connection verified")
        else:
            logger.error("❌ Wasabi connection failed")
    except Exception as e:
        logger.error(f"❌ Wasabi initialization error: {e}")
    
    await app.start()
    logger.info("✅ Bot has started successfully!")
    
    # Set bot commands
    await app.set_bot_commands([
        ("start", "Start the bot"),
        ("help", "Get help"),
        ("status", "Check bot status")
    ])
    
    me = await app.get_me()
    logger.info(f"🤖 Bot @{me.username} is ready!")
    
    await asyncio.Event().wait()  # Keep running

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("👋 Bot shutting down...")
    except Exception as e:
        logger.error(f"💥 Fatal error: {e}")
