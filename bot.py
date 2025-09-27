import os
import time
import asyncio
import secrets
import string
import traceback
from typing import Union

import boto3
from botocore.config import Config as BotoConfig
from botocore.exceptions import ClientError
from pyrogram import Client, filters
from pyrogram.types import Message
from pyrogram.errors import RPCError

# Import configuration
try:
    from config import config
except ImportError:
    print("Error: config.py not found. Please create it with your credentials.")
    exit()

# --- Utility Functions ---
def format_bytes(size: int) -> str:
    """Converts bytes to a human-readable format."""
    if not size or size == 0:
        return "0 B"
    
    power = 1024
    power_labels = {0: 'B', 1: 'KB', 2: 'MB', 3: 'GB', 4: 'TB'}
    
    for n in range(len(power_labels) - 1, -1, -1):
        if size >= (power ** n):
            size = size / (power ** n)
            return f"{size:.2f} {power_labels[n]}"
    
    return f"{size} B"

def generate_safe_filename(original_name: str) -> str:
    """Generate a safe filename to avoid path traversal and conflicts."""
    # Extract file extension
    name, ext = os.path.splitext(original_name)
    
    # Generate random filename with timestamp
    timestamp = int(time.time())
    random_str = ''.join(secrets.choice(string.ascii_letters + string.digits) for _ in range(8))
    
    # Clean the original name for safety
    safe_name = "".join(c for c in name if c.isalnum() or c in (' ', '-', '_')).rstrip()
    safe_name = safe_name[:50]  # Limit length
    
    return f"{timestamp}_{random_str}_{safe_name}{ext}"

# --- Bot Initialization ---
app = Client(
    "wasabi_bot",
    api_id=config.API_ID,
    api_hash=config.API_HASH,
    bot_token=config.BOT_TOKEN,
    workers=4,
    max_concurrent_transmissions=3
)

# --- Wasabi S3 Client Initialization ---
wasabi_endpoint_url = f'https://s3.{config.WASABI_REGION}.wasabisys.com'

try:
    s3_client = boto3.client(
        's3',
        endpoint_url=wasabi_endpoint_url,
        aws_access_key_id=config.WASABI_ACCESS_KEY,
        aws_secret_access_key=config.WASABI_SECRET_KEY,
        config=BotoConfig(
            signature_version='s3v4',
            retries={
                'max_attempts': 3,
                'mode': 'standard'
            },
            read_timeout=300,
            connect_timeout=300
        )
    )
    
    # Test connection
    s3_client.head_bucket(Bucket=config.WASABI_BUCKET)
    print("✅ Successfully connected to Wasabi bucket")
    
except ClientError as e:
    print(f"❌ Wasabi connection error: {e}")
    exit()
except Exception as e:
    print(f"❌ Unexpected error connecting to Wasabi: {e}")
    exit()

# --- Progress Tracking ---
last_update_time = {}

async def progress_callback(current: int, total: int, message: Message, action: str):
    """Updates the user on progress with throttling."""
    message_identifier = (message.chat.id, message.id)
    now = time.time()
    
    # Throttle updates to every 3 seconds
    if message_identifier not in last_update_time or now - last_update_time[message_identifier] > 3:
        last_update_time[message_identifier] = now
        
        percentage = (current / total) * 100 if total > 0 else 0
        filled_length = int(percentage / 5)
        progress_bar = "█" * filled_length + "░" * (20 - filled_length)
        
        status_text = (
            f"**{action} Progress**\n"
            f"`{format_bytes(current)} / {format_bytes(total)}`\n"
            f"[{progress_bar}] {percentage:.1f}%\n"
            f"**File:** `{message.document.file_name if message.document else 'Unknown'}`"
        )
        
        try:
            await message.edit_text(status_text)
        except RPCError:
            pass  # Ignore message not modified errors

# --- Bot Command Handlers ---

@app.on_message(filters.command("start"))
async def start_handler(client, message: Message):
    """Welcome message."""
    await message.reply_text(
        "**🤖 Telegram to Wasabi Uploader Bot**\n\n"
        "Send me any file and I'll upload it to Wasabi storage!\n\n"
        "**Features:**\n"
        "• Support for all file types\n"
        "• Files up to 10GB\n"
        "• Real-time progress updates\n"
        "• Streamable direct links\n\n"
        "**Commands:**\n"
        "/start - Show this message\n"
        "/status - Check bot status\n\n"
        "Just send me a file to get started!"
    )

@app.on_message(filters.command("status"))
async def status_handler(client, message: Message):
    """Check bot status."""
    try:
        # Test Wasabi connection
        s3_client.head_bucket(Bucket=config.WASABI_BUCKET)
        wasabi_status = "✅ Connected"
    except ClientError:
        wasabi_status = "❌ Disconnected"
    
    await message.reply_text(
        f"**Bot Status**\n\n"
        f"**Wasabi Storage:** {wasabi_status}\n"
        f"**Bucket:** `{config.WASABI_BUCKET}`\n"
        f"**Region:** `{config.WASABI_REGION}`\n"
        f"**Max File Size:** `{format_bytes(config.MAX_FILE_SIZE)}`\n\n"
        f"Ready to receive files!"
    )

# --- File Processing ---

@app.on_message(filters.document | filters.video | filters.audio | filters.photo)
async def file_handler(client, message: Message):
    """Handle incoming files."""
    
    media = message.document or message.video or message.audio or message.photo
    if not media:
        await message.reply_text("❌ Unsupported file type.")
        return

    # Get file info
    if message.document:
        file_name = media.file_name
        file_size = media.file_size
    elif message.video:
        file_name = media.file_name or f"video_{int(time.time())}.mp4"
        file_size = media.file_size
    elif message.audio:
        file_name = media.file_name or f"audio_{int(time.time())}.mp3"
        file_size = media.file_size
    else:  # photo
        file_name = f"photo_{int(time.time())}.jpg"
        file_size = media.file_size
    
    if not file_name:
        file_name = f"file_{int(time.time())}"

    # Check file size
    if file_size > config.MAX_FILE_SIZE:
        await message.reply_text(
            f"❌ File too large. Maximum size is {format_bytes(config.MAX_FILE_SIZE)}."
        )
        return

    # Generate safe filename
    safe_filename = generate_safe_filename(file_name)
    file_path = os.path.join(config.DOWNLOAD_DIR, safe_filename)
    os.makedirs(os.path.dirname(file_path), exist_ok=True)

    status_msg = await message.reply_text(
        f"**📥 Downloading File**\n"
        f"**Name:** `{file_name}`\n"
        f"**Size:** `{format_bytes(file_size)}`\n"
        f"**Status:** Preparing..."
    )

    try:
        # Download from Telegram
        download_task = client.download_media(
            message=message,
            file_name=file_path,
            progress=progress_callback,
            progress_args=(status_msg, "Downloading")
        )
        
        if asyncio.iscoroutine(download_task):
            await download_task
        else:
            await asyncio.get_event_loop().run_in_executor(None, lambda: download_task)

        # Verify download
        if not os.path.exists(file_path) or os.path.getsize(file_path) == 0:
            raise Exception("Downloaded file is empty or missing")

        await status_msg.edit_text("✅ Download complete! Uploading to Wasabi...")

        # Upload to Wasabi
        def upload_to_wasabi():
            s3_client.upload_file(
                Filename=file_path,
                Bucket=config.WASABI_BUCKET,
                Key=safe_filename,
                ExtraArgs={
                    'ContentType': getattr(media, 'mime_type', 'application/octet-stream'),
                    'Metadata': {
                        'original-filename': file_name,
                        'uploaded-by': f"telegram:{message.from_user.id}",
                        'upload-time': str(int(time.time()))
                    }
                }
            )

        await asyncio.get_event_loop().run_in_executor(None, upload_to_wasabi)
        
        await status_msg.edit_text("✅ Upload complete! Generating shareable link...")

        # Generate presigned URL
        presigned_url = s3_client.generate_presigned_url(
            'get_object',
            Params={
                'Bucket': config.WASABI_BUCKET,
                'Key': safe_filename,
                'ResponseContentDisposition': f'attachment; filename="{file_name}"'
            },
            ExpiresIn=config.URL_EXPIRATION
        )

        # Success message
        success_text = (
            f"**✅ File Successfully Uploaded!**\n\n"
            f"**📁 File Name:** `{file_name}`\n"
            f"**📊 Size:** `{format_bytes(file_size)}`\n"
            f"**🔗 Link Expires:** {config.URL_EXPIRATION // 86400} days\n\n"
            f"**🌐 Direct Link:**\n"
            f"`{presigned_url}`\n\n"
            f"**💡 Tip:** This link can be used in media players like VLC, MX Player, or web browsers."
        )
        
        await status_msg.edit_text(success_text)

    except ClientError as e:
        error_code = e.response['Error']['Code']
        error_msg = f"**❌ Wasabi Storage Error**\n\n`{error_code}`\n\nPlease check your bucket configuration."
        await status_msg.edit_text(error_msg)
        print(f"Wasabi Error: {e}")

    except RPCError as e:
        await status_msg.edit_text("❌ Telegram API error. Please try again.")
        print(f"Telegram RPC Error: {e}")

    except Exception as e:
        error_msg = f"**❌ Unexpected Error**\n\n`{str(e)}`\n\nPlease try again later."
        await status_msg.edit_text(error_msg)
        print(f"General Error: {traceback.format_exc()}")

    finally:
        # Cleanup downloaded file
        try:
            if os.path.exists(file_path):
                os.remove(file_path)
        except Exception as e:
            print(f"Cleanup error: {e}")

@app.on_message(filters.command("help"))
async def help_handler(client, message: Message):
    """Show help information."""
    await message.reply_text(
        "**ℹ️ Bot Help**\n\n"
        "**How to use:**\n"
        "1. Send any file (document, video, audio, photo)\n"
        "2. Bot will download it with progress updates\n"
        "3. File gets uploaded to Wasabi storage\n"
        "4. Receive a direct, streamable link\n\n"
        "**Supported files:**\n"
        "• Documents (all types)\n"
        "• Videos (MP4, AVI, MKV, etc.)\n"
        "• Audio files (MP3, WAV, etc.)\n"
        "• Photos (JPG, PNG, etc.)\n\n"
        "**Maximum file size:** 10GB\n"
        "**Link validity:** 7 days\n\n"
        "**Commands:**\n"
        "/start - Welcome message\n"
        "/status - Check bot status\n"
        "/help - This message\n\n"
        "Just send a file to get started!"
    )

# --- Main Application ---
async def main():
    """Start the bot."""
    print("🚀 Starting Telegram to Wasabi Uploader Bot...")
    
    # Validate configuration
    if any('your_' in str(value) or value == 1234567 for value in [
        config.API_ID, config.API_HASH, config.BOT_TOKEN,
        config.WASABI_ACCESS_KEY, config.WASABI_SECRET_KEY, config.WASABI_BUCKET
    ]):
        print("❌ Please fill in your actual credentials in config.py")
        return

    try:
        await app.start()
        print("✅ Bot started successfully!")
        print("🤖 Bot is listening for messages...")
        
        # Get bot info
        me = await app.get_me()
        print(f"👤 Bot username: @{me.username}")
        
        # Keep the bot running
        await asyncio.Event().wait()
        
    except Exception as e:
        print(f"❌ Failed to start bot: {e}")
    finally:
        await app.stop()
        print("👋 Bot stopped.")

if __name__ == "__main__":
    # Create downloads directory
    os.makedirs(config.DOWNLOAD_DIR, exist_ok=True)
    
    # Run the bot
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n👋 Bot stopped by user.")
