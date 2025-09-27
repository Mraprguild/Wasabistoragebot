import os
import time
import asyncio
import math
import boto3
import threading
from botocore.exceptions import NoCredentialsError, ClientError
from config import config
from pyrogram import Client, filters
from pyrogram.types import InlineKeyboardMarkup, InlineKeyboardButton, Message
from pyrogram.errors import FloodWait

# Validate configuration
try:
    config.validate()
    print("✓ Configuration validated successfully")
except ValueError as e:
    print(f"✗ Configuration error: {e}")
    exit(1)

# Bot initialization
app = Client(
    "wasabi_bot",
    api_id=config.API_ID,
    api_hash=config.API_HASH,
    bot_token=config.BOT_TOKEN
)

# --- Boto3 S3 Client Setup ---
def create_s3_client(access_key, secret_key, region):
    """Create S3 client with error handling"""
    try:
        return boto3.client(
            's3',
            endpoint_url=f'https://s3.{region}.wasabisys.com',
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
            region_name=region,
            config=boto3.session.Config(
                retries={'max_attempts': 3},
                connect_timeout=30,
                read_timeout=30
            )
        )
    except Exception as e:
        print(f"Error creating S3 client: {e}")
        return None

# Setup for Server 1
s3_client_1 = create_s3_client(
    config.WASABI_ACCESS_KEY_1, 
    config.WASABI_SECRET_KEY_1, 
    config.WASABI_REGION_1
)

# Setup for Server 2 (if configured)
s3_client_2 = None
if all([config.WASABI_ACCESS_KEY_2, config.WASABI_SECRET_KEY_2, 
         config.WASABI_BUCKET_2, config.WASABI_REGION_2]):
    s3_client_2 = create_s3_client(
        config.WASABI_ACCESS_KEY_2, 
        config.WASABI_SECRET_KEY_2, 
        config.WASABI_REGION_2
    )
    print("✓ Secondary Wasabi server configured")
else:
    print("ℹ Secondary Wasabi server not configured")

# --- Helper Functions ---
def humanbytes(size):
    """Converts bytes to a human-readable format."""
    if not size or size == 0:
        return "0B"
    power = 1024
    power_dict = {0: "B", 1: "KB", 2: "MB", 3: "GB", 4: "TB", 5: "PB"}
    
    for i in range(len(power_dict)):
        if size < power ** (i + 1) or i == len(power_dict) - 1:
            return f"{size / (power ** i):.2f} {power_dict[i]}"
    return f"{size:.2f} B"

def human_time(seconds):
    """Convert seconds to human readable time format"""
    if seconds < 60:
        return f"{seconds:.1f}s"
    elif seconds < 3600:
        return f"{seconds / 60:.1f}m"
    else:
        return f"{seconds / 3600:.1f}h"

class ProgressTracker:
    """Thread-safe progress tracker for uploads"""
    def __init__(self, total_size, message, action):
        self.total_size = total_size
        self.message = message
        self.action = action
        self.start_time = time.time()
        self.transferred = 0
        self.lock = threading.Lock()
        self.last_update_time = self.start_time
        self.last_update_size = 0
        
    def update(self, bytes_transferred):
        with self.lock:
            self.transferred = bytes_transferred
            
    async def get_progress_text(self):
        with self.lock:
            current = self.transferred
            total = self.total_size
            now = time.time()
            elapsed = now - self.start_time
            
            if elapsed == 0:
                return ""
                
            percentage = (current / total) * 100 if total > 0 else 0
            speed = current / elapsed
            
            # Calculate ETA
            if current > 0 and speed > 0:
                remaining = total - current
                eta = remaining / speed
                eta_text = f"**ETA:** `{human_time(eta)}`"
            else:
                eta_text = "**ETA:** `Calculating...`"
            
            progress_bar = "[{0}{1}]".format(
                '█' * int(math.floor(percentage / 5)),  # 20 segments for better granularity
                '░' * (20 - int(math.floor(percentage / 5)))
            )
            
            progress_text = (
                f"**{self.action}**\n"
                f"{progress_bar} {percentage:.1f}%\n"
                f"**Progress:** `{humanbytes(current)} / {humanbytes(total)}`\n"
                f"**Speed:** `{humanbytes(speed)}/s`\n"
                f"**Elapsed:** `{human_time(elapsed)}`\n"
                f"{eta_text}"
            )
            
            return progress_text

async def safe_edit_message(message, text):
    """Safely edit message with flood wait handling"""
    try:
        await message.edit_text(text)
        return True
    except FloodWait as e:
        print(f"Flood wait: Waiting {e.value} seconds")
        await asyncio.sleep(e.value)
        return await safe_edit_message(message, text)
    except Exception as e:
        print(f"Error editing message: {e}")
        return False

async def upload_file_with_progress(s3_client, file_path, bucket_name, key, progress_tracker):
    """Upload file with progress tracking"""
    try:
        # Use boto3's upload_file with custom callback
        s3_client.upload_file(
            file_path,
            bucket_name,
            key,
            Callback=lambda bytes_transferred: progress_tracker.update(bytes_transferred)
        )
        return True
    except Exception as e:
        print(f"Upload error: {e}")
        return False

async def progress_updater(progress_tracker, message, update_interval=2):
    """Update progress message periodically"""
    last_percentage = 0
    
    while progress_tracker.transferred < progress_tracker.total_size:
        current_percentage = (progress_tracker.transferred / progress_tracker.total_size) * 100
        
        # Only update if progress has changed significantly (1% or 2 seconds passed)
        if (abs(current_percentage - last_percentage) >= 1 or 
            time.time() - progress_tracker.last_update_time >= update_interval):
            
            progress_text = await progress_tracker.get_progress_text()
            if progress_text:
                await safe_edit_message(message, progress_text)
                last_percentage = current_percentage
                progress_tracker.last_update_time = time.time()
        
        await asyncio.sleep(0.5)  # Small delay to prevent excessive updates
    
    # Final update
    progress_text = await progress_tracker.get_progress_text()
    await safe_edit_message(message, progress_text)

# --- Bot Handlers ---
@app.on_message(filters.command("start"))
async def start_command(client, message):
    """Handle /start command"""
    welcome_text = (
        "🚀 **Wasabi Upload Bot**\n\n"
        "Send me any file (document, video, or audio) and I'll upload it to Wasabi storage.\n\n"
        "**Features:**\n"
        "• Fast multi-server uploads\n"
        "• Real-time progress tracking\n"
        "• Direct streaming links\n"
        "• 7-day pre-signed URLs\n\n"
        "Just send a file to get started!"
    )
    await message.reply_text(welcome_text)

@app.on_message(filters.command("status"))
async def status_command(client, message):
    """Check bot and server status"""
    status_text = "🔍 **Bot Status**\n\n"
    
    # Check Server 1
    try:
        s3_client_1.head_bucket(Bucket=config.WASABI_BUCKET_1)
        status_text += "✅ **Server 1:** Connected and working\n"
    except Exception as e:
        status_text += f"❌ **Server 1:** Error - {str(e)}\n"
    
    # Check Server 2 if configured
    if s3_client_2:
        try:
            s3_client_2.head_bucket(Bucket=config.WASABI_BUCKET_2)
            status_text += "✅ **Server 2:** Connected and working\n"
        except Exception as e:
            status_text += f"❌ **Server 2:** Error - {str(e)}\n"
    else:
        status_text += "⚪ **Server 2:** Not configured\n"
    
    await message.reply_text(status_text)

@app.on_message(filters.document | filters.video | filters.audio | filters.photo)
async def handle_file(client, message: Message):
    """Handle incoming files and show server selection"""
    # Get file info
    if message.document:
        file = message.document
    elif message.video:
        file = message.video
    elif message.audio:
        file = message.audio
    elif message.photo:
        file = message.photo
    else:
        await message.reply_text("❌ Unsupported file type")
        return
    
    file_name = getattr(file, "file_name", "Unknown")
    file_size = humanbytes(file.file_size)
    
    # Create server selection buttons
    buttons = [
        [InlineKeyboardButton("📤 Server 1", callback_data=f"upload_1_{message.id}")],
    ]
    
    if s3_client_2:
        buttons.append([InlineKeyboardButton("📤 Server 2", callback_data=f"upload_2_{message.id}")])
    
    keyboard = InlineKeyboardMarkup(buttons)
    
    info_text = (
        f"📄 **File Info**\n"
        f"**Name:** `{file_name}`\n"
        f"**Size:** `{file_size}`\n\n"
        f"Select a server to upload to:"
    )
    
    await message.reply_text(info_text, reply_markup=keyboard)

@app.on_callback_query(filters.regex("^upload_"))
async def upload_callback(client, callback_query):
    """Handle server selection and start upload process"""
    await callback_query.answer()
    
    try:
        parts = callback_query.data.split("_")
        server_choice = int(parts[1])
        message_id = int(parts[2])
        
        # Get the original message
        original_message = await client.get_messages(
            callback_query.message.chat.id,
            message_id
        )
        
        if not original_message:
            await callback_query.message.edit_text("❌ Original message not found")
            return
        
        # Determine which server to use
        if server_choice == 1:
            s3_client, bucket_name, region = s3_client_1, config.WASABI_BUCKET_1, config.WASABI_REGION_1
            server_name = "Server 1"
        elif server_choice == 2 and s3_client_2:
            s3_client, bucket_name, region = s3_client_2, config.WASABI_BUCKET_2, config.WASABI_REGION_2
            server_name = "Server 2"
        else:
            await callback_query.message.edit_text("❌ Selected server is not available")
            return
        
        if not s3_client:
            await callback_query.message.edit_text("❌ Server connection error")
            return
        
        # Get file info
        file_obj = (original_message.document or original_message.video or 
                   original_message.audio or original_message.photo)
        
        if not file_obj:
            await callback_query.message.edit_text("❌ No file found in message")
            return
        
        file_size = file_obj.file_size
        file_name = getattr(file_obj, "file_name", f"file_{original_message.id}")
        
        await callback_query.message.edit_text(f"📥 **Downloading from Telegram...**\n\n**File:** `{file_name}`\n**Server:** `{server_name}`")
        
        # Download file
        download_path = None
        try:
            download_start_time = time.time()
            download_path = await original_message.download(
                file_name=file_name,
                progress=lambda current, total: asyncio.create_task(
                    update_download_progress(callback_query.message, current, total, download_start_time, file_name)
                )
            )
        except Exception as e:
            await callback_query.message.edit_text(f"❌ Download failed: {str(e)}")
            return
        
        if not download_path or not os.path.exists(download_path):
            await callback_query.message.edit_text("❌ Download failed: File not found")
            return
        
        # Upload to Wasabi
        await callback_query.message.edit_text(f"📤 **Uploading to {server_name}...**\n\n**File:** `{file_name}`")
        
        try:
            # Create progress tracker
            progress_tracker = ProgressTracker(file_size, callback_query.message, f"Uploading to {server_name}")
            
            # Start progress updates
            progress_task = asyncio.create_task(
                progress_updater(progress_tracker, callback_query.message)
            )
            
            # Start upload
            upload_success = await asyncio.get_event_loop().run_in_executor(
                None,
                lambda: upload_file_with_progress(
                    s3_client, download_path, bucket_name, file_name, progress_tracker
                )
            )
            
            # Wait for progress updates to finish
            progress_tracker.transferred = file_size  # Force completion
            await asyncio.sleep(1)  # Let final update happen
            
            if not upload_success:
                await callback_query.message.edit_text("❌ Upload failed")
                if os.path.exists(download_path):
                    os.remove(download_path)
                return
            
            # Generate links
            stream_link = f"https://s3.{region}.wasabisys.com/{bucket_name}/{file_name}"
            
            presigned_url = s3_client.generate_presigned_url(
                'get_object',
                Params={'Bucket': bucket_name, 'Key': file_name},
                ExpiresIn=604800  # 7 days
            )
            
            success_text = (
                f"✅ **Upload Successful!**\n\n"
                f"**File:** `{file_name}`\n"
                f"**Server:** `{server_name}`\n"
                f"**Size:** `{humanbytes(file_size)}`\n\n"
                f"**🌐 Direct Stream Link:**\n`{stream_link}`\n\n"
                f"**🔗 Pre-signed URL (7 days):**\n`{presigned_url}`"
            )
            
            await callback_query.message.edit_text(success_text)
            
        except Exception as e:
            await callback_query.message.edit_text(f"❌ Upload error: {str(e)}")
        
        finally:
            # Cleanup
            if download_path and os.path.exists(download_path):
                os.remove(download_path)
    
    except Exception as e:
        await callback_query.message.edit_text(f"❌ Error: {str(e)}")

async def update_download_progress(message, current, total, start_time, file_name):
    """Update download progress"""
    if total == 0:
        return
        
    percentage = (current / total) * 100
    elapsed = time.time() - start_time
    speed = current / elapsed if elapsed > 0 else 0
    
    progress_text = (
        f"📥 **Downloading...**\n\n"
        f"**File:** `{file_name}`\n"
        f"**Progress:** `{humanbytes(current)} / {humanbytes(total)}`\n"
        f"**Speed:** `{humanbytes(speed)}/s`\n"
        f"**Completed:** `{percentage:.1f}%`"
    )
    
    try:
        await message.edit_text(progress_text)
    except Exception:
        pass  # Ignore edit errors during rapid updates

@app.on_message(filters.command("help"))
async def help_command(client, message):
    """Show help information"""
    help_text = (
        "🤖 **Wasabi Upload Bot Help**\n\n"
        "**Commands:**\n"
        "• `/start` - Start the bot\n"
        "• `/status` - Check server status\n"
        "• `/help` - Show this help message\n\n"
        "**How to use:**\n"
        "1. Send any file (document, video, audio, photo)\n"
        "2. Choose your preferred server\n"
        "3. Wait for upload to complete\n"
        "4. Get your streaming links!\n\n"
        "**Supported files:** Documents, Videos, Audio, Photos"
    )
    await message.reply_text(help_text)

# --- Main Application ---
async def main():
    """Main application entry point"""
    print("🚀 Starting Wasabi Upload Bot...")
    
    try:
        await app.start()
        print("✅ Bot started successfully!")
        
        # Get bot info
        me = await app.get_me()
        print(f"🤖 Bot: @{me.username}")
        print(f"🆔 ID: {me.id}")
        print("📝 Send a file to the bot to test uploads")
        
        # Keep the bot running
        await asyncio.Event().wait()
        
    except Exception as e:
        print(f"❌ Error starting bot: {e}")
    finally:
        await app.stop()
        print("🛑 Bot stopped")

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n🛑 Bot interrupted by user")
    except Exception as e:
        print(f"❌ Fatal error: {e}")
