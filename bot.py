import os
import time
import asyncio
import math
import boto3
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

# Convert API_ID to integer
try:
    API_ID = int(config.API_ID)
except ValueError:
    print("✗ API_ID must be a valid integer")
    exit(1)

# Bot initialization
app = Client(
    "wasabi_bot",
    api_id=API_ID,
    api_hash=config.API_HASH,
    bot_token=config.BOT_TOKEN
)

# --- Boto3 S3 Client Setup ---
def create_s3_client(access_key, secret_key, region, bucket_name):
    """Create S3 client with error handling"""
    try:
        client = boto3.client(
            's3',
            endpoint_url=f'https://s3.{region}.wasabisys.com',
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
            region_name=region
        )
        # Test the connection
        client.head_bucket(Bucket=bucket_name)
        print(f"✓ Connected to Wasabi bucket: {bucket_name} in region: {region}")
        return client
    except Exception as e:
        print(f"✗ Error connecting to Wasabi: {e}")
        return None

# Setup for Server 1
s3_client_1 = create_s3_client(
    config.WASABI_ACCESS_KEY_1, 
    config.WASABI_SECRET_KEY_1, 
    config.WASABI_REGION_1,
    config.WASABI_BUCKET_1
)

if not s3_client_1:
    print("✗ Failed to connect to primary Wasabi server")
    exit(1)

# Setup for Server 2 (if configured)
s3_client_2 = None
if all([config.WASABI_ACCESS_KEY_2, config.WASABI_SECRET_KEY_2, 
         config.WASABI_BUCKET_2, config.WASABI_REGION_2]):
    s3_client_2 = create_s3_client(
        config.WASABI_ACCESS_KEY_2, 
        config.WASABI_SECRET_KEY_2, 
        config.WASABI_REGION_2,
        config.WASABI_BUCKET_2
    )
    if s3_client_2:
        print("✓ Secondary Wasabi server configured")
    else:
        print("⚠ Secondary Wasabi server configuration failed")
else:
    print("ℹ Secondary Wasabi server not configured")

# --- Helper Functions ---
def humanbytes(size):
    """Converts bytes to a human-readable format."""
    if not size or size == 0:
        return "0B"
    power = 1024
    power_dict = {0: "B", 1: "KB", 2: "MB", 3: "GB", 4: "TB"}
    
    for i in range(len(power_dict)):
        if size < power ** (i + 1) or i == len(power_dict) - 1:
            size = size / (power ** i)
            return f"{size:.2f} {power_dict[i]}"

async def progress_callback(current, total, message, start_time, action="Processing"):
    """Simple progress callback for downloads"""
    try:
        now = time.time()
        diff = now - start_time
        
        if diff == 0 or total == 0:
            return
            
        percentage = (current / total) * 100
        speed = current / diff
        elapsed_time = round(diff)
        
        # Estimate remaining time
        if current > 0 and speed > 0:
            remaining = total - current
            eta = remaining / speed
            eta_text = f"**ETA:** `{round(eta)}s`"
        else:
            eta_text = ""
        
        progress_bar = "[{0}{1}]".format(
            '█' * int(percentage / 10),
            '░' * (10 - int(percentage / 10))
        )
        
        progress_text = (
            f"**{action}**\n"
            f"{progress_bar} {percentage:.1f}%\n"
            f"**Progress:** `{humanbytes(current)} / {humanbytes(total)}`\n"
            f"**Speed:** `{humanbytes(speed)}/s`\n"
            f"**Elapsed:** `{elapsed_time}s`\n"
            f"{eta_text}"
        )
        
        # Update message only if significant progress has been made
        await message.edit_text(progress_text)
    except Exception as e:
        # Ignore errors during progress updates
        pass

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

@app.on_message(filters.document | filters.video | filters.audio)
async def handle_file(client, message: Message):
    """Handle incoming files and show server selection"""
    # Get file info
    if message.document:
        file = message.document
        file_name = file.file_name or "Document"
    elif message.video:
        file = message.video
        file_name = file.file_name or "Video"
    elif message.audio:
        file = message.audio
        file_name = file.file_name or "Audio"
    else:
        await message.reply_text("❌ Unsupported file type")
        return
    
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
    await callback_query.answer("Starting upload...")
    
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
            s3_client = s3_client_1
            bucket_name = config.WASABI_BUCKET_1
            region = config.WASABI_REGION_1
            server_name = "Server 1"
        elif server_choice == 2 and s3_client_2:
            s3_client = s3_client_2
            bucket_name = config.WASABI_BUCKET_2
            region = config.WASABI_REGION_2
            server_name = "Server 2"
        else:
            await callback_query.message.edit_text("❌ Selected server is not available")
            return
        
        # Get file info
        if original_message.document:
            file_obj = original_message.document
        elif original_message.video:
            file_obj = original_message.video
        elif original_message.audio:
            file_obj = original_message.audio
        else:
            await callback_query.message.edit_text("❌ No file found in message")
            return
        
        file_size = file_obj.file_size
        file_name = file_obj.file_name or f"file_{original_message.id}"
        
        # Download file
        download_msg = await callback_query.message.edit_text(
            f"📥 **Downloading from Telegram...**\n\n"
            f"**File:** `{file_name}`\n"
            f"**Size:** `{humanbytes(file_size)}`\n"
            f"**Server:** `{server_name}`\n\n"
            f"Please wait..."
        )
        
        download_path = None
        try:
            download_start = time.time()
            download_path = await original_message.download(
                file_name=file_name,
                progress=lambda current, total: asyncio.create_task(
                    progress_callback(current, total, download_msg, download_start, "Downloading")
                )
            )
            
            if not download_path or not os.path.exists(download_path):
                await callback_query.message.edit_text("❌ Download failed: File not found")
                return
                
        except Exception as e:
            await callback_query.message.edit_text(f"❌ Download failed: {str(e)}")
            return
        
        # Upload to Wasabi
        upload_msg = await callback_query.message.edit_text(
            f"📤 **Uploading to {server_name}...**\n\n"
            f"**File:** `{file_name}`\n"
            f"**Size:** `{humanbytes(file_size)}`\n\n"
            f"Please wait..."
        )
        
        try:
            upload_start = time.time()
            
            # Simple upload with basic progress
            def upload_file():
                s3_client.upload_file(
                    download_path,
                    bucket_name,
                    file_name
                )
            
            # Run upload in thread to avoid blocking
            await asyncio.get_event_loop().run_in_executor(None, upload_file)
            
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
            await callback_query.message.edit_text(f"❌ Upload failed: {str(e)}")
        
        finally:
            # Cleanup downloaded file
            if download_path and os.path.exists(download_path):
                try:
                    os.remove(download_path)
                except:
                    pass
    
    except Exception as e:
        await callback_query.message.edit_text(f"❌ Error: {str(e)}")

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
        "1. Send any file (document, video, audio)\n"
        "2. Choose your preferred server\n"
        "3. Wait for upload to complete\n"
        "4. Get your streaming links!\n\n"
        "**Supported files:** Documents, Videos, Audio"
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
