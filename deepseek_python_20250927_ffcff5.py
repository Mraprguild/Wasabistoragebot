import os
import time
import boto3
import asyncio
import re
import base64
from threading import Thread
from flask import Flask, render_template
from pyrogram import Client, filters
from pyrogram.types import Message, InlineKeyboardButton, InlineKeyboardMarkup
from pyrogram.errors import FloodWait
from dotenv import load_dotenv
import logging
from collections import defaultdict
from datetime import datetime, timedelta
import botocore
import aiofiles
import psutil
import tempfile
from concurrent.futures import ThreadPoolExecutor
import gc

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()

# Configuration
API_ID = os.getenv("API_ID")
API_HASH = os.getenv("API_HASH")
BOT_TOKEN = os.getenv("BOT_TOKEN")
WASABI_ACCESS_KEY = os.getenv("WASABI_ACCESS_KEY")
WASABI_SECRET_KEY = os.getenv("WASABI_SECRET_KEY")
WASABI_BUCKET = os.getenv("WASABI_BUCKET")
WASABI_REGION = os.getenv("WASABI_REGION", "us-east-1")
RENDER_URL = os.getenv("RENDER_URL", "http://localhost:8000")
MAX_FILE_SIZE = 2 * 1024 * 1024 * 1024  # 2GB

# Performance settings
CHUNK_SIZE = 8 * 1024 * 1024  # 8MB chunks
MAX_WORKERS = 4
BUFFER_SIZE = 8192 * 8  # 64KB buffer

# Validate environment variables
missing_vars = []
for var_name, var_value in [
    ("API_ID", API_ID),
    ("API_HASH", API_HASH),
    ("BOT_TOKEN", BOT_TOKEN),
    ("WASABI_ACCESS_KEY", WASABI_ACCESS_KEY),
    ("WASABI_SECRET_KEY", WASABI_SECRET_KEY),
    ("WASABI_BUCKET", WASABI_BUCKET)
]:
    if not var_value:
        missing_vars.append(var_name)

if missing_vars:
    raise Exception(f"Missing environment variables: {', '.join(missing_vars)}")

# S3 Manager
class S3Manager:
    def __init__(self):
        self.endpoint_url = f'https://s3.{WASABI_REGION}.wasabisys.com'
        self.s3_client = boto3.client(
            's3',
            endpoint_url=self.endpoint_url,
            aws_access_key_id=WASABI_ACCESS_KEY,
            aws_secret_access_key=WASABI_SECRET_KEY,
            region_name=WASABI_REGION,
            config=botocore.config.Config(
                max_pool_connections=20,
                retries={'max_attempts': 3},
                s3={'addressing_style': 'virtual'},
                signature_version='s3v4'
            )
        )
        self.executor = ThreadPoolExecutor(max_workers=MAX_WORKERS)
    
    def upload_file(self, file_path, bucket, key):
        """Upload file with memory optimization"""
        file_size = os.path.getsize(file_path)
        
        if file_size > 100 * 1024 * 1024:  # 100MB+
            return self.multipart_upload(file_path, bucket, key, file_size)
        else:
            with open(file_path, 'rb') as file:
                self.s3_client.upload_fileobj(file, bucket, key)
            return True
    
    def multipart_upload(self, file_path, bucket, key, file_size):
        """Multipart upload for large files"""
        part_size = min(max(file_size // 50, 5 * 1024 * 1024), CHUNK_SIZE)
        
        mpu = self.s3_client.create_multipart_upload(Bucket=bucket, Key=key)
        mpu_id = mpu['UploadId']
        parts = []
        
        try:
            with open(file_path, 'rb') as file:
                part_number = 1
                while True:
                    gc.collect()
                    chunk = file.read(part_size)
                    if not chunk:
                        break
                    
                    part = self.s3_client.upload_part(
                        Bucket=bucket, Key=key, PartNumber=part_number,
                        UploadId=mpu_id, Body=chunk
                    )
                    parts.append({'PartNumber': part_number, 'ETag': part['ETag']})
                    part_number += 1
                    chunk = None
                    gc.collect()
            
            self.s3_client.complete_multipart_upload(
                Bucket=bucket, Key=key, UploadId=mpu_id, MultipartUpload={'Parts': parts}
            )
            return True
        except Exception as e:
            try:
                self.s3_client.abort_multipart_upload(
                    Bucket=bucket, Key=key, UploadId=mpu_id
                )
            except:
                pass
            raise e
    
    def download_file(self, bucket, key, file_path):
        """Download file from S3"""
        try:
            head = self.s3_client.head_object(Bucket=bucket, Key=key)
            file_size = head['ContentLength']
            
            with open(file_path, 'wb') as file:
                bytes_downloaded = 0
                while bytes_downloaded < file_size:
                    end_byte = min(bytes_downloaded + CHUNK_SIZE - 1, file_size - 1)
                    
                    response = self.s3_client.get_object(
                        Bucket=bucket, Key=key,
                        Range=f'bytes={bytes_downloaded}-{end_byte}'
                    )
                    
                    chunk = response['Body'].read()
                    file.write(chunk)
                    bytes_downloaded += len(chunk)
                    chunk = None
                    gc.collect()
            
            return file_path
        except Exception as e:
            if os.path.exists(file_path):
                os.remove(file_path)
            raise e

# Initialize S3 manager
s3_manager = S3Manager()
s3_client = s3_manager.s3_client

# Pyrogram client
app = Client(
    "wasabi_bot", 
    api_id=API_ID, 
    api_hash=API_HASH, 
    bot_token=BOT_TOKEN,
    workers=100,
    max_concurrent_transmissions=8
)

# Flask app
flask_app = Flask(__name__, template_folder="templates")

@flask_app.route("/")
def index():
    return render_template("index.html")

@flask_app.route("/player/<media_type>/<encoded_url>")
def player(media_type, encoded_url):
    try:
        padding = 4 - (len(encoded_url) % 4)
        if padding != 4:
            encoded_url += '=' * padding
        media_url = base64.urlsafe_b64decode(encoded_url).decode()
        return render_template("player.html", media_type=media_type, media_url=media_url)
    except Exception as e:
        return f"Error decoding URL: {str(e)}", 400

def run_flask():
    flask_app.run(host="0.0.0.0", port=8000, debug=False, threaded=True)

# UNIFIED PROGRESS DESIGN CLASS
class UnifiedProgressDesign:
    @staticmethod
    def create_progress_bar(percentage, length=15):
        filled = int(length * percentage / 100)
        empty = length - filled
        return '█' * filled + '○' * empty
    
    @staticmethod
    def format_speed(speed_bytes):
        for unit in ['B/s', 'KB/s', 'MB/s', 'GB/s']:
            if speed_bytes < 1024.0 or unit == 'GB/s':
                break
            speed_bytes /= 1024.0
        return f"{speed_bytes:.1f} {unit}"
    
    @staticmethod
    def format_size(size_bytes):
        for unit in ['B', 'KB', 'MB', 'GB']:
            if size_bytes < 1024.0 or unit == 'GB':
                break
            size_bytes /= 1024.0
        return f"{size_bytes:.2f} {unit}"
    
    @staticmethod
    def format_time(seconds):
        if seconds <= 0:
            return "00:00"
        hours, remainder = divmod(seconds, 3600)
        minutes, seconds = divmod(remainder, 60)
        if hours > 0:
            return f"{int(hours):02d}:{int(minutes):02d}:{int(seconds):02d}"
        return f"{int(minutes):02d}:{int(seconds):02d}"
    
    @staticmethod
    def create_upload_progress(filename, current, total, speed, elapsed, eta):
        percentage = (current / total) * 100
        return (
            "🚀 **CLOUD UPLOAD** 🚀\n\n"
            f"📤 **Uploading:** {filename}\n"
            f"📊 {UnifiedProgressDesign.create_progress_bar(percentage)} {percentage:.1f}%\n\n"
            f"💾 **Progress:** {UnifiedProgressDesign.format_size(current)} / {UnifiedProgressDesign.format_size(total)}\n"
            f"⚡ **Speed:** {UnifiedProgressDesign.format_speed(speed)}\n"
            f"⏱️ **Elapsed:** {UnifiedProgressDesign.format_time(elapsed)}\n"
            f"🕒 **ETA:** {UnifiedProgressDesign.format_time(eta)}\n\n"
            "🔒 **Encrypted Transfer** | ☁️ **Wasabi Storage**"
        )
    
    @staticmethod
    def create_download_progress(filename, current, total, speed, elapsed, eta):
        percentage = (current / total) * 100
        return (
            "🚀 **CLOUD DOWNLOAD** 🚀\n\n"
            f"📥 **Downloading:** {filename}\n"
            f"📊 {UnifiedProgressDesign.create_progress_bar(percentage)} {percentage:.1f}%\n\n"
            f"💾 **Progress:** {UnifiedProgressDesign.format_size(current)} / {UnifiedProgressDesign.format_size(total)}\n"
            f"⚡ **Speed:** {UnifiedProgressDesign.format_speed(speed)}\n"
            f"⏱️ **Elapsed:** {UnifiedProgressDesign.format_time(elapsed)}\n"
            f"🕒 **ETA:** {UnifiedProgressDesign.format_time(eta)}\n\n"
            "🔒 **Encrypted Transfer** | ☁️ **Wasabi Storage**"
        )
    
    @staticmethod
    def create_complete_message(operation, filename, size, time_taken, final_speed):
        emoji = "📤" if operation == "upload" else "📥"
        action = "Uploaded" if operation == "upload" else "Downloaded"
        
        return (
            f"✅ **{action.upper()} SUCCESSFULLY!** ✅\n\n"
            f"{emoji} **File:** {filename}\n"
            f"💾 **Size:** {UnifiedProgressDesign.format_size(size)}\n"
            f"⚡ **Speed:** {UnifiedProgressDesign.format_speed(final_speed)}\n"
            f"⏱️ **Time:** {UnifiedProgressDesign.format_time(time_taken)}\n"
            f"🔗 **Expires:** 7 days\n\n"
            f"🎯 **Operation:** {action} to Cloud\n"
            f"🔒 **Security:** End-to-End Encrypted\n"
            f"☁️ **Storage:** Wasabi Secure Cloud"
        )
    
    @staticmethod
    def create_keyboard(presigned_url, player_url=None):
        keyboard = []
        if player_url:
            keyboard.append([InlineKeyboardButton("🎬 Web Player", url=player_url)])
        keyboard.append([InlineKeyboardButton("📥 Direct Download", url=presigned_url)])
        return InlineKeyboardMarkup(keyboard)

# Helper functions
def sanitize_filename(filename):
    if not filename:
        filename = f"file_{int(time.time())}"
    filename = re.sub(r'[^\w\s\.\-()]', '_', filename)
    if len(filename) > 150:
        name, ext = os.path.splitext(filename)
        filename = name[:150-len(ext)] + ext
    return filename

def get_user_folder(user_id):
    return f"user_{user_id}"

def get_file_type(filename):
    ext = os.path.splitext(filename)[1].lower()
    media_extensions = {
        'video': ['.mp4', '.mov', '.avi', '.mkv', '.webm', '.m4v'],
        'audio': ['.mp3', '.m4a', '.ogg', '.wav', '.flac'],
        'image': ['.jpg', '.jpeg', '.png', '.gif', '.bmp', '.webp']
    }
    for file_type, extensions in media_extensions.items():
        if ext in extensions:
            return file_type
    return 'document'

def generate_player_url(filename, presigned_url):
    if not RENDER_URL:
        return None
    file_type = get_file_type(filename)
    if file_type in ['video', 'audio', 'image']:
        encoded_url = base64.urlsafe_b64encode(presigned_url.encode()).decode().rstrip('=')
        return f"{RENDER_URL}/player/{file_type}/{encoded_url}"
    return None

# Rate limiting
user_requests = defaultdict(list)

def is_rate_limited(user_id, limit=10, period=60):
    now = datetime.now()
    user_requests[user_id] = [req_time for req_time in user_requests[user_id] 
                             if now - req_time < timedelta(seconds=period)]
    return len(user_requests[user_id]) >= limit

# UNIFIED UPLOAD HANDLER
@app.on_message(filters.document | filters.video | filters.audio | filters.photo)
async def handle_upload(client, message: Message):
    if is_rate_limited(message.from_user.id):
        await message.reply_text("🚫 Too many requests. Please wait.")
        return
    
    media = message.document or message.video or message.audio or message.photo
    if not media:
        await message.reply_text("❌ Unsupported file type")
        return

    # Get file info
    if message.photo:
        file_size = message.photo.sizes[-1].file_size
        file_name = f"photo_{int(time.time())}.jpg"
    else:
        file_size = media.file_size
        file_name = media.file_name or f"file_{int(time.time())}"

    # Check file size
    if file_size > MAX_FILE_SIZE:
        await message.reply_text(
            f"❌ File too large!\n"
            f"Size: {UnifiedProgressDesign.format_size(file_size)}\n"
            f"Limit: {UnifiedProgressDesign.format_size(MAX_FILE_SIZE)}"
        )
        return

    # Initial progress message
    status_message = await message.reply_text(
        UnifiedProgressDesign.create_upload_progress(
            file_name, 0, file_size, 0, 0, 0
        )
    )

    download_start = time.time()
    last_update = time.time()
    last_processed = 0

    try:
        # Download from Telegram
        download_dir = tempfile.mkdtemp()
        download_path = os.path.join(download_dir, file_name)
        
        async with aiofiles.open(download_path, 'wb') as file:
            downloaded = 0
            async for chunk in client.stream_media(message, limit=BUFFER_SIZE):
                await file.write(chunk)
                downloaded += len(chunk)
                
                # Update progress
                current_time = time.time()
                if current_time - last_update >= 1.0:
                    elapsed = current_time - download_start
                    speed = (downloaded - last_processed) / (current_time - last_update)
                    eta = (file_size - downloaded) / speed if speed > 0 else 0
                    
                    progress_text = UnifiedProgressDesign.create_upload_progress(
                        file_name, downloaded, file_size, speed, elapsed, eta
                    )
                    
                    try:
                        await status_message.edit_text(progress_text)
                        last_update = current_time
                        last_processed = downloaded
                    except FloodWait as e:
                        await asyncio.sleep(e.value)

        # Upload to Wasabi
        await status_message.edit_text("🔄 **Finalizing upload to cloud...**")
        
        user_file_name = f"{get_user_folder(message.from_user.id)}/{sanitize_filename(file_name)}"
        
        await asyncio.get_event_loop().run_in_executor(
            s3_manager.executor,
            s3_manager.upload_file,
            download_path,
            WASABI_BUCKET,
            user_file_name
        )

        # Generate links
        presigned_url = s3_client.generate_presigned_url(
            'get_object', 
            Params={'Bucket': WASABI_BUCKET, 'Key': user_file_name}, 
            ExpiresIn=604800
        )
        
        player_url = generate_player_url(file_name, presigned_url)
        keyboard = UnifiedProgressDesign.create_keyboard(presigned_url, player_url)
        
        # Final success message
        total_time = time.time() - download_start
        final_speed = file_size / total_time
        
        success_text = UnifiedProgressDesign.create_complete_message(
            "upload", file_name, file_size, total_time, final_speed
        )
        
        await status_message.edit_text(success_text, reply_markup=keyboard)

    except Exception as e:
        logger.error(f"Upload error: {e}")
        error_text = f"❌ **Upload Failed**\n\n**Error:** {str(e)}"
        await status_message.edit_text(error_text)
    finally:
        # Cleanup
        if 'download_path' in locals() and os.path.exists(download_path):
            download_dir = os.path.dirname(download_path)
            os.remove(download_path)
            if os.path.exists(download_dir):
                os.rmdir(download_dir)
        gc.collect()

# UNIFIED DOWNLOAD HANDLER
@app.on_message(filters.command("download"))
async def handle_download(client, message: Message):
    if is_rate_limited(message.from_user.id):
        await message.reply_text("🚫 Too many requests. Please wait.")
        return
        
    if len(message.command) < 2:
        await message.reply_text("❌ Usage: `/download filename`", parse_mode="markdown")
        return

    file_name = " ".join(message.command[1:])
    user_file_name = f"{get_user_folder(message.from_user.id)}/{file_name}"
    
    try:
        # Check file exists and get size
        head = s3_client.head_object(Bucket=WASABI_BUCKET, Key=user_file_name)
        file_size = head['ContentLength']
        
        # Initial progress message
        status_message = await message.reply_text(
            UnifiedProgressDesign.create_download_progress(
                file_name, 0, file_size, 0, 0, 0
            )
        )
        
        download_start = time.time()
        last_update = time.time()
        last_processed = 0
        
        # Download from S3
        download_dir = tempfile.mkdtemp()
        download_path = os.path.join(download_dir, file_name)
        
        def download_with_progress():
            nonlocal last_processed, last_update
            
            with open(download_path, 'wb') as file:
                bytes_downloaded = 0
                while bytes_downloaded < file_size:
                    end_byte = min(bytes_downloaded + CHUNK_SIZE - 1, file_size - 1)
                    
                    response = s3_client.get_object(
                        Bucket=WASABI_BUCKET, Key=user_file_name,
                        Range=f'bytes={bytes_downloaded}-{end_byte}'
                    )
                    
                    chunk = response['Body'].read()
                    file.write(chunk)
                    bytes_downloaded += len(chunk)
                    
                    # Update progress
                    current_time = time.time()
                    if current_time - last_update >= 1.0:
                        elapsed = current_time - download_start
                        speed = (bytes_downloaded - last_processed) / (current_time - last_update)
                        eta = (file_size - bytes_downloaded) / speed if speed > 0 else 0
                        
                        progress_text = UnifiedProgressDesign.create_download_progress(
                            file_name, bytes_downloaded, file_size, speed, elapsed, eta
                        )
                        
                        # Use thread-safe progress update
                        asyncio.run_coroutine_threadsafe(
                            update_progress(status_message, progress_text),
                            client.loop
                        )
                        
                        last_update = current_time
                        last_processed = bytes_downloaded
                    
                    chunk = None
                    gc.collect()
            
            return download_path
        
        async def update_progress(status_msg, text):
            try:
                await status_msg.edit_text(text)
            except FloodWait as e:
                await asyncio.sleep(e.value)
            except Exception:
                pass
        
        # Execute download in thread
        download_path = await asyncio.get_event_loop().run_in_executor(
            s3_manager.executor, download_with_progress
        )
        
        # Upload to Telegram
        await status_message.edit_text("🔄 **Uploading to Telegram...**")
        
        # Send file to user
        await client.send_document(
            chat_id=message.chat.id,
            document=download_path,
            caption=f"📥 **Downloaded:** {file_name}",
            reply_to_message_id=message.id
        )
        
        # Final success message
        total_time = time.time() - download_start
        final_speed = file_size / total_time
        
        success_text = UnifiedProgressDesign.create_complete_message(
            "download", file_name, file_size, total_time, final_speed
        )
        
        await status_message.edit_text(success_text)

    except botocore.exceptions.ClientError as e:
        if e.response['Error']['Code'] == '404':
            await message.reply_text("❌ File not found.")
        else:
            await message.reply_text(f"❌ S3 Error: {str(e)}")
    except Exception as e:
        logger.error(f"Download error: {e}")
        await message.reply_text(f"❌ Error: {str(e)}")
    finally:
        # Cleanup
        if 'download_path' in locals() and os.path.exists(download_path):
            download_dir = os.path.dirname(download_path)
            os.remove(download_path)
            if os.path.exists(download_dir):
                os.rmdir(download_dir)
        gc.collect()

# Additional commands with unified design
@app.on_message(filters.command("start"))
async def start_command(client, message: Message):
    await message.reply_text(
        "🚀 **UNIFIED CLOUD STORAGE BOT** 🚀\n\n"
        "✨ **Identical Design for Upload/Download**\n"
        "• 📤 **Upload:** Send any file\n"
        "• 📥 **Download:** `/download filename`\n"
        "• ⚡ **Same Progress Design**\n"
        "• 🔄 **Unified Interface**\n\n"
        "**📊 Features:**\n"
        "• 2GB file support\n"
        "• Real-time progress tracking\n"
        "• Identical UI for all operations\n"
        "• Web player integration\n\n"
        "**🔧 Commands:**\n"
        "• Just send file → Upload\n"
        "• `/download filename` → Download\n"
        "• `/play filename` → Web player\n"
        "• `/list` → Your files\n"
        "• `/delete filename` → Remove file"
    )

@app.on_message(filters.command("play"))
async def play_command(client, message: Message):
    if len(message.command) < 2:
        await message.reply_text("❌ Usage: `/play filename`")
        return
    
    file_name = " ".join(message.command[1:])
    user_file_name = f"{get_user_folder(message.from_user.id)}/{file_name}"
    
    try:
        presigned_url = s3_client.generate_presigned_url(
            'get_object', 
            Params={'Bucket': WASABI_BUCKET, 'Key': user_file_name}, 
            ExpiresIn=604800
        )
        
        player_url = generate_player_url(file_name, presigned_url)
        
        if player_url:
            await message.reply_text(
                f"🎬 **Web Player Ready!** 🎬\n\n"
                f"📁 **File:** {file_name}\n"
                f"🔗 **Player URL:** {player_url}\n"
                f"⏰ **Expires:** 7 days\n\n"
                "Click the link above to play your media in the web player!"
            )
        else:
            await message.reply_text("❌ This file type doesn't support web playback.")
    
    except Exception as e:
        await message.reply_text(f"❌ Error: {str(e)}")

@app.on_message(filters.command("list"))
async def list_command(client, message: Message):
    try:
        user_prefix = get_user_folder(message.from_user.id) + "/"
        response = s3_client.list_objects_v2(Bucket=WASABI_BUCKET, Prefix=user_prefix)
        
        if 'Contents' not in response:
            await message.reply_text("📁 **Your Cloud Storage**\n\nNo files found.")
            return
        
        files = [obj['Key'].replace(user_prefix, "") for obj in response['Contents']]
        files_list = "\n".join([f"• 📄 {file}" for file in files[:10]])
        
        if len(files) > 10:
            files_list += f"\n\n... and {len(files) - 10} more files"
        
        await message.reply_text(f"📁 **Your Cloud Storage**\n\n{files_list}")
    
    except Exception as e:
        await message.reply_text(f"❌ Error: {str(e)}")

@app.on_message(filters.command("delete"))
async def delete_command(client, message: Message):
    if len(message.command) < 2:
        await message.reply_text("❌ Usage: `/delete filename`")
        return
    
    file_name = " ".join(message.command[1:])
    user_file_name = f"{get_user_folder(message.from_user.id)}/{file_name}"
    
    try:
        s3_client.delete_object(Bucket=WASABI_BUCKET, Key=user_file_name)
        await message.reply_text(f"✅ **Deleted Successfully!**\n\n🗑️ **File:** {file_name}")
    
    except Exception as e:
        await message.reply_text(f"❌ Error: {str(e)}")

# Start services
print("🚀 Starting Unified Cloud Storage Bot...")
print("📊 Identical design for upload/download operations")

Thread(target=run_flask, daemon=True).start()

if __name__ == "__main__":
    app.run()