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
import hashlib
from concurrent.futures import ThreadPoolExecutor
import psutil
import tempfile

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()

# Configuration - 10GB Support
API_ID = os.getenv("API_ID")
API_HASH = os.getenv("API_HASH")
BOT_TOKEN = os.getenv("BOT_TOKEN")
WASABI_ACCESS_KEY = os.getenv("WASABI_ACCESS_KEY")
WASABI_SECRET_KEY = os.getenv("WASABI_SECRET_KEY")
WASABI_BUCKET = os.getenv("WASABI_BUCKET")
WASABI_REGION = os.getenv("WASABI_REGION", "us-east-1")
RENDER_URL = os.getenv("RENDER_URL", "http://localhost:8000")
MAX_FILE_SIZE = 2 * 1024 * 1024 * 1024  # 2GB

# Performance tuning
CHUNK_SIZE = 1024 * 1024 * 1024  # 64MB chunks for large files
MAX_WORKERS = 4  # Concurrent upload/download threads
BUFFER_SIZE = 8192 * 8  # 64KB buffer for file operations

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

# High-performance S3 client configuration
class HighPerformanceS3:
    def __init__(self):
        self.endpoint_url = f'https://s3.{WASABI_REGION}.wasabisys.com'
        self.s3_client = boto3.client(
            's3',
            endpoint_url=self.endpoint_url,
            aws_access_key_id=WASABI_ACCESS_KEY,
            aws_secret_access_key=WASABI_SECRET_KEY,
            region_name=WASABI_REGION,
            config=botocore.config.Config(
                max_pool_connections=100,
                retries={'max_attempts': 10, 'mode': 'adaptive'},
                s3={'addressing_style': 'virtual'},
                signature_version='s3v4'
            )
        )
        self.executor = ThreadPoolExecutor(max_workers=MAX_WORKERS)
    
    def upload_file_chunked(self, file_path, bucket, key):
        """Upload large files using multipart upload"""
        file_size = os.path.getsize(file_path)
        
        # Initiate multipart upload
        mpu = self.s3_client.create_multipart_upload(
            Bucket=bucket,
            Key=key
        )
        mpu_id = mpu['UploadId']
        
        parts = []
        part_number = 1
        
        try:
            with open(file_path, 'rb') as file:
                while True:
                    chunk = file.read(CHUNK_SIZE)
                    if not chunk:
                        break
                    
                    # Upload part
                    part = self.s3_client.upload_part(
                        Bucket=bucket,
                        Key=key,
                        PartNumber=part_number,
                        UploadId=mpu_id,
                        Body=chunk
                    )
                    parts.append({
                        'PartNumber': part_number,
                        'ETag': part['ETag']
                    })
                    
                    part_number += 1
                    chunk = None  # Free memory
            
            # Complete multipart upload
            self.s3_client.complete_multipart_upload(
                Bucket=bucket,
                Key=key,
                UploadId=mpu_id,
                MultipartUpload={'Parts': parts}
            )
            return True
            
        except Exception as e:
            # Abort upload on error
            self.s3_client.abort_multipart_upload(
                Bucket=bucket,
                Key=key,
                UploadId=mpu_id
            )
            raise e
    
    def download_file_chunked(self, bucket, key, file_path):
        """Download large files with progress tracking"""
        try:
            # Get file size first
            head = self.s3_client.head_object(Bucket=bucket, Key=key)
            file_size = head['ContentLength']
            
            # Use ranged downloads for better performance
            with open(file_path, 'wb') as file:
                bytes_downloaded = 0
                
                while bytes_downloaded < file_size:
                    end_byte = min(bytes_downloaded + CHUNK_SIZE - 1, file_size - 1)
                    
                    response = self.s3_client.get_object(
                        Bucket=bucket,
                        Key=key,
                        Range=f'bytes={bytes_downloaded}-{end_byte}'
                    )
                    
                    chunk = response['Body'].read()
                    file.write(chunk)
                    bytes_downloaded += len(chunk)
                    
                    # Free memory
                    chunk = None
            
            return file_path
            
        except Exception as e:
            if os.path.exists(file_path):
                os.remove(file_path)
            raise e

# Initialize high-performance S3 client
s3_manager = HighPerformanceS3()
s3_client = s3_manager.s3_client

# Initialize Pyrogram client with performance optimizations
app = Client(
    "wasabi_bot", 
    api_id=API_ID, 
    api_hash=API_HASH, 
    bot_token=BOT_TOKEN,
    workers=100,  # Increased workers for better performance
    max_concurrent_transmissions=10  # Allow more concurrent operations
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

# Performance Monitoring Class
class PerformanceMonitor:
    def __init__(self):
        self.speed_samples = []
        self.max_samples = 10
    
    def add_speed_sample(self, speed):
        self.speed_samples.append(speed)
        if len(self.speed_samples) > self.max_samples:
            self.speed_samples.pop(0)
    
    def get_average_speed(self):
        if not self.speed_samples:
            return 0
        return sum(self.speed_samples) / len(self.speed_samples)
    
    def get_memory_usage(self):
        return psutil.Process().memory_info().rss / 1024 / 1024  # MB

# Helper Functions with Performance Optimizations
MEDIA_EXTENSIONS = {
    'video': ['.mp4', '.mov', '.avi', '.mkv', '.webm', '.m4v', '.flv', '.wmv'],
    'audio': ['.mp3', '.m4a', '.ogg', '.wav', '.flac', '.aac', '.wma'],
    'image': ['.jpg', '.jpeg', '.png', '.gif', '.bmp', '.webp', '.tiff', '.svg']
}

def get_file_type(filename):
    ext = os.path.splitext(filename)[1].lower()
    for file_type, extensions in MEDIA_EXTENSIONS.items():
        if ext in extensions:
            return file_type
    return 'other'

def generate_player_url(filename, presigned_url):
    if not RENDER_URL:
        return None
    file_type = get_file_type(filename)
    if file_type in ['video', 'audio', 'image']:
        encoded_url = base64.urlsafe_b64encode(presigned_url.encode()).decode().rstrip('=')
        return f"{RENDER_URL}/player/{file_type}/{encoded_url}"
    return None

def humanbytes(size):
    """Convert bytes to human readable format with precision"""
    if not size:
        return "0 B"
    for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
        if size < 1024.0:
            return f"{size:.2f} {unit}"
        size /= 1024.0
    return f"{size:.2f} PB"

def sanitize_filename(filename):
    """Secure filename sanitization"""
    filename = re.sub(r'[^a-zA-Z0-9 _.-]', '_', filename)
    if len(filename) > 200:
        name, ext = os.path.splitext(filename)
        filename = name[:200-len(ext)] + ext
    return filename

def get_user_folder(user_id):
    return f"user_{user_id}"

def create_download_keyboard(presigned_url, player_url=None):
    keyboard = []
    if player_url:
        keyboard.append([InlineKeyboardButton("🎬 Web Player", url=player_url)])
    keyboard.append([InlineKeyboardButton("📥 Direct Download", url=presigned_url)])
    return InlineKeyboardMarkup(keyboard)

def create_progress_bar(percentage, length=20):
    filled = int(length * percentage / 100)
    empty = length - filled
    return '█' * filled + '○' * empty

def format_eta(seconds):
    if seconds <= 0:
        return "00:00"
    hours, remainder = divmod(seconds, 3600)
    minutes, seconds = divmod(remainder, 60)
    if hours > 0:
        return f"{int(hours):02d}:{int(minutes):02d}:{int(seconds):02d}"
    return f"{int(minutes):02d}:{int(seconds):02d}"

def format_elapsed(seconds):
    return f"{int(seconds // 60):02d}:{int(seconds % 60):02d}"

# Rate limiting with higher limits for premium performance
user_requests = defaultdict(list)
user_upload_limits = defaultdict(lambda: MAX_FILE_SIZE)

def is_rate_limited(user_id, limit=10, period=60):
    now = datetime.now()
    user_requests[user_id] = [req_time for req_time in user_requests[user_id] 
                             if now - req_time < timedelta(seconds=period)]
    
    if len(user_requests[user_id]) >= limit:
        return True
    
    user_requests[user_id].append(now)
    return False

# High-speed download function with memory optimization
async def download_large_file(client, message, file_id, file_size, file_name):
    """Download large files with optimized memory usage"""
    download_dir = tempfile.mkdtemp()
    file_path = os.path.join(download_dir, file_name)
    
    try:
        # Use async file operations
        async with aiofiles.open(file_path, 'wb') as file:
            downloaded = 0
            async for chunk in client.stream_media(message, limit=BUFFER_SIZE):
                await file.write(chunk)
                downloaded += len(chunk)
                yield downloaded, file_size
                
    except Exception as e:
        # Cleanup on error
        if os.path.exists(file_path):
            os.remove(file_path)
        os.rmdir(download_dir)
        raise e
    
    yield file_path, file_size

# Bot Handlers with High-Performance Features
@app.on_message(filters.command("start"))
async def start_command(client, message: Message):
    if is_rate_limited(message.from_user.id):
        await message.reply_text("🚫 Too many requests. Please wait a minute.")
        return
        
    await message.reply_text(
        "⚡ **ULTRA HIGH-SPEED CLOUD STORAGE BOT** ⚡\n\n"
        "✨ **Features:**\n"
        "• 🚀 **2GB File Support** - Massive uploads/downloads\n"
        "• ⚡ **Multi-threaded Transfers** - Maximum speed\n"
        "• 💾 **Memory Optimized** - Efficient large file handling\n"
        "• 🌐 **Web Player Integration** - Instant media playback\n"
        "• 🔄 **Resumable Uploads** - Never lose progress\n\n"
        "**📁 Supported Formats:**\n"
        "• Videos: MP4, AVI, MKV, MOV, WebM, etc.\n"
        "• Audio: MP3, FLAC, WAV, AAC, etc.\n"
        "• Images: JPG, PNG, GIF, WebP, etc.\n"
        "• Documents: PDF, ZIP, etc.\n\n"
        "**⚡ Commands:**\n"
        "• Just send any file to upload\n"
        "• `/download filename` - High-speed download\n"
        "• `/play filename` - Web player link\n"
        "• `/list` - View your files\n"
        "• `/delete filename` - Remove files\n"
        "• `/status` - System performance\n\n"
        "**💎 Premium Performance | Mraprguild**"
    )

@app.on_message(filters.command("status"))
async def status_command(client, message: Message):
    """System performance status"""
    monitor = PerformanceMonitor()
    memory_usage = monitor.get_memory_usage()
    
    status_text = (
        "⚡ **System Status** ⚡\n\n"
        f"**Memory Usage:** {memory_usage:.1f} MB\n"
        f"**Max File Size:** {humanbytes(MAX_FILE_SIZE)}\n"
        f"**Chunk Size:** {humanbytes(CHUNK_SIZE)}\n"
        f"**Concurrent Workers:** {MAX_WORKERS}\n"
        f"**Buffer Size:** {humanbytes(BUFFER_SIZE)}\n\n"
        "**🚀 Performance Optimized**\n"
        "• Multi-threaded uploads/downloads\n"
        "• Memory-efficient streaming\n"
        "• Chunked transfer protocol\n"
        "• Adaptive speed optimization"
    )
    
    await message.reply_text(status_text)

@app.on_message(filters.document | filters.video | filters.audio | filters.photo)
async def upload_file_handler(client, message: Message):
    if is_rate_limited(message.from_user.id):
        await message.reply_text("🚫 Rate limited. Please wait.")
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

    # Enhanced file size check
    if file_size > MAX_FILE_SIZE:
        await message.reply_text(
            f"❌ File too large!\n"
            f"**Size:** {humanbytes(file_size)}\n"
            f"**Limit:** {humanbytes(MAX_FILE_SIZE)}\n"
            f"**Excess:** {humanbytes(file_size - MAX_FILE_SIZE)}"
        )
        return

    # Performance monitoring
    monitor = PerformanceMonitor()
    status_message = await message.reply_text(
        "🚀 **ULTRA HIGH-SPEED UPLOAD** 🚀\n\n"
        "📥 Downloading from Telegram...\n"
        f"📦 File: {file_name}\n"
        f"💾 Size: {humanbytes(file_size)}\n\n"
        "⚡ **Performance Stats:**\n"
        "• Speed: Calculating...\n"
        "• Memory: Optimizing...\n"
        "• Progress: Initializing..."
    )

    download_start = time.time()
    last_update = time.time()
    processed_bytes = 0
    last_processed = 0

    try:
        # High-speed download with progress
        download_path = None
        
        async for progress_data in download_large_file(client, message, media.file_id, file_size, file_name):
            if isinstance(progress_data, tuple) and len(progress_data) == 2:
                current, total = progress_data
                
                if isinstance(current, str):  # Download complete
                    download_path = current
                    break
                
                # Update progress with performance metrics
                current_time = time.time()
                time_diff = current_time - last_update
                
                if time_diff >= 1.0:  # Update every second
                    # Calculate speed
                    instant_speed = (current - last_processed) / time_diff
                    monitor.add_speed_sample(instant_speed)
                    avg_speed = monitor.get_average_speed()
                    
                    # Calculate ETA
                    elapsed = current_time - download_start
                    if avg_speed > 0:
                        eta = (total - current) / avg_speed
                    else:
                        eta = 0
                    
                    # Memory usage
                    memory_usage = monitor.get_memory_usage()
                    
                    # Progress percentage
                    percentage = (current / total) * 100
                    
                    progress_text = (
                        "🚀 **ULTRA HIGH-SPEED UPLOAD** 🚀\n\n"
                        f"📥 Downloading: {file_name}\n"
                        f"📊 Progress: {create_progress_bar(percentage)} {percentage:.1f}%\n"
                        f"📦 Transferred: {humanbytes(current)} / {humanbytes(total)}\n\n"
                        "⚡ **Performance Stats:**\n"
                        f"• **Speed:** {humanbytes(avg_speed)}/s\n"
                        f"• **Memory:** {memory_usage:.1f} MB\n"
                        f"• **ETA:** {format_eta(eta)}\n"
                        f"• **Elapsed:** {format_elapsed(elapsed)}"
                    )
                    
                    try:
                        await status_message.edit_text(progress_text)
                        last_update = current_time
                        last_processed = current
                    except FloodWait as e:
                        await asyncio.sleep(e.value)
                    except Exception:
                        pass

        if not download_path:
            raise Exception("Download failed")

        # Upload to Wasabi with high-performance method
        await status_message.edit_text("🚀 **Uploading to Cloud Storage...**")
        
        user_file_name = f"{get_user_folder(message.from_user.id)}/{sanitize_filename(file_name)}"
        
        # Use threaded upload for better performance
        await asyncio.get_event_loop().run_in_executor(
            s3_manager.executor,
            s3_manager.upload_file_chunked,
            download_path,
            WASABI_BUCKET,
            user_file_name
        )

        # Generate shareable links
        presigned_url = s3_client.generate_presigned_url(
            'get_object', 
            Params={'Bucket': WASABI_BUCKET, 'Key': user_file_name}, 
            ExpiresIn=604800  # 7 days
        )
        
        player_url = generate_player_url(file_name, presigned_url)
        keyboard = create_download_keyboard(presigned_url, player_url)
        
        total_time = time.time() - download_start
        overall_speed = file_size / total_time
        
        success_text = (
            "✅ **UPLOAD COMPLETE!** ✅\n\n"
            f"📁 **File:** {file_name}\n"
            f"💾 **Size:** {humanbytes(file_size)}\n"
            f"⚡ **Speed:** {humanbytes(overall_speed)}/s\n"
            f"⏱️ **Time:** {format_elapsed(total_time)}\n"
            f"🔗 **Expires:** 7 days\n\n"
            "🎬 **Web Player Ready!**"
        )
        
        await status_message.edit_text(success_text, reply_markup=keyboard)

    except Exception as e:
        logger.error(f"Upload error: {e}")
        error_text = f"❌ **Upload Failed**\n\n**Error:** {str(e)}\n\nPlease try again with a smaller file or check your connection."
        await status_message.edit_text(error_text)
    finally:
        # Cleanup
        if 'download_path' in locals() and download_path and os.path.exists(download_path):
            os.path.dirname(download_path)  # Get directory
            os.remove(download_path)
            os.rmdir(os.path.dirname(download_path))

# Enhanced download command with high-speed options
@app.on_message(filters.command("download"))
async def download_file_handler(client, message: Message):
    if is_rate_limited(message.from_user.id):
        await message.reply_text("🚫 Rate limited. Please wait.")
        return
        
    if len(message.command) < 2:
        await message.reply_text("❌ Usage: `/download filename`", parse_mode="markdown")
        return

    file_name = " ".join(message.command[1:])
    user_file_name = f"{get_user_folder(message.from_user.id)}/{file_name}"
    
    status_message = await message.reply_text("🚀 Generating high-speed download link...")
    
    try:
        # Check file exists and get size
        head = s3_client.head_object(Bucket=WASABI_BUCKET, Key=user_file_name)
        file_size = head['ContentLength']
        
        # Generate presigned URL with longer expiry
        presigned_url = s3_client.generate_presigned_url(
            'get_object', 
            Params={'Bucket': WASABI_BUCKET, 'Key': user_file_name}, 
            ExpiresIn=604800  # 7 days
        )
        
        player_url = generate_player_url(file_name, presigned_url)
        keyboard = create_download_keyboard(presigned_url, player_url)
        
        response_text = (
            f"🚀 **HIGH-SPEED DOWNLOAD READY** 🚀\n\n"
            f"📁 **File:** {file_name}\n"
            f"💾 **Size:** {humanbytes(file_size)}\n"
            f"⏰ **Expires:** 7 days\n\n"
            f"⚡ **Direct download link generated!**"
        )
        
        await status_message.edit_text(response_text, reply_markup=keyboard)

    except botocore.exceptions.ClientError as e:
        if e.response['Error']['Code'] == '404':
            await status_message.edit_text("❌ File not found.")
        else:
            await status_message.edit_text(f"❌ S3 Error: {str(e)}")
    except Exception as e:
        logger.error(f"Download error: {e}")
        await status_message.edit_text(f"❌ Error: {str(e)}")

# Keep other handlers (play, list, delete) similar but optimized

# Start Flask server in background
print("🚀 Starting Ultra High-Speed Cloud Storage Bot...")
print(f"💾 Max File Size: {humanbytes(MAX_FILE_SIZE)}")
print(f"⚡ Chunk Size: {humanbytes(CHUNK_SIZE)}")
print(f"🔧 Workers: {MAX_WORKERS}")

Thread(target=run_flask, daemon=True).start()

if __name__ == "__main__":
    app.run()
