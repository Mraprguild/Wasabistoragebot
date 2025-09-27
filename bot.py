import os
import asyncio
import logging
from typing import Dict, List, Tuple
from datetime import datetime
import time

# Third-party imports
from pyrogram import Client, filters
from pyrogram.types import Message, InlineKeyboardMarkup, InlineKeyboardButton
from pyrogram.errors import FloodWait
import tgcrypto
import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Environment variables
class Config:
    # Telegram
    API_ID = int(os.getenv("API_ID"))
    API_HASH = os.getenv("API_HASH")
    BOT_TOKEN = os.getenv("BOT_TOKEN")
    
    # Wasabi Primary Server
    WASABI_ACCESS_KEY_1 = os.getenv("WASABI_ACCESS_KEY_1")
    WASABI_SECRET_KEY_1 = os.getenv("WASABI_SECRET_KEY_1")
    WASABI_BUCKET_1 = os.getenv("WASABI_BUCKET_1")
    WASABI_REGION_1 = os.getenv("WASABI_REGION_1", "us-east-1")
    
    # Wasabi Secondary Server
    WASABI_ACCESS_KEY_2 = os.getenv("WASABI_ACCESS_KEY_2")
    WASABI_SECRET_KEY_2 = os.getenv("WASABI_SECRET_KEY_2")
    WASABI_BUCKET_2 = os.getenv("WASABI_BUCKET_2")
    WASABI_REGION_2 = os.getenv("WASABI_REGION_2", "us-east-1")
    
    # Bot settings
    MAX_FILE_SIZE = 10 * 1024 * 1024 * 1024  # 10GB
    DOWNLOAD_URL_EXPIRY = 3600  # 1 hour

# Wasabi Storage Manager
class WasabiStorageManager:
    def __init__(self):
        self.clients = self._initialize_clients()
        self.buckets = [Config.WASABI_BUCKET_1, Config.WASABI_BUCKET_2]
        
    def _initialize_clients(self) -> List:
        """Initialize both Wasabi S3 clients"""
        clients = []
        
        # Client 1 configuration
        s3_config_1 = Config(
            region_name=Config.WASABI_REGION_1,
            retries={'max_attempts': 3, 'mode': 'standard'},
            s3={'addressing_style': 'virtual'}
        )
        
        client1 = boto3.client(
            's3',
            aws_access_key_id=Config.WASABI_ACCESS_KEY_1,
            aws_secret_access_key=Config.WASABI_SECRET_KEY_1,
            endpoint_url=f'https://s3.{Config.WASABI_REGION_1}.wasabisys.com',
            config=s3_config_1
        )
        clients.append(client1)
        
        # Client 2 configuration
        s3_config_2 = Config(
            region_name=Config.WASABI_REGION_2,
            retries={'max_attempts': 3, 'mode': 'standard'},
            s3={'addressing_style': 'virtual'}
        )
        
        client2 = boto3.client(
            's3',
            aws_access_key_id=Config.WASABI_ACCESS_KEY_2,
            aws_secret_access_key=Config.WASABI_SECRET_KEY_2,
            endpoint_url=f'https://s3.{Config.WASABI_REGION_2}.wasabisys.com',
            config=s3_config_2
        )
        clients.append(client2)
        
        return clients
    
    async def upload_file(self, file_path: str, object_name: str) -> Tuple[bool, str, int]:
        """Upload file to both Wasabi servers simultaneously"""
        file_size = os.path.getsize(file_path)
        server_choice = hash(object_name) % 2  # Distribute files between servers
        
        try:
            # Upload to primary server
            def _upload_to_server(client_idx):
                client = self.clients[client_idx]
                bucket = self.buckets[client_idx]
                
                client.upload_file(
                    file_path,
                    bucket,
                    object_name,
                    Callback=ProgressCallback(file_size, f"Server {client_idx + 1}")
                )
                return True
            
            # Start uploads to both servers concurrently
            loop = asyncio.get_event_loop()
            tasks = []
            
            # Upload to primary server (main storage)
            task1 = loop.run_in_executor(None, _upload_to_server, server_choice)
            tasks.append(task1)
            
            # Upload to secondary server (backup)
            task2 = loop.run_in_executor(None, _upload_to_server, 1 - server_choice)
            tasks.append(task2)
            
            # Wait for both uploads to complete
            results = await asyncio.gather(*tasks, return_exceptions=True)
            
            # Check if both uploads were successful
            success = all(isinstance(result, bool) and result for result in results)
            
            if success:
                return True, object_name, file_size
            else:
                logger.error(f"Upload failed for {object_name}")
                return False, "", 0
                
        except Exception as e:
            logger.error(f"Upload error: {str(e)}")
            return False, "", 0
    
    async def generate_download_url(self, object_name: str, original_filename: str) -> str:
        """Generate pre-signed download URL from available server"""
        for i, (client, bucket) in enumerate(zip(self.clients, self.buckets)):
            try:
                url = client.generate_presigned_url(
                    'get_object',
                    Params={
                        'Bucket': bucket,
                        'Key': object_name,
                        'ResponseContentDisposition': f'attachment; filename="{original_filename}"'
                    },
                    ExpiresIn=Config.DOWNLOAD_URL_EXPIRY
                )
                return url
            except ClientError as e:
                logger.warning(f"Server {i+1} failed: {e}, trying next server")
                continue
        
        raise Exception("All Wasabi servers are unavailable")
    
    async def delete_file(self, object_name: str):
        """Delete file from both servers"""
        tasks = []
        loop = asyncio.get_event_loop()
        
        for i, (client, bucket) in enumerate(zip(self.clients, self.buckets)):
            task = loop.run_in_executor(
                None, 
                lambda c=client, b=bucket: c.delete_object(Bucket=b, Key=object_name)
            )
            tasks.append(task)
        
        await asyncio.gather(*tasks, return_exceptions=True)

# Progress callback class
class ProgressCallback:
    def __init__(self, total_size: int, server_name: str):
        self.total_size = total_size
        self.server_name = server_name
        self.uploaded = 0
        self.start_time = time.time()
    
    def __call__(self, bytes_amount):
        self.uploaded += bytes_amount
        if self.total_size == 0:
            return
        
        percentage = (self.uploaded / self.total_size) * 100
        elapsed_time = time.time() - self.start_time
        speed = self.uploaded / elapsed_time if elapsed_time > 0 else 0
        
        logger.info(
            f"{self.server_name}: {percentage:.1f}% - "
            f"{self._human_readable_size(self.uploaded)}/"
            f"{self._human_readable_size(self.total_size)} - "
            f"{self._human_readable_size(speed)}/s"
        )
    
    @staticmethod
    def _human_readable_size(size_bytes):
        if size_bytes == 0:
            return "0B"
        units = ['B', 'KB', 'MB', 'GB', 'TB']
        import math
        unit_index = int(math.floor(math.log(size_bytes, 1024)))
        size = round(size_bytes / math.pow(1024, unit_index), 2)
        return f"{size} {units[unit_index]}"

# Telegram Bot
class TelegramWasabiBot:
    def __init__(self):
        self.app = Client(
            "wasabi_bot",
            api_id=Config.API_ID,
            api_hash=Config.API_HASH,
            bot_token=Config.BOT_TOKEN
        )
        self.storage = WasabiStorageManager()
        self.user_sessions = {}
        
        # Register handlers
        self.register_handlers()
    
    def register_handlers(self):
        @self.app.on_message(filters.command("start"))
        async def start_command(client, message: Message):
            welcome_text = """
🤖 **Welcome to High-Speed File Storage Bot!**

**Features:**
✅ **10GB File Support** - Upload large files up to 10GB
✅ **High-Speed Transfer** - Fast upload/download with dual servers
✅ **Any File Format** - Support all file types
✅ **Streaming Links** - Direct play in MX Player, VLC, etc.
✅ **Dual Wasabi Storage** - Redundant storage for reliability

**How to use:**
1. Send me any file (document, video, audio, etc.)
2. I'll upload it to secure Wasabi storage
3. Get a direct download/streaming link

**Supported Players:** MX Player, VLC, PotPlayer, and more!
            """
            await message.reply_text(welcome_text)
        
        @self.app.on_message(filters.document | filters.video | filters.audio)
        async def handle_files(client, message: Message):
            try:
                # Check file size
                file_size = message.document.file_size if message.document else \
                           message.video.file_size if message.video else \
                           message.audio.file_size
                
                if file_size > Config.MAX_FILE_SIZE:
                    await message.reply_text(
                        f"❌ File size exceeds 10GB limit. "
                        f"Your file: {file_size / (1024**3):.1f}GB"
                    )
                    return
                
                # Inform user
                status_msg = await message.reply_text("📥 Downloading file from Telegram...")
                
                # Download file
                file_path = await self.download_telegram_file(message)
                if not file_path:
                    await status_msg.edit_text("❌ Failed to download file from Telegram")
                    return
                
                await status_msg.edit_text("🔄 Uploading to Wasabi storage (dual servers)...")
                
                # Generate unique object name
                file_ext = os.path.splitext(file_path)[1]
                object_name = f"{int(time.time())}_{message.from_user.id}{file_ext}"
                original_filename = message.document.file_name if message.document else \
                                  f"file{file_ext}"
                
                # Upload to Wasabi
                success, stored_name, size = await self.storage.upload_file(
                    file_path, object_name
                )
                
                if success:
                    await status_msg.edit_text("🔗 Generating download link...")
                    
                    # Generate download URL
                    download_url = await self.storage.generate_download_url(
                        stored_name, original_filename
                    )
                    
                    # Create streaming links for players
                    streaming_info = self._generate_streaming_info(download_url, original_filename)
                    
                    # Send success message with links
                    response_text = self._create_success_message(
                        original_filename, size, download_url, streaming_info
                    )
                    
                    keyboard = InlineKeyboardMarkup([
                        [InlineKeyboardButton("📥 Direct Download", url=download_url)],
                        [InlineKeyboardButton("🎬 MX Player", url=streaming_info['mx_player'])],
                        [InlineKeyboardButton("📺 VLC Player", url=streaming_info['vlc_player'])]
                    ])
                    
                    await status_msg.edit_text(response_text, reply_markup=keyboard)
                    
                else:
                    await status_msg.edit_text("❌ Upload failed. Please try again.")
                
                # Cleanup local file
                try:
                    os.remove(file_path)
                except:
                    pass
                    
            except FloodWait as e:
                await asyncio.sleep(e.value)
                await self.handle_files(client, message)
            except Exception as e:
                logger.error(f"Error handling file: {str(e)}")
                await message.reply_text("❌ An error occurred. Please try again.")
    
    async def download_telegram_file(self, message: Message) -> str:
        """Download file from Telegram with progress"""
        try:
            if message.document:
                file_id = message.document.file_id
                file_name = message.document.file_name
            elif message.video:
                file_id = message.video.file_id
                file_name = f"video_{int(time.time())}.mp4"
            elif message.audio:
                file_id = message.audio.file_id
                file_name = f"audio_{int(time.time())}.mp3"
            else:
                return None
            
            # Create downloads directory
            os.makedirs("downloads", exist_ok=True)
            file_path = f"downloads/{file_name}"
            
            # Download with progress
            file = await message.download(
                file_name=file_path,
                progress=self._download_progress,
                progress_args=(message, "📥 Downloading...")
            )
            
            return file_path if file else None
            
        except Exception as e:
            logger.error(f"Download error: {str(e)}")
            return None
    
    async def _download_progress(self, current, total, message, text):
        """Progress callback for download"""
        # You can implement progress updates here
        pass
    
    def _generate_streaming_info(self, download_url: str, filename: str) -> Dict:
        """Generate streaming URLs for various players"""
        return {
            'mx_player': f"intent:{download_url}#Intent;package=com.mxtech.videoplayer.ad;end",
            'vlc_player': f"vlc://{download_url}",
            'direct_stream': download_url
        }
    
    def _create_success_message(self, filename: str, size: int, 
                              download_url: str, streaming_info: Dict) -> str:
        """Create success message with file info"""
        size_mb = size / (1024 * 1024)
        size_gb = size / (1024 * 1024 * 1024)
        
        size_text = f"{size_mb:.1f} MB" if size_mb < 1024 else f"{size_gb:.1f} GB"
        
        return f"""
✅ **File Uploaded Successfully!**

📁 **Filename:** `{filename}`
💾 **Size:** {size_text}
⏰ **Link Expires:** 1 hour

**Download Options:**
• 📥 Direct Download
• 🎬 MX Player (Android)
• 📺 VLC Player
• 🌐 Any media player

**Direct URL:**
`{download_url}`
        """
    
    async def run(self):
        """Start the bot"""
        logger.info("Starting Telegram Wasabi Bot...")
        await self.app.start()
        logger.info("Bot started successfully!")
        
        # Keep the bot running
        await asyncio.Event().wait()

# Main execution
async def main():
    # Validate environment variables
    required_vars = [
        "API_ID", "API_HASH", "BOT_TOKEN",
        "WASABI_ACCESS_KEY_1", "WASABI_SECRET_KEY_1", "WASABI_BUCKET_1",
        "WASABI_ACCESS_KEY_2", "WASABI_SECRET_KEY_2", "WASABI_BUCKET_2"
    ]
    
    missing_vars = [var for var in required_vars if not os.getenv(var)]
    if missing_vars:
        logger.error(f"Missing environment variables: {', '.join(missing_vars)}")
        return
    
    # Initialize and run bot
    bot = TelegramWasabiBot()
    await bot.run()

if __name__ == "__main__":
    asyncio.run(main())
