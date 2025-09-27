import os
import asyncio
import boto3
from pyrogram import Client, filters
from pyrogram.types import Message
from pyrogram.enums import ParseMode
from botocore.exceptions import ClientError

from config import Config

# Initialize the bot with configuration
app = Client(
    "wasabi_bot",
    api_id=Config.API_ID,
    api_hash=Config.API_HASH,
    bot_token=Config.BOT_TOKEN
)

def create_s3_client(access_key, secret_key, region):
    """Create S3 client for Wasabi"""
    endpoint = f'https://s3.{region}.wasabisys.com'
    return boto3.client(
        's3',
        endpoint_url=endpoint,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key
    )

# Initialize S3 clients
primary_s3 = create_s3_client(
    Config.WASABI_ACCESS_KEY, 
    Config.WASABI_SECRET_KEY, 
    Config.WASABI_REGION
)

backup_clients = []
for account in Config.get_backup_accounts():
    try:
        client = create_s3_client(
            account['access_key'],
            account['secret_key'],
            account['region']
        )
        backup_clients.append({
            'name': account['name'],
            'client': client,
            'bucket': account['bucket']
        })
        print(f"✅ {account['name']} configured successfully")
    except Exception as e:
        print(f"❌ Failed to configure {account['name']}: {e}")

# Utility functions
def get_user_folder(user_id):
    return f"user_{user_id}"

def humanbytes(size):
    """Convert bytes to human readable format"""
    if not size:
        return "0 B"
    for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
        if size < 1024.0:
            return f"{size:.2f} {unit}"
        size /= 1024.0
    return f"{size:.2f} PB"

async def is_authorized(user_id):
    """Check if user is authorized to use the bot"""
    if not Config.AUTHORIZED_USERS:  # Empty list means public bot
        return True
    return user_id in Config.AUTHORIZED_USERS

# Rate limiting storage
user_requests = {}

async def check_rate_limit(user_id):
    """Simple rate limiting"""
    import time
    current_time = time.time()
    
    if user_id not in user_requests:
        user_requests[user_id] = []
    
    # Clean old requests (older than 1 minute)
    user_requests[user_id] = [t for t in user_requests[user_id] if current_time - t < 60]
    
    if len(user_requests[user_id]) >= Config.MAX_REQUESTS_PER_MINUTE:
        return False
    
    user_requests[user_id].append(current_time)
    return True

# Bot handlers (same as previous code, but using Config)
@app.on_message(filters.command("start"))
async def start(client, message: Message):
    if not await is_authorized(message.from_user.id):
        await message.reply_text("❌ Unauthorized access.")
        return
        
    backup_info = f"\n🔒 **Backup Accounts:** {len(backup_clients)} configured" if backup_clients else ""
    
    await message.reply_text(
        f"🚀 **Wasabi Storage Bot with Backup**{backup_info}\n\n"
        "**Upload:** Send any file (auto-backup to multiple accounts)\n"
        "**Download:** `/download filename`\n"
        "**List:** `/list [account]`\n"
        "**Delete:** `/delete filename [account]`\n"
        "**Sync:** `/sync filename`\n"
        "**Backups:** `/backups`\n"
        "**Help:** `/help`",
        parse_mode=ParseMode.MARKDOWN
    )

# ... (include all other handlers from previous code)

@app.on_message(filters.command("config"))
async def show_config(client, message: Message):
    """Show current configuration (admin only)"""
    if not await is_authorized(message.from_user.id):
        await message.reply_text("❌ Unauthorized access.")
        return
    
    config_info = f"""
🔧 **Bot Configuration**

**Telegram:**
- API ID: `{Config.API_ID}`
- Bot: ✅ Connected

**Primary Wasabi:**
- Bucket: `{Config.WASABI_BUCKET}`
- Region: `{Config.WASABI_REGION}`
- Status: ✅ Connected

**Backup Accounts:** {len(backup_clients)} configured
**Max File Size:** {humanbytes(Config.MAX_FILE_SIZE)}
**Rate Limit:** {Config.MAX_REQUESTS_PER_MINUTE}/minute
**Authorization:** {'Restricted' if Config.AUTHORIZED_USERS else 'Public'}
"""
    await message.reply_text(config_info, parse_mode=ParseMode.MARKDOWN)

if __name__ == "__main__":
    print("=" * 50)
    print("🚀 Wasabi Storage Bot Starting...")
    print("=" * 50)
    print(f"Primary Bucket: {Config.WASABI_BUCKET}")
    print(f"Backup Accounts: {len(backup_clients)}")
    print(f"Max File Size: {humanbytes(Config.MAX_FILE_SIZE)}")
    print(f"Authorized Users: {len(Config.AUTHORIZED_USERS) if Config.AUTHORIZED_USERS else 'All'}")
    print("=" * 50)
    
    app.run()
