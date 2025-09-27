import os
import asyncio
import boto3
from pyrogram import Client, filters
from pyrogram.types import Message
from pyrogram.enums import ParseMode
from botocore.exceptions import ClientError
from dotenv import load_dotenv
import json
from datetime import datetime

# Load environment variables
load_dotenv()

# Configuration for multiple Wasabi accounts
API_ID = os.getenv("API_ID")
API_HASH = os.getenv("API_HASH")
BOT_TOKEN = os.getenv("BOT_TOKEN")

# Primary Wasabi account
WASABI_ACCESS_KEY = os.getenv("WASABI_ACCESS_KEY")
WASABI_SECRET_KEY = os.getenv("WASABI_SECRET_KEY")
WASABI_BUCKET = os.getenv("WASABI_BUCKET")
WASABI_REGION = os.getenv("WASABI_REGION")

# Backup Wasabi accounts (WASABI_BACKUP_1_ACCESS_KEY, WASABI_BACKUP_1_SECRET_KEY, etc.)
BACKUP_ACCOUNTS = []

# Load backup accounts from environment variables
for i in range(1, 6):  # Support up to 5 backup accounts
    access_key = os.getenv(f"WASABI_BACKUP_{i}_ACCESS_KEY")
    secret_key = os.getenv(f"WASABI_BACKUP_{i}_SECRET_KEY")
    bucket = os.getenv(f"WASABI_BACKUP_{i}_BUCKET", WASABI_BUCKET)
    region = os.getenv(f"WASABI_BACKUP_{i}_REGION", WASABI_REGION)
    
    if access_key and secret_key:
        BACKUP_ACCOUNTS.append({
            'name': f'Backup {i}',
            'access_key': access_key,
            'secret_key': secret_key,
            'bucket': bucket,
            'region': region
        })

# Initialize clients
app = Client("wasabi_bot", api_id=API_ID, api_hash=API_HASH, bot_token=BOT_TOKEN)

def create_s3_client(access_key, secret_key, region):
    """Create S3 client for Wasabi"""
    endpoint = f'https://s3.{region}.wasabisys.com'
    return boto3.client(
        's3',
        endpoint_url=endpoint,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key
    )

# Primary S3 client
primary_s3 = create_s3_client(WASABI_ACCESS_KEY, WASABI_SECRET_KEY, WASABI_REGION)

# Backup S3 clients
backup_clients = []
for account in BACKUP_ACCOUNTS:
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

def get_user_folder(user_id):
    return f"user_{user_id}"

def humanbytes(size):
    """Convert bytes to human readable format"""
    if not size or size == 0:
        return "0 B"
    try:
        size = float(size)
    except (TypeError, ValueError):
        return "Unknown"
        
    for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
        if size < 1024.0:
            return f"{size:.2f} {unit}"
        size /= 1024.0
    return f"{size:.2f} PB"

async def safe_edit_message(message, text, parse_mode=ParseMode.MARKDOWN):
    """Safely edit a message without causing MESSAGE_NOT_MODIFIED errors"""
    try:
        if hasattr(message, 'text') and message.text == text:
            return message
        await message.edit_text(text, parse_mode=parse_mode)
        return message
    except Exception:
        try:
            return await message.reply_text(text, parse_mode=parse_mode)
        except:
            return message

async def upload_to_backups(file_path, file_name, file_size, status_msg):
    """Upload file to all backup accounts"""
    backup_results = []
    
    for backup in backup_clients:
        try:
            await safe_edit_message(
                status_msg, 
                f"🔄 Uploading to {backup['name']}... ({humanbytes(file_size)})"
            )
            
            # Upload to backup account
            await asyncio.to_thread(
                backup['client'].upload_file,
                file_path,
                backup['bucket'],
                file_name
            )
            
            backup_results.append({
                'name': backup['name'],
                'status': '✅ Success',
                'bucket': backup['bucket']
            })
            
        except Exception as e:
            backup_results.append({
                'name': backup['name'],
                'status': f'❌ Failed: {str(e)}',
                'bucket': backup['bucket']
            })
    
    return backup_results

async def download_from_backup(file_name, user_file_name, status_msg):
    """Try to download file from backup accounts if primary fails"""
    for backup in backup_clients:
        try:
            await safe_edit_message(
                status_msg,
                f"🔍 Trying {backup['name']}..."
            )
            
            # Check if file exists in backup
            await asyncio.to_thread(
                backup['client'].head_object,
                Bucket=backup['bucket'],
                Key=user_file_name
            )
            
            # Download from backup
            local_path = f"download_{file_name}_from_backup"
            await asyncio.to_thread(
                backup['client'].download_file,
                backup['bucket'],
                user_file_name,
                local_path
            )
            
            return local_path, backup['name']
            
        except Exception:
            continue
    
    return None, None

async def sync_file_to_backups(source_client, source_bucket, file_name, status_msg):
    """Sync a file from primary to all backup accounts"""
    sync_results = []
    
    # First download the file from primary
    temp_path = f"temp_sync_{file_name}"
    try:
        await asyncio.to_thread(
            source_client.download_file,
            source_bucket,
            file_name,
            temp_path
        )
        
        # Now upload to all backups
        for backup in backup_clients:
            try:
                await asyncio.to_thread(
                    backup['client'].upload_file,
                    temp_path,
                    backup['bucket'],
                    file_name
                )
                sync_results.append(f"✅ {backup['name']}: Synced")
            except Exception as e:
                sync_results.append(f"❌ {backup['name']}: {str(e)}")
                
    except Exception as e:
        sync_results.append(f"❌ Failed to download from source: {str(e)}")
    finally:
        if os.path.exists(temp_path):
            os.remove(temp_path)
    
    return sync_results

@app.on_message(filters.command("start"))
async def start(client, message: Message):
    backup_info = ""
    if backup_clients:
        backup_info = f"\n🔒 **Backup Accounts:** {len(backup_clients)} configured"
    
    await message.reply_text(
        f"🚀 **Wasabi Storage Bot with Backup**{backup_info}\n\n"
        "**Upload:** Send any file (auto-backup to multiple accounts)\n"
        "**Download:** `/download filename` (tries backups if primary fails)\n"
        "**List:** `/list [account]` (primary, backup1, backup2, etc.)\n"
        "**Delete:** `/delete filename [account]`\n"
        "**Sync:** `/sync filename` (sync from primary to backups)\n"
        "**Backups:** `/backups` (show backup status)\n"
        "**Help:** `/help`",
        parse_mode=ParseMode.MARKDOWN
    )

@app.on_message(filters.command("backups"))
async def show_backups(client, message: Message):
    """Show status of all backup accounts"""
    if not backup_clients:
        await message.reply_text("❌ No backup accounts configured.")
        return
    
    status_text = "🔒 **Backup Accounts Status:**\n\n"
    
    for backup in backup_clients:
        try:
            # Test connection by listing buckets or simple operation
            await asyncio.to_thread(backup['client'].list_buckets)
            status_text += f"✅ **{backup['name']}** - 🟢 Online\n"
            status_text += f"   Bucket: `{backup['bucket']}`\n\n"
        except Exception as e:
            status_text += f"❌ **{backup['name']}** - 🔴 Offline\n"
            status_text += f"   Error: {str(e)}\n\n"
    
    await message.reply_text(status_text, parse_mode=ParseMode.MARKDOWN)

@app.on_message(filters.document | filters.video | filters.audio | filters.photo)
async def upload_file(client, message: Message):
    # Get the media object safely
    media = None
    file_size = 0
    
    if message.document:
        media = message.document
        file_size = media.file_size
    elif message.video:
        media = message.video  
        file_size = media.file_size
    elif message.audio:
        media = message.audio
        file_size = media.file_size
    elif message.photo:
        # Photos are different - they're in an array
        media = message.photo[-1]  # Get the largest version
        file_size = media.file_size if hasattr(media, 'file_size') else 0
    
    if not media:
        await message.reply_text("Unsupported file type.")
        return

    status_msg = await message.reply_text("📤 Downloading file from Telegram...")
    file_path = None
    
    try:
        # Download from Telegram
        file_path = await message.download()
        if not file_path:
            await safe_edit_message(status_msg, "❌ Failed to download file from Telegram.")
            return
            
        file_name = f"{get_user_folder(message.from_user.id)}/{os.path.basename(file_path)}"
        original_name = getattr(media, 'file_name', None) or os.path.basename(file_path)
        
        # Upload to primary account
        await safe_edit_message(status_msg, "☁️ Uploading to primary storage...")
        await asyncio.to_thread(
            primary_s3.upload_file,
            file_path,
            WASABI_BUCKET,
            file_name
        )
        
        # Upload to backup accounts
        backup_results = []
        if backup_clients:
            backup_results = await upload_to_backups(file_path, file_name, file_size, status_msg)
        
        # Generate shareable link from primary
        presigned_url = primary_s3.generate_presigned_url(
            'get_object',
            Params={'Bucket': WASABI_BUCKET, 'Key': file_name},
            ExpiresIn=86400
        )
        
        # Create result message
        result_text = (
            f"✅ **Upload Complete!**\n\n"
            f"📁 **File:** `{original_name}`\n"
            f"📦 **Size:** {humanbytes(file_size)}\n"
            f"🔗 **Primary Link:** `{presigned_url}`\n"
            f"🏠 **Primary:** ✅ Success\n"
        )
        
        # Add backup results
        if backup_results:
            result_text += "\n**Backup Results:**\n"
            for result in backup_results:
                result_text += f"• {result['name']}: {result['status']}\n"
        
        await safe_edit_message(status_msg, result_text)
        
    except Exception as e:
        await safe_edit_message(status_msg, f"❌ **Error:** {str(e)}")
    finally:
        if file_path and os.path.exists(file_path):
            os.remove(file_path)

@app.on_message(filters.command("download"))
async def download_file(client, message: Message):
    if len(message.command) < 2:
        await message.reply_text("Usage: `/download <filename> [account]`", parse_mode=ParseMode.MARKDOWN)
        return

    file_name = " ".join(message.command[1:])
    if not file_name or file_name.strip() == "":
        await message.reply_text("❌ Please provide a valid filename.")
        return
        
    user_file_name = f"{get_user_folder(message.from_user.id)}/{file_name}"
    local_path = f"download_{file_name}"
    
    status_msg = await message.reply_text("🔍 Searching for file...")
    downloaded_from = "Primary"
    
    try:
        # Try primary account first
        try:
            meta = await asyncio.to_thread(primary_s3.head_object, Bucket=WASABI_BUCKET, Key=user_file_name)
            file_size = meta['ContentLength']
            
            await safe_edit_message(status_msg, f"📥 Downloading from primary ({humanbytes(file_size)})...")
            
            await asyncio.to_thread(
                primary_s3.download_file,
                WASABI_BUCKET,
                user_file_name,
                local_path
            )
            
        except ClientError:
            # Primary failed, try backups
            backup_path, backup_name = await download_from_backup(file_name, user_file_name, status_msg)
            if backup_path:
                local_path = backup_path
                downloaded_from = backup_name
                # Get file size for backup file
                file_size = os.path.getsize(local_path)
            else:
                raise Exception("File not found in primary or any backup account")
        
        await safe_edit_message(status_msg, "📤 Uploading to Telegram...")
        
        await message.reply_document(
            document=local_path,
            caption=f"✅ **Download Complete!**\n`{file_name}`\nSource: {downloaded_from}"
        )
        
        await status_msg.delete()
        
    except Exception as e:
        await safe_edit_message(status_msg, f"❌ **Error:** {str(e)}")
    finally:
        if os.path.exists(local_path):
            os.remove(local_path)

@app.on_message(filters.command("list"))
async def list_files(client, message: Message):
    account_name = "primary"
    if len(message.command) > 1:
        account_name = message.command[1].lower()
    
    status_msg = await message.reply_text(f"📂 Loading files from {account_name}...")
    
    try:
        # Determine which account to list from
        if account_name == "primary":
            s3_client = primary_s3
            bucket = WASABI_BUCKET
            account_display = "Primary Storage"
        else:
            # Find backup account
            backup_found = False
            for backup in backup_clients:
                if account_name in backup['name'].lower():
                    s3_client = backup['client']
                    bucket = backup['bucket']
                    account_display = backup['name']
                    backup_found = True
                    break
            
            if not backup_found:
                await safe_edit_message(status_msg, f"❌ Account not found: {account_name}")
                return
        
        user_prefix = get_user_folder(message.from_user.id) + "/"
        response = await asyncio.to_thread(
            s3_client.list_objects_v2,
            Bucket=bucket,
            Prefix=user_prefix
        )
        
        if 'Contents' not in response:
            await safe_edit_message(status_msg, f"📂 No files found in {account_display}.")
            return
        
        files = [obj['Key'].replace(user_prefix, "") for obj in response['Contents']]
        total_size = sum(obj['Size'] for obj in response['Contents'])
        
        files_list = "\n".join([f"• `{file}` ({humanbytes(response['Contents'][i]['Size'])})" 
                              for i, file in enumerate(files[:10])])
        
        if len(files) > 10:
            files_list += f"\n\n...and {len(files) - 10} more files"
        
        await status_msg.delete()
        await message.reply_text(
            f"📁 **{account_display}** ({len(files)} files, {humanbytes(total_size)} total):\n\n{files_list}",
            parse_mode=ParseMode.MARKDOWN
        )
        
    except Exception as e:
        await safe_edit_message(status_msg, f"❌ Error listing files: {str(e)}")

@app.on_message(filters.command("sync"))
async def sync_command(client, message: Message):
    """Sync a file from primary to all backup accounts"""
    if len(message.command) < 2:
        await message.reply_text("Usage: `/sync <filename>`", parse_mode=ParseMode.MARKDOWN)
        return

    if not backup_clients:
        await message.reply_text("❌ No backup accounts configured.")
        return

    file_name = " ".join(message.command[1:])
    if not file_name or file_name.strip() == "":
        await message.reply_text("❌ Please provide a valid filename.")
        return
        
    user_file_name = f"{get_user_folder(message.from_user.id)}/{file_name}"
    
    status_msg = await message.reply_text(f"🔄 Syncing {file_name} to backups...")
    
    try:
        # Check if file exists in primary
        await asyncio.to_thread(primary_s3.head_object, Bucket=WASABI_BUCKET, Key=user_file_name)
        
        # Sync to backups
        sync_results = await sync_file_to_backups(primary_s3, WASABI_BUCKET, user_file_name, status_msg)
        
        result_text = f"🔄 **Sync Results for** `{file_name}`:\n\n"
        for result in sync_results:
            result_text += f"{result}\n"
        
        await safe_edit_message(status_msg, result_text)
        
    except ClientError as e:
        if e.response['Error']['Code'] == '404':
            await safe_edit_message(status_msg, f"❌ File not found in primary: `{file_name}`")
        else:
            await safe_edit_message(status_msg, f"❌ Error: {e.response['Error']['Code']}")
    except Exception as e:
        await safe_edit_message(status_msg, f"❌ Sync failed: {str(e)}")

@app.on_message(filters.command("delete"))
async def delete_file(client, message: Message):
    if len(message.command) < 2:
        await message.reply_text("Usage: `/delete <filename> [account]`\nUse 'all' to delete from all accounts.", 
                               parse_mode=ParseMode.MARKDOWN)
        return

    file_name = " ".join(message.command[1:-1] if len(message.command) > 2 else message.command[1:])
    if not file_name or file_name.strip() == "":
        await message.reply_text("❌ Please provide a valid filename.")
        return
        
    account_name = message.command[-1] if len(message.command) > 2 else "primary"
    
    user_file_name = f"{get_user_folder(message.from_user.id)}/{file_name}"
    
    if account_name.lower() == "all":
        # Delete from all accounts
        results = []
        accounts = [('Primary', primary_s3, WASABI_BUCKET)] + \
                  [(backup['name'], backup['client'], backup['bucket']) for backup in backup_clients]
        
        for acc_name, client, bucket in accounts:
            try:
                await asyncio.to_thread(client.delete_object, Bucket=bucket, Key=user_file_name)
                results.append(f"✅ {acc_name}: Deleted")
            except Exception as e:
                results.append(f"❌ {acc_name}: {str(e)}")
        
        result_text = f"🗑️ **Delete Results for** `{file_name}`:\n\n" + "\n".join(results)
        await message.reply_text(result_text, parse_mode=ParseMode.MARKDOWN)
        
    else:
        # Delete from specific account
        try:
            if account_name == "primary":
                client = primary_s3
                bucket = WASABI_BUCKET
            else:
                client = None
                for backup in backup_clients:
                    if account_name in backup['name'].lower():
                        client = backup['client']
                        bucket = backup['bucket']
                        break
                if not client:
                    await message.reply_text(f"❌ Account not found: {account_name}")
                    return
            
            await asyncio.to_thread(client.delete_object, Bucket=bucket, Key=user_file_name)
            await message.reply_text(f"✅ Deleted `{file_name}` from {account_name}", 
                                   parse_mode=ParseMode.MARKDOWN)
            
        except Exception as e:
            await message.reply_text(f"❌ Error deleting file: {str(e)}")

@app.on_message(filters.command("help"))
async def help_command(client, message: Message):
    help_text = """
🤖 **Wasabi Storage Bot with Multi-Backup**

**Core Commands:**
📤 **Upload:** Send any file (auto-backup to all configured accounts)
📥 **Download:** `/download filename` (tries backups if primary fails)
📂 **List Files:** `/list [account]` (primary, backup1, backup2, etc.)
🗑️ **Delete:** `/delete filename [account]` (or 'all' for all accounts)
🔄 **Sync:** `/sync filename` (sync from primary to all backups)
🔒 **Backups:** `/backups` (show backup account status)

**Account Names for List/Delete:**
- `primary` - Main storage account
- `backup1`, `backup2`, etc. - Backup accounts
- `all` - For deleting from all accounts
**Features:**
- Automatic multi-account backup on upload
- Redundant storage across multiple Wasabi accounts
- Failover download (if primary fails, tries backups)
- Individual account management
- Real-time backup status monitoring
    """
    
    await message.reply_text(help_text, parse_mode=ParseMode.MARKDOWN)

if __name__ == "__main__":
    try:
        # Validate environment variables
        required_vars = ["API_ID", "API_HASH", "BOT_TOKEN", "WASABI_ACCESS_KEY", "WASABI_SECRET_KEY", "WASABI_BUCKET", "WASABI_REGION"]
        missing_vars = [var for var in required_vars if not os.getenv(var)]
        
        if missing_vars:
            print(f"❌ Missing required environment variables: {', '.join(missing_vars)}")
            exit(1)
            
        print("✅ Starting Wasabi Storage Bot with Backup Support...")
        print(f"📦 Primary account: {WASABI_BUCKET} ({WASABI_REGION})")
        print(f"🔒 Backup accounts: {len(backup_clients)} configured")
       
        app.run()
    except Exception as e:
        print(f"❌ Failed to start bot: {e}")
