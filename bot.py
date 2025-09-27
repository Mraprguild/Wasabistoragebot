import os
import asyncio
import boto3
from pyrogram import Client, filters
from pyrogram.types import Message
from pyrogram.enums import ParseMode
from botocore.exceptions import ClientError
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Configuration
API_ID = os.getenv("API_ID")
API_HASH = os.getenv("API_HASH")
BOT_TOKEN = os.getenv("BOT_TOKEN")
WASABI_ACCESS_KEY = os.getenv("WASABI_ACCESS_KEY")
WASABI_SECRET_KEY = os.getenv("WASABI_SECRET_KEY")
WASABI_BUCKET = os.getenv("WASABI_BUCKET")
WASABI_REGION = os.getenv("WASABI_REGION")

# Initialize clients
app = Client("wasabi_bot", api_id=API_ID, api_hash=API_HASH, bot_token=BOT_TOKEN)

wasabi_endpoint = f'https://s3.{WASABI_REGION}.wasabisys.com'
s3_client = boto3.client(
    's3',
    endpoint_url=wasabi_endpoint,
    aws_access_key_id=WASABI_ACCESS_KEY,
    aws_secret_access_key=WASABI_SECRET_KEY
)

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

@app.on_message(filters.command("start"))
async def start(client, message: Message):
    await message.reply_text(
        "🚀 **Wasabi Storage Bot**\n\n"
        "Send me any file to upload to Wasabi storage.\n"
        "Use `/download <filename>` to download files.\n"
        "Use `/list` to see your files.",
        parse_mode=ParseMode.MARKDOWN
    )

@app.on_message(filters.document | filters.video | filters.audio | filters.photo)
async def upload_file(client, message: Message):
    media = message.document or message.video or message.audio or message.photo
    if not media:
        await message.reply_text("Unsupported file type.")
        return

    status_msg = await message.reply_text("📤 Downloading file from Telegram...")
    
    try:
        # Download from Telegram
        file_path = await message.download()
        file_name = f"{get_user_folder(message.from_user.id)}/{os.path.basename(file_path)}"
        
        # Upload to Wasabi
        await status_msg.edit_text("☁️ Uploading to Wasabi...")
        await asyncio.to_thread(
            s3_client.upload_file,
            file_path,
            WASABI_BUCKET,
            file_name
        )
        
        # Generate shareable link
        presigned_url = s3_client.generate_presigned_url(
            'get_object',
            Params={'Bucket': WASABI_BUCKET, 'Key': file_name},
            ExpiresIn=86400  # 24 hours
        )
        
        file_size = media.file_size if hasattr(media, 'file_size') else "Unknown"
        await status_msg.edit_text(
            f"✅ **Upload Complete!**\n\n"
            f"📁 **File:** `{os.path.basename(file_path)}`\n"
            f"📦 **Size:** {humanbytes(file_size)}\n"
            f"🔗 **Link:** `{presigned_url}`",
            parse_mode=ParseMode.MARKDOWN
        )
        
        # Cleanup
        if os.path.exists(file_path):
            os.remove(file_path)
            
    except Exception as e:
        await status_msg.edit_text(f"❌ **Error:** {str(e)}")

@app.on_message(filters.command("download"))
async def download_file(client, message: Message):
    if len(message.command) < 2:
        await message.reply_text("Usage: `/download <filename>`", parse_mode=ParseMode.MARKDOWN)
        return

    file_name = " ".join(message.command[1:])
    user_file_name = f"{get_user_folder(message.from_user.id)}/{file_name}"
    local_path = f"download_{file_name}"
    
    status_msg = await message.reply_text("🔍 Searching for file...")
    
    try:
        # Check if file exists
        await asyncio.to_thread(s3_client.head_object, Bucket=WASABI_BUCKET, Key=user_file_name)
        
        await status_msg.edit_text("📥 Downloading from Wasabi...")
        
        # Download from Wasabi
        await asyncio.to_thread(
            s3_client.download_file,
            WASABI_BUCKET,
            user_file_name,
            local_path
        )
        
        await status_msg.edit_text("📤 Uploading to Telegram...")
        
        # Send to user
        await message.reply_document(
            document=local_path,
            caption=f"✅ **Download Complete!**\n`{file_name}`"
        )
        
        await status_msg.delete()
        
    except ClientError as e:
        error_code = e.response['Error']['Code']
        if error_code == '404':
            await status_msg.edit_text(f"❌ File not found: `{file_name}`", parse_mode=ParseMode.MARKDOWN)
        else:
            await status_msg.edit_text(f"❌ Error: {error_code}")
    except Exception as e:
        await status_msg.edit_text(f"❌ Error: {str(e)}")
    finally:
        # Cleanup
        if os.path.exists(local_path):
            os.remove(local_path)

@app.on_message(filters.command("list"))
async def list_files(client, message: Message):
    status_msg = await message.reply_text("📂 Loading file list...")
    
    try:
        user_prefix = get_user_folder(message.from_user.id) + "/"
        response = await asyncio.to_thread(
            s3_client.list_objects_v2,
            Bucket=WASABI_BUCKET,
            Prefix=user_prefix
        )
        
        if 'Contents' not in response:
            await status_msg.edit_text("📂 No files found in your storage.")
            return
        
        files = [obj['Key'].replace(user_prefix, "") for obj in response['Contents']]
        files_list = "\n".join([f"• `{file}`" for file in files[:15]])  # Show first 15 files
        
        if len(files) > 15:
            files_list += f"\n\n...and {len(files) - 15} more files"
        
        await status_msg.edit_text(
            f"📁 **Your Files:**\n\n{files_list}",
            parse_mode=ParseMode.MARKDOWN
        )
        
    except Exception as e:
        await status_msg.edit_text(f"❌ Error listing files: {str(e)}")

@app.on_message(filters.command("delete"))
async def delete_file(client, message: Message):
    if len(message.command) < 2:
        await message.reply_text("Usage: `/delete <filename>`", parse_mode=ParseMode.MARKDOWN)
        return

    file_name = " ".join(message.command[1:])
    user_file_name = f"{get_user_folder(message.from_user.id)}/{file_name}"
    
    try:
        await asyncio.to_thread(
            s3_client.delete_object,
            Bucket=WASABI_BUCKET,
            Key=user_file_name
        )
        
        await message.reply_text(f"✅ Deleted: `{file_name}`", parse_mode=ParseMode.MARKDOWN)
        
    except Exception as e:
        await message.reply_text(f"❌ Error deleting file: {str(e)}")

if __name__ == "__main__":
    print("Starting Wasabi Storage Bot...")
    app.run()
