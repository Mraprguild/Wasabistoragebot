# bot.py
import os
import time
import asyncio
import logging
from pyrogram import Client, filters
from pyrogram.types import Message, ForceReply
from pyrogram.errors import MessageNotModified

from config import config
from wasabi_handler import wasabi
from progress import progress_for_pyrogram, Boto3Progress

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Initialize the Pyrogram client
# The 'tgcrypto' package will be used automatically if installed for better performance
app = Client(
    "WasabiUploadBot",
    api_id=config.API_ID,
    api_hash=config.API_HASH,
    bot_token=config.BOT_TOKEN
)

# A dictionary to store user states and data during the conversation
user_states = {}
# Structure: {user_id: {"state": "AWAITING_FILENAME", "file_message": message}}
#            {user_id: {"state": "AWAITING_THUMBNAIL", "file_message": message, "new_name": "..."}}

@app.on_message(filters.command("start"))
async def start_handler(client, message: Message):
    """Handler for the /start command."""
    await message.reply_text(
        "👋 Hello! I am your Wasabi Upload Assistant.\n\n"
        "Send me any file, and I will help you rename it and upload it to Wasabi storage. "
        "I can also add a custom thumbnail to video files."
    )

@app.on_message(filters.media & ~filters.photo & filters.private)
async def file_handler(client, message: Message):
    """Handles any file/media sent by the user (except photos)."""
    user_id = message.from_user.id
    
    # Store the file message and set the state to await a new filename
    user_states[user_id] = {
        "state": "AWAITING_FILENAME",
        "file_message": message
    }
    
    await message.reply_text(
        "File received. Please send me the new file name (without the extension).",
        reply_markup=ForceReply(selective=True)
    )

@app.on_message(filters.photo & filters.private)
async def photo_handler(client, message: Message):
    """Handles photos, which are treated as potential thumbnails."""
    user_id = message.from_user.id
    
    if user_id in user_states and user_states[user_id].get("state") == "AWAITING_THUMBNAIL":
        # This photo is the custom thumbnail
        user_states[user_id]["state"] = "PROCESSING"
        user_states[user_id]["thumbnail_message"] = message
        await process_file(client, message.chat.id, user_id)
    else:
        await message.reply_text("Please send a file (like a document or video) first if you want to upload something.")

@app.on_message(filters.text & filters.private)
async def text_handler(client, message: Message):
    """Handles text messages, which could be new filenames or other commands."""
    user_id = message.from_user.id

    if user_id not in user_states:
        return # Ignore text if there's no active state

    state_data = user_states[user_id]
    current_state = state_data.get("state")

    if current_state == "AWAITING_FILENAME":
        # User has provided the new filename
        state_data["new_name"] = message.text.strip()
        original_message = state_data["file_message"]
        
        if original_message.video:
            # If it's a video, ask for a thumbnail
            state_data["state"] = "AWAITING_THUMBNAIL"
            await message.reply_text(
                "Great! Now, please send a custom thumbnail for the video.\n"
                "If you want to skip, send /skip.",
                reply_markup=ForceReply(selective=True)
            )
        else:
            # If it's a document, proceed directly to processing
            state_data["state"] = "PROCESSING"
            state_data["thumbnail_message"] = None # No thumbnail for documents
            await process_file(client, message.chat.id, user_id)
            
    elif current_state == "AWAITING_THUMBNAIL" and message.text.lower() == "/skip":
        # User chose to skip the thumbnail
        state_data["state"] = "PROCESSING"
        state_data["thumbnail_message"] = None
        await process_file(client, message.chat.id, user_id)

async def process_file(client, chat_id, user_id):
    """
    The core function to download, rename, and upload the file.
    """
    state_data = user_states.get(user_id)
    if not state_data or state_data.get("state") != "PROCESSING":
        return

    status_message = await client.send_message(chat_id, "Processing your request...")

    try:
        # 1. Download from Telegram
        await status_message.edit("Downloading from Telegram...")
        original_media_message = state_data["file_message"]
        
        start_time = time.time()
        download_path = await client.download_media(
            message=original_media_message,
            progress=progress_for_pyrogram,
            progress_args=("Downloading...", status_message, start_time)
        )
        await status_message.edit("Download complete!")

        # 2. Download thumbnail if provided
        thumb_path = None
        if state_data.get("thumbnail_message"):
            thumb_path = await client.download_media(message=state_data["thumbnail_message"])

        # 3. Rename the file
        file_name = os.path.basename(download_path)
        file_ext = os.path.splitext(file_name)[1]
        new_file_name = f"{state_data['new_name']}{file_ext}"
        new_path = os.path.join(os.path.dirname(download_path), new_file_name)
        os.rename(download_path, new_path)
        logger.info(f"File renamed to: {new_path}")

        # 4. Upload to Wasabi
        await status_message.edit("Uploading to Wasabi...")
        file_size = os.path.getsize(new_path)
        loop = asyncio.get_event_loop()
        boto3_progress = Boto3Progress(status_message, file_size, loop)

        upload_success = await wasabi.upload_file(
            file_path=new_path,
            object_name=new_file_name,
            callback=boto3_progress
        )

        # 5. Finalize and clean up
        if upload_success:
            s3_url = f"https://s3.{config.WASABI_REGION}.wasabisys.com/{config.WASABI_BUCKET}/{new_file_name}"
            final_text = (
                f"✅ **Upload Successful!**\n\n"
                f"**File:** `{new_file_name}`\n"
                f"**URL:** `{s3_url}`"
            )
            await status_message.edit(final_text, disable_web_page_preview=True)
        else:
            await status_message.edit("❌ **Upload Failed!**\n\nSomething went wrong while uploading to Wasabi. Please check the logs.")
    
    except Exception as e:
        logger.error(f"Error during processing for user {user_id}: {e}", exc_info=True)
        try:
            await status_message.edit(f"An error occurred: {e}")
        except MessageNotModified:
            pass # Ignore if the message is already showing an error
            
    finally:
        # Clean up local files and state
        if 'download_path' in locals() and os.path.exists(download_path):
            os.remove(download_path)
        if 'new_path' in locals() and os.path.exists(new_path):
            os.remove(new_path)
        if 'thumb_path' in locals() and os.path.exists(thumb_path):
            os.remove(thumb_path)
        if user_id in user_states:
            del user_states[user_id]
        logger.info(f"Cleaned up state and files for user {user_id}")


async def main():
    """Main function to start the bot."""
    logger.info("Bot is starting...")
    await app.start()
    logger.info("Bot has started successfully!")
    await asyncio.Event().wait() # Keep the bot running indefinitely

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Bot shutting down...")

