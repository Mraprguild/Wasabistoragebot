import os
from dotenv import load_dotenv

load_dotenv()

class Config:
    # Telegram API Configuration - Get these from https://my.telegram.org
    API_ID = 1234567  # Your API ID (integer)
    API_HASH = "your_telegram_api_hash_here"  # Your API Hash
    BOT_TOKEN = "1234567890:your_bot_token_here"  # Your Bot Token from @BotFather
    
    # Wasabi S3 Configuration
    WASABI_ACCESS_KEY = "your_wasabi_access_key_here"  # Wasabi Access Key
    WASABI_SECRET_KEY = "your_wasabi_secret_key_here"  # Wasabi Secret Key
    WASABI_BUCKET = "your_bucket_name_here"  # Your Wasabi bucket name
    WASABI_REGION = "us-east-1"  # Wasabi region (us-east-1, us-central-1, etc.)
    
    # Optional: Download directory
    DOWNLOAD_DIR = "./downloads"
    
    # Optional: Maximum file size (in bytes) - 10GB default
    MAX_FILE_SIZE = 10 * 1024 * 1024 * 1024
    
    # Optional: Presigned URL expiration (in seconds) - 7 days default
    URL_EXPIRATION = 604800

def load_config():
    """Load configuration from environment variables if available"""
    config = Config()
    
    # Telegram Config
    if os.getenv('API_ID'):
        config.API_ID = int(os.getenv('API_ID'))
    if os.getenv('API_HASH'):
        config.API_HASH = os.getenv('API_HASH')
    if os.getenv('BOT_TOKEN'):
        config.BOT_TOKEN = os.getenv('BOT_TOKEN')
    
    # Wasabi Config
    if os.getenv('WASABI_ACCESS_KEY'):
        config.WASABI_ACCESS_KEY = os.getenv('WASABI_ACCESS_KEY')
    if os.getenv('WASABI_SECRET_KEY'):
        config.WASABI_SECRET_KEY = os.getenv('WASABI_SECRET_KEY')
    if os.getenv('WASABI_BUCKET'):
        config.WASABI_BUCKET = os.getenv('WASABI_BUCKET')
    if os.getenv('WASABI_REGION'):
        config.WASABI_REGION = os.getenv('WASABI_REGION')
    
    # Optional settings
    if os.getenv('MAX_FILE_SIZE'):
        config.MAX_FILE_SIZE = int(os.getenv('MAX_FILE_SIZE'))
    if os.getenv('URL_EXPIRATION'):
        config.URL_EXPIRATION = int(os.getenv('URL_EXPIRATION'))
    
    return config

# Create config instance
try:
    config = load_config()
except:
    config = Config()
