# config.py
import os
from dotenv import load_dotenv

# Load environment variables from a .env file if it exists
load_dotenv()

class Config:
    """
    Configuration class to hold all environment variables.
    """
    # Telegram Bot Configuration
    API_ID = int(os.environ.get("API_ID", 0))
    API_HASH = os.environ.get("API_HASH")
    BOT_TOKEN = os.environ.get("BOT_TOKEN")

    # Wasabi Configuration
    WASABI_ACCESS_KEY = os.environ.get("WASABI_ACCESS_KEY")
    WASABI_SECRET_KEY = os.environ.get("WASABI_SECRET_KEY")
    WASABI_BUCKET = os.environ.get("WASABI_BUCKET")
    WASABI_REGION = os.environ.get("WASABI_REGION", "us-east-1") # Default region if not set

# Instantiate the config
config = Config()

# Basic validation to ensure essential variables are set
if not all([config.API_ID, config.API_HASH, config.BOT_TOKEN, config.WASABI_ACCESS_KEY, config.WASABI_SECRET_KEY, config.WASABI_BUCKET]):
    raise ValueError("One or more essential environment variables are missing. Please check your .env file or environment.")

