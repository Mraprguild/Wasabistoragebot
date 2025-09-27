import os
from dotenv import load_dotenv

load_dotenv()

class Config:
    # Telegram API Configuration
    API_ID = int(os.getenv("API_ID"))
    API_HASH = os.getenv("API_HASH")
    BOT_TOKEN = os.getenv("BOT_TOKEN")
    
    # Server 1 Configuration
    WASABI_ACCESS_KEY_1 = os.getenv("WASABI_ACCESS_KEY_1")
    WASABI_SECRET_KEY_1 = os.getenv("WASABI_SECRET_KEY_1")
    WASABI_BUCKET_1 = os.getenv("WASABI_BUCKET_1")
    WASABI_REGION_1 = os.getenv("WASABI_REGION_1")
    
    # Server 2 Configuration
    WASABI_ACCESS_KEY_2 = os.getenv("WASABI_ACCESS_KEY_2")
    WASABI_SECRET_KEY_2 = os.getenv("WASABI_SECRET_KEY_2")
    WASABI_BUCKET_2 = os.getenv("WASABI_BUCKET_2")
    WASABI_REGION_2 = os.getenv("WASABI_REGION_2")
    
    # Validation
    @classmethod
    def validate(cls):
        required_vars = [
            cls.API_ID, cls.API_HASH, cls.BOT_TOKEN,
            cls.WASABI_ACCESS_KEY_1, cls.WASABI_SECRET_KEY_1, 
            cls.WASABI_BUCKET_1, cls.WASABI_REGION_1
        ]
        if any(v is None for v in required_vars):
            raise ValueError("One or more essential environment variables are missing.")

# Create config instance
config = Config()
