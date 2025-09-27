import os
from dataclasses import dataclass

@dataclass
class Config:
    # Telegram API credentials
    API_ID: int = int(os.getenv("API_ID", 0))
    API_HASH: str = os.getenv("API_HASH", "")
    BOT_TOKEN: str = os.getenv("BOT_TOKEN", "")
    
    # Wasabi S3 credentials
    WASABI_ACCESS_KEY: str = os.getenv("WASABI_ACCESS_KEY", "")
    WASABI_SECRET_KEY: str = os.getenv("WASABI_SECRET_KEY", "")
    WASABI_BUCKET: str = os.getenv("WASABI_BUCKET", "")
    WASABI_REGION: str = os.getenv("WASABI_REGION", "us-east-1")
    
    # Bot settings
    MAX_FILE_SIZE: int = int(os.getenv("MAX_FILE_SIZE", 2147483648))  # 2GB default
    ALLOWED_EXTENSIONS: list = None
    
    def __post_init__(self):
        if self.ALLOWED_EXTENSIONS is None:
            self.ALLOWED_EXTENSIONS = ['.mp4', '.avi', '.mov', '.mkv', '.pdf', 
                                     '.doc', '.docx', '.zip', '.rar', '.7z']

# Create config instance
config = Config()
