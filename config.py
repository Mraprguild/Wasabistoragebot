import os
from dotenv import load_dotenv

load_dotenv()

class Config:
    # Telegram Configuration
    API_ID = int(os.getenv("API_ID", 0))
    API_HASH = os.getenv("API_HASH", "")
    BOT_TOKEN = os.getenv("BOT_TOKEN", "")
    
    # Primary Wasabi Configuration
    WASABI_ACCESS_KEY = os.getenv("WASABI_ACCESS_KEY")
    WASABI_SECRET_KEY = os.getenv("WASABI_SECRET_KEY")
    WASABI_BUCKET = os.getenv("WASABI_BUCKET")
    WASABI_REGION = os.getenv("WASABI_REGION", "us-east-1")
    
    # Optional Settings
    MAX_FILE_SIZE = int(os.getenv("MAX_FILE_SIZE", 10 * 1024 * 1024 * 1024))  # 10GB
    MAX_REQUESTS_PER_MINUTE = int(os.getenv("MAX_REQUESTS_PER_MINUTE", 30))
    
    # Authorization (optional - leave empty for public bot)
    AUTHORIZED_USERS = []
    auth_users = os.getenv("AUTHORIZED_USERS", "")
    if auth_users:
        AUTHORIZED_USERS = [int(user_id.strip()) for user_id in auth_users.split(",") if user_id.strip()]
    
    # Backup Accounts Configuration
    @staticmethod
    def get_backup_accounts():
        backups = []
        for i in range(1, 6):  # Support up to 5 backups
            access_key = os.getenv(f"WASABI_BACKUP_{i}_ACCESS_KEY")
            secret_key = os.getenv(f"WASABI_BACKUP_{i}_SECRET_KEY")
            
            if access_key and secret_key:
                backup_config = {
                    'name': f'Backup {i}',
                    'access_key': access_key,
                    'secret_key': secret_key,
                    'bucket': os.getenv(f"WASABI_BACKUP_{i}_BUCKET", Config.WASABI_BUCKET),
                    'region': os.getenv(f"WASABI_BACKUP_{i}_REGION", Config.WASABI_REGION)
                }
                backups.append(backup_config)
        
        return backups
    
    # Validation
    @staticmethod
    def validate():
        required_vars = [
            'API_ID', 'API_HASH', 'BOT_TOKEN',
            'WASABI_ACCESS_KEY', 'WASABI_SECRET_KEY', 'WASABI_BUCKET'
        ]
        
        missing = [var for var in required_vars if not getattr(Config, var, None)]
        if missing:
            raise ValueError(f"Missing required environment variables: {', '.join(missing)}")
        
        if Config.API_ID == 0:
            raise ValueError("API_ID must be set and valid")
        
        # Check if we have at least primary Wasabi credentials
        if not all([Config.WASABI_ACCESS_KEY, Config.WASABI_SECRET_KEY, Config.WASABI_BUCKET]):
            raise ValueError("Primary Wasabi credentials are required")
        
        return True

# Validate configuration on import
Config.validate()
