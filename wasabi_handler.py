# wasabi_handler.py
import asyncio
import boto3
from botocore.exceptions import NoCredentialsError, PartialCredentialsError
from config import config
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class WasabiHandler:
    """
    Handles connections and file uploads to Wasabi S3 storage.
    """
    def __init__(self):
        try:
            self.session = boto3.session.Session()
            self.s3_client = self.session.client(
                's3',
                region_name=config.WASABI_REGION,
                endpoint_url=f'https://s3.{config.WASABI_REGION}.wasabisys.com',
                aws_access_key_id=config.WASABI_ACCESS_KEY,
                aws_secret_access_key=config.WASABI_SECRET_KEY
            )
            logger.info("Wasabi S3 client initialized successfully.")
        except (NoCredentialsError, PartialCredentialsError) as e:
            logger.error(f"Credentials error initializing Wasabi client: {e}")
            raise
        except Exception as e:
            logger.error(f"An unexpected error occurred during Wasabi client initialization: {e}")
            raise

    async def upload_file(self, file_path: str, object_name: str, callback=None):
        """
        Asynchronously uploads a file to the Wasabi bucket.
        
        Args:
            file_path (str): The local path to the file to upload.
            object_name (str): The desired name of the object in the bucket.
            callback: A callback function for reporting progress.
        
        Returns:
            bool: True if upload was successful, False otherwise.
        """
        try:
            loop = asyncio.get_event_loop()
            await loop.run_in_executor(
                None,  # Uses the default executor
                lambda: self.s3_client.upload_file(file_path, config.WASABI_BUCKET, object_name, Callback=callback)
            )
            logger.info(f"Successfully uploaded {file_path} to {config.WASABI_BUCKET}/{object_name}")
            return True
        except Exception as e:
            logger.error(f"Failed to upload {file_path} to Wasabi: {e}")
            return False

# Instantiate the handler
wasabi = WasabiHandler()
  
