import asyncio
import boto3
from botocore.exceptions import (
    NoCredentialsError, 
    PartialCredentialsError, 
    ClientError
)
from config import config
import logging

logger = logging.getLogger(__name__)

class WasabiHandler:
    """Handles connections and file uploads to Wasabi S3 storage"""
    
    def __init__(self):
        self.s3_client = None
        self._initialize_client()
    
    def _initialize_client(self):
        """Initialize the S3 client with credentials"""
        try:
            self.session = boto3.session.Session()
            self.s3_client = self.session.client(
                's3',
                region_name=config.WASABI_REGION,
                endpoint_url=f'https://s3.{config.WASABI_REGION}.wasabisys.com',
                aws_access_key_id=config.WASABI_ACCESS_KEY,
                aws_secret_access_key=config.WASABI_SECRET_KEY
            )
            
            # Test connection
            self.s3_client.head_bucket(Bucket=config.WASABI_BUCKET)
            logger.info("Wasabi S3 client initialized and connection verified")
            
        except (NoCredentialsError, PartialCredentialsError) as e:
            logger.error(f"Credentials error: {e}")
            raise
        except ClientError as e:
            error_code = e.response['Error']['Code']
            if error_code == '404':
                logger.error(f"Bucket '{config.WASABI_BUCKET}' not found")
            elif error_code == '403':
                logger.error("Access denied - check credentials and bucket permissions")
            else:
                logger.error(f"S3 client error: {e}")
            raise
        except Exception as e:
            logger.error(f"Unexpected error during Wasabi client initialization: {e}")
            raise

    async def upload_file(self, file_path: str, object_name: str, callback=None):
        """Asynchronously uploads a file to the Wasabi bucket"""
        try:
            loop = asyncio.get_event_loop()
            
            await loop.run_in_executor(
                None,
                lambda: self.s3_client.upload_file(
                    Filename=file_path,
                    Bucket=config.WASABI_BUCKET,
                    Key=object_name,
                    Callback=callback,
                    ExtraArgs={
                        'ACL': 'private',  # Set appropriate permissions
                        'StorageClass': 'STANDARD'
                    }
                )
            )
            
            logger.info(f"Successfully uploaded {file_path} to {config.WASABI_BUCKET}/{object_name}")
            return True
            
        except ClientError as e:
            logger.error(f"S3 upload error for {file_path}: {e}")
            return False
        except Exception as e:
            logger.error(f"Unexpected error uploading {file_path}: {e}")
            return False

    async def check_bucket_access(self):
        """Check if we have access to the bucket"""
        try:
            await asyncio.get_event_loop().run_in_executor(
                None, 
                lambda: self.s3_client.head_bucket(Bucket=config.WASABI_BUCKET)
            )
            return True
        except ClientError:
            return False

# Instantiate the handler
wasabi = WasabiHandler()
