import asyncio
import boto3
from botocore.exceptions import (
    NoCredentialsError, 
    PartialCredentialsError, 
    ClientError,
    ParamValidationError
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
            # Validate required credentials first
            if not all([config.WASABI_ACCESS_KEY, config.WASABI_SECRET_KEY, config.WASABI_BUCKET]):
                raise ValueError("Missing required Wasabi credentials in configuration")
            
            # Create session with basic configuration first
            self.session = boto3.session.Session()
            
            # Build client configuration step by step
            client_config = {
                'service_name': 's3',
                'region_name': config.WASABI_REGION,
                'aws_access_key_id': config.WASABI_ACCESS_KEY,
                'aws_secret_access_key': config.WASABI_SECRET_KEY
            }
            
            # Only add endpoint_url if it's a valid Wasabi region
            wasabi_regions = ['us-east-1', 'us-east-2', 'us-central-1', 'us-west-1', 
                            'eu-central-1', 'eu-west-1', 'eu-west-2', 'ap-northeast-1', 'ap-northeast-2']
            
            if config.WASABI_REGION in wasabi_regions:
                client_config['endpoint_url'] = f'https://s3.{config.WASABI_REGION}.wasabisys.com'
            else:
                logger.warning(f"Unknown Wasabi region: {config.WASABI_REGION}. Using default endpoint.")
            
            self.s3_client = self.session.client(**client_config)
            
            # Test connection with simpler call
            logger.info("Testing Wasabi connection...")
            self.s3_client.list_buckets()  # Simple API call to test credentials
            
            # Verify bucket exists and is accessible
            try:
                self.s3_client.head_bucket(Bucket=config.WASABI_BUCKET)
                logger.info(f"✅ Wasabi S3 client initialized. Bucket '{config.WASABI_BUCKET}' is accessible.")
            except ClientError as e:
                error_code = e.response['Error']['Code']
                if error_code == '404':
                    logger.error(f"❌ Bucket '{config.WASABI_BUCKET}' not found.")
                    raise
                elif error_code == '403':
                    logger.error(f"❌ Access denied to bucket '{config.WASABI_BUCKET}'. Check permissions.")
                    raise
                else:
                    logger.warning(f"Bucket check returned: {e}. Continuing anyway...")
            
        except ParamValidationError as e:
            logger.error(f"❌ Parameter validation error: {e}")
            # Provide more specific error message
            if 'endpoint_url' in str(e):
                logger.error("Invalid endpoint URL format. Check WASABI_REGION.")
            raise
        except NoCredentialsError:
            logger.error("❌ No Wasabi credentials found. Check WASABI_ACCESS_KEY and WASABI_SECRET_KEY.")
            raise
        except PartialCredentialsError:
            logger.error("❌ Incomplete Wasabi credentials. Both ACCESS_KEY and SECRET_KEY are required.")
            raise
        except ClientError as e:
            error_code = e.response['Error']['Code']
            if error_code == 'InvalidAccessKeyId':
                logger.error("❌ Invalid Wasabi Access Key ID.")
            elif error_code == 'SignatureDoesNotMatch':
                logger.error("❌ Invalid Wasabi Secret Key.")
            else:
                logger.error(f"❌ AWS Client Error: {e}")
            raise
        except Exception as e:
            logger.error(f"❌ Unexpected error during Wasabi client initialization: {e}")
            raise

    async def upload_file(self, file_path: str, object_name: str, callback=None):
        """Asynchronously uploads a file to the Wasabi bucket"""
        try:
            # Validate file exists
            if not os.path.exists(file_path):
                logger.error(f"File not found: {file_path}")
                return False
            
            # Validate file size
            file_size = os.path.getsize(file_path)
            if file_size == 0:
                logger.error(f"File is empty: {file_path}")
                return False
            
            loop = asyncio.get_event_loop()
            
            # Upload file with progress callback
            await loop.run_in_executor(
                None,
                self._upload_file_sync,
                file_path,
                object_name,
                callback
            )
            
            logger.info(f"✅ Successfully uploaded {object_name} to {config.WASABI_BUCKET}")
            return True
            
        except ClientError as e:
            logger.error(f"❌ S3 upload error for {object_name}: {e}")
            return False
        except Exception as e:
            logger.error(f"❌ Unexpected error uploading {object_name}: {e}")
            return False

    def _upload_file_sync(self, file_path: str, object_name: str, callback=None):
        """Synchronous upload method for executor"""
        try:
            self.s3_client.upload_file(
                Filename=file_path,
                Bucket=config.WASABI_BUCKET,
                Key=object_name,
                Callback=callback,
                ExtraArgs={
                    'ACL': 'private',
                    'StorageClass': 'STANDARD'
                }
            )
            return True
        except Exception as e:
            logger.error(f"Upload failed: {e}")
            raise

    async def check_bucket_access(self):
        """Check if we have access to the bucket"""
        try:
            await asyncio.get_event_loop().run_in_executor(
                None, 
                lambda: self.s3_client.head_bucket(Bucket=config.WASABI_BUCKET)
            )
            return True
        except ClientError as e:
            logger.error(f"Bucket access check failed: {e}")
            return False
        except Exception as e:
            logger.error(f"Unexpected error checking bucket access: {e}")
            return False

    async def get_bucket_info(self):
        """Get information about the bucket"""
        try:
            response = await asyncio.get_event_loop().run_in_executor(
                None,
                lambda: self.s3_client.get_bucket_location(Bucket=config.WASABI_BUCKET)
            )
            return response
        except Exception as e:
            logger.error(f"Failed to get bucket info: {e}")
            return None

# Instantiate the handler
wasabi = WasabiHandler()
