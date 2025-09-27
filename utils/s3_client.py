import boto3
import botocore
import logging
from typing import Optional, Dict, List
import asyncio
from threading import Thread

logger = logging.getLogger(__name__)

class WasabiClient:
    def __init__(self, access_key: str, secret_key: str, bucket: str, region: str):
        self.access_key = access_key
        self.secret_key = secret_key
        self.bucket = bucket
        self.region = region
        self.client = self._create_client()
    
    def _create_client(self):
        """Create and configure Wasabi S3 client"""
        endpoint_url = f'https://s3.{self.region}.wasabisys.com'
        
        try:
            client = boto3.client(
                's3',
                endpoint_url=endpoint_url,
                aws_access_key_id=self.access_key,
                aws_secret_access_key=self.secret_key,
                region_name=self.region,
                config=boto3.session.Config(
                    s3={'addressing_style': 'virtual'},
                    signature_version='s3v4',
                    retries={'max_attempts': 3}
                )
            )
            
            # Test connection
            client.head_bucket(Bucket=self.bucket)
            logger.info("✅ Successfully connected to Wasabi bucket")
            return client
            
        except Exception as e:
            logger.error(f"❌ Wasabi connection failed: {e}")
            raise
    
    def upload_file(self, file_path: str, s3_key: str) -> bool:
        """Upload file to Wasabi with error handling"""
        try:
            self.client.upload_file(file_path, self.bucket, s3_key)
            return True
        except Exception as e:
            logger.error(f"Upload failed for {s3_key}: {e}")
            return False
    
    def download_file(self, s3_key: str, local_path: str) -> bool:
        """Download file from Wasabi"""
        try:
            self.client.download_file(self.bucket, s3_key, local_path)
            return True
        except Exception as e:
            logger.error(f"Download failed for {s3_key}: {e}")
            return False
    
    def generate_presigned_url(self, s3_key: str, expires_in: int = 86400) -> Optional[str]:
        """Generate presigned URL for secure access"""
        try:
            url = self.client.generate_presigned_url(
                'get_object',
                Params={'Bucket': self.bucket, 'Key': s3_key},
                ExpiresIn=expires_in
            )
            return url
        except Exception as e:
            logger.error(f"URL generation failed for {s3_key}: {e}")
            return None
    
    def delete_file(self, s3_key: str) -> bool:
        """Delete file from Wasabi"""
        try:
            self.client.delete_object(Bucket=self.bucket, Key=s3_key)
            return True
        except Exception as e:
            logger.error(f"Delete failed for {s3_key}: {e}")
            return False
    
    def list_user_files(self, user_prefix: str) -> List[Dict]:
        """List all files for a user"""
        try:
            response = self.client.list_objects_v2(
                Bucket=self.bucket,
                Prefix=user_prefix
            )
            
            files = []
            if 'Contents' in response:
                for obj in response['Contents']:
                    files.append({
                        'key': obj['Key'],
                        'size': obj['Size'],
                        'last_modified': obj['LastModified']
                    })
            return files
        except Exception as e:
            logger.error(f"List files failed: {e}")
            return []
    
    def get_file_info(self, s3_key: str) -> Optional[Dict]:
        """Get file metadata"""
        try:
            response = self.client.head_object(Bucket=self.bucket, Key=s3_key)
            return {
                'size': response['ContentLength'],
                'last_modified': response['LastModified'],
                'content_type': response.get('ContentType', 'unknown')
            }
        except botocore.exceptions.ClientError as e:
            if e.response['Error']['Code'] == '404':
                return None
            raise
