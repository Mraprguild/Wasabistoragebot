import os
import re
import base64
from typing import Dict, Any
from datetime import datetime

class HelperFunctions:
    # Media type mappings
    MEDIA_EXTENSIONS = {
        'video': ['.mp4', '.mov', '.avi', '.mkv', '.webm', '.m4v', '.flv', '.3gp'],
        'audio': ['.mp3', '.m4a', '.ogg', '.wav', '.flac', '.aac', '.wma'],
        'image': ['.jpg', '.jpeg', '.png', '.gif', '.bmp', '.webp', '.tiff', '.svg']
    }
    
    @staticmethod
    def get_file_type(filename: str) -> str:
        """Determine file type from extension"""
        ext = os.path.splitext(filename)[1].lower()
        for file_type, extensions in HelperFunctions.MEDIA_EXTENSIONS.items():
            if ext in extensions:
                return file_type
        return 'document'
    
    @staticmethod
    def humanbytes(size: int) -> str:
        """Convert bytes to human readable format"""
        if not size:
            return "0 B"
        
        units = ["B", "KB", "MB", "GB", "TB"]
        power = 1024
        
        for unit in units:
            if size < power:
                return f"{size:.2f} {unit}"
            size /= power
        return f"{size:.2f} TB"
    
    @staticmethod
    def format_duration(seconds: int) -> str:
        """Format seconds into HH:MM:SS or MM:SS"""
        if seconds <= 0:
            return "00:00"
        
        hours, remainder = divmod(seconds, 3600)
        minutes, seconds = divmod(remainder, 60)
        
        if hours > 0:
            return f"{int(hours):02d}:{int(minutes):02d}:{int(seconds):02d}"
        return f"{int(minutes):02d}:{int(seconds):02d}"
    
    @staticmethod
    def create_progress_bar(percentage: float, length: int = 20) -> str:
        """Create visual progress bar"""
        filled = int(length * percentage / 100)
        empty = length - filled
        return '█' * filled + '○' * empty
    
    @staticmethod
    def encode_url(url: str) -> str:
        """Base64 encode URL for web player"""
        encoded = base64.urlsafe_b64encode(url.encode()).decode()
        return encoded.rstrip('=')
    
    @staticmethod
    def decode_url(encoded_url: str) -> str:
        """Base64 decode URL"""
        padding = 4 - (len(encoded_url) % 4)
        if padding != 4:
            encoded_url += '=' * padding
        return base64.urlsafe_b64decode(encoded_url).decode()
    
    @staticmethod
    def get_user_folder(user_id: int) -> str:
        """Generate user-specific folder path"""
        return f"users/{user_id}"
    
    @staticmethod
    def format_file_list(files: list, page: int = 1, per_page: int = 10) -> str:
        """Format file list for display"""
        start_idx = (page - 1) * per_page
        end_idx = start_idx + per_page
        page_files = files[start_idx:end_idx]
        
        if not page_files:
            return "No files found."
        
        file_list = []
        for i, file in enumerate(page_files, start=start_idx + 1):
            size = HelperFunctions.humanbytes(file['size'])
            file_list.append(f"{i}. {file['name']} ({size})")
        
        result = "\n".join(file_list)
        
        if len(files) > per_page:
            total_pages = (len(files) + per_page - 1) // per_page
            result += f"\n\nPage {page} of {total_pages}"
        
        return result
