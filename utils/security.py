import hashlib
import hmac
import time
from datetime import datetime, timedelta
from typing import Optional
import secrets

class SecurityManager:
    def __init__(self, secret_key: str):
        self.secret_key = secret_key
    
    def generate_token(self, user_id: int, filename: str, expires_in: int = 3600) -> str:
        """Generate secure token for web player access"""
        timestamp = str(int(time.time() + expires_in))
        data = f"{user_id}:{filename}:{timestamp}"
        signature = hmac.new(
            self.secret_key.encode(), 
            data.encode(), 
            hashlib.sha256
        ).hexdigest()
        return f"{data}:{signature}"
    
    def verify_token(self, token: str) -> Optional[dict]:
        """Verify token and return payload if valid"""
        try:
            parts = token.split(':')
            if len(parts) != 4:
                return None
                
            user_id, filename, expiry, signature = parts
            data = f"{user_id}:{filename}:{expiry}"
            
            # Verify signature
            expected_sig = hmac.new(
                self.secret_key.encode(), 
                data.encode(), 
                hashlib.sha256
            ).hexdigest()
            
            if not hmac.compare_digest(signature, expected_sig):
                return None
            
            # Check expiry
            if int(expiry) < time.time():
                return None
                
            return {'user_id': int(user_id), 'filename': filename}
        except:
            return None
    
    def sanitize_filename(self, filename: str) -> str:
        """Secure filename sanitization"""
        # Remove path traversal attempts
        filename = filename.replace('../', '').replace('./', '')
        # Keep only safe characters
        filename = ''.join(c for c in filename if c.isalnum() or c in '._- ')
        # Limit length
        if len(filename) > 200:
            name, ext = os.path.splitext(filename)
            filename = name[:200-len(ext)] + ext
        return filename
