# web_server.py
import os
import base64
from flask import Flask, render_template, request, jsonify
from dotenv import load_dotenv
import boto3
import botocore
from threading import Thread
import logging

# Load environment variables
load_dotenv()

# Configuration
WASABI_ACCESS_KEY = os.getenv("WASABI_ACCESS_KEY")
WASABI_SECRET_KEY = os.getenv("WASABI_SECRET_KEY")
WASABI_BUCKET = os.getenv("WASABI_BUCKET")
WASABI_REGION = os.getenv("WASABI_REGION", "us-east-1")
RENDER_URL = os.getenv("RENDER_URL", "http://localhost:8000")

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Initialize S3 client
s3_client = boto3.client(
    's3',
    endpoint_url=f'https://s3.{WASABI_REGION}.wasabisys.com',
    aws_access_key_id=WASABI_ACCESS_KEY,
    aws_secret_access_key=WASABI_SECRET_KEY,
    region_name=WASABI_REGION,
    config=botocore.config.Config(
        signature_version='s3v4',
        retries={'max_attempts': 3}
    )
)

# Flask app
app = Flask(__name__, template_folder="templates")

# Media extensions for classification
MEDIA_EXTENSIONS = {
    'video': ['.mp4', '.mov', '.avi', '.mkv', '.webm', '.m4v', '.flv', '.wmv'],
    'audio': ['.mp3', '.m4a', '.ogg', '.wav', '.flac', '.aac', '.wma'],
    'image': ['.jpg', '.jpeg', '.png', '.gif', '.bmp', '.webp', '.tiff', '.svg']
}

def get_file_type(filename):
    """Determine file type based on extension"""
    ext = os.path.splitext(filename)[1].lower()
    for file_type, extensions in MEDIA_EXTENSIONS.items():
        if ext in extensions:
            return file_type
    return 'other'

def decode_media_url(encoded_url):
    """Decode base64 encoded media URL"""
    try:
        # Add padding if necessary
        padding = 4 - (len(encoded_url) % 4)
        if padding != 4:
            encoded_url += '=' * padding
        return base64.urlsafe_b64decode(encoded_url).decode()
    except Exception as e:
        logger.error(f"Error decoding URL: {e}")
        return None

@app.route("/")
def index():
    """Main page with upload/download interface"""
    return render_template("index.html", render_url=RENDER_URL)

@app.route("/player/<media_type>/<encoded_url>")
def player(media_type, encoded_url):
    """Media player page"""
    media_url = decode_media_url(encoded_url)
    if not media_url:
        return "Invalid media URL", 400
    
    return render_template("player.html", 
                         media_type=media_type, 
                         media_url=media_url,
                         filename=os.path.basename(media_url.split('?')[0]))

@app.route("/browse")
def browse_files():
    """File browser interface"""
    return render_template("index.html")

@app.route("/api/files")
def list_files():
    """API endpoint to list user files"""
    try:
        # This would typically require user authentication
        # For now, we'll return a sample or implement basic listing
        prefix = request.args.get('prefix', '')
        
        response = s3_client.list_objects_v2(
            Bucket=WASABI_BUCKET,
            Prefix=prefix,
            MaxKeys=50
        )
        
        files = []
        if 'Contents' in response:
            for obj in response['Contents']:
                file_type = get_file_type(obj['Key'])
                files.append({
                    'name': os.path.basename(obj['Key']),
                    'key': obj['Key'],
                    'size': obj['Size'],
                    'last_modified': obj['LastModified'].isoformat(),
                    'type': file_type
                })
        
        return jsonify({'files': files})
    
    except Exception as e:
        logger.error(f"Error listing files: {e}")
        return jsonify({'error': str(e)}), 500

@app.route("/api/generate-url")
def generate_download_url():
    """Generate presigned URL for download"""
    try:
        file_key = request.args.get('key')
        if not file_key:
            return jsonify({'error': 'Missing file key'}), 400
        
        # Generate presigned URL
        presigned_url = s3_client.generate_presigned_url(
            'get_object',
            Params={'Bucket': WASABI_BUCKET, 'Key': file_key},
            ExpiresIn=3600  # 1 hour
        )
        
        return jsonify({'url': presigned_url})
    
    except Exception as e:
        logger.error(f"Error generating URL: {e}")
        return jsonify({'error': str(e)}), 500

@app.route("/health")
def health_check():
    """Health check endpoint for monitoring"""
    try:
        # Test S3 connection
        s3_client.head_bucket(Bucket=WASABI_BUCKET)
        return jsonify({'status': 'healthy', 'service': 'web_server'})
    except Exception as e:
        logger.error(f"Health check failed: {e}")
        return jsonify({'status': 'unhealthy', 'error': str(e)}), 503

@app.errorhandler(404)
def not_found(error):
    return render_template('404.html'), 404

@app.errorhandler(500)
def internal_error(error):
    return render_template('500.html'), 500

def run_flask_server(host="0.0.0.0", port=8000, debug=False):
    """Run the Flask web server"""
    logger.info(f"Starting Flask web server on {host}:{port}")
    app.run(host=host, port=port, debug=debug, threaded=True)

if __name__ == "__main__":
    run_flask_server()
