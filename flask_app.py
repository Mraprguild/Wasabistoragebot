from flask import Flask, render_template, request, jsonify, flash, redirect, url_for
import base64
import os
from utils.security import SecurityManager
from utils.helpers import HelperFunctions

app = Flask(__name__)
app.secret_key = os.getenv('SECRET_KEY', 'dev-secret-key')

security = SecurityManager(app.secret_key)

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/player/<media_type>/<encoded_url>')
def player(media_type, encoded_url):
    try:
        # Decode URL
        media_url = HelperFunctions.decode_url(encoded_url)
        
        # Verify token if present
        token = request.args.get('token')
        if token:
            payload = security.verify_token(token)
            if not payload:
                return "Invalid or expired token", 403
        
        return render_template('player.html', 
                             media_type=media_type, 
                             media_url=media_url)
    except Exception as e:
        return f"Error loading player: {str(e)}", 400

@app.route('/dashboard')
def dashboard():
    return render_template('dashboard.html')

@app.route('/about')
def about():
    return render_template('about.html')

@app.errorhandler(404)
def not_found(error):
    return render_template('404.html'), 404

@app.errorhandler(500)
def internal_error(error):
    return render_template('500.html'), 500

if __name__ == '__main__':
    port = int(os.getenv('PORT', 8000))
    app.run(host='0.0.0.0', port=port, debug=False)
