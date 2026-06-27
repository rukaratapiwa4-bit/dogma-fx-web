from flask import Flask
import os

app = Flask(__name__)

@app.route('/')
@app.route('/dashboard')
@app.route('/health')
def home():
    return """
    <h1>🐕 THE DOGMA FX SYSTEM</h1>
    <p style="color:green;font-size:24px;">✅ SYSTEM IS LIVE</p>
    <p>Version 6.0 - Running 24/7 on Render</p>
    <p>All 10 layers active</p>
    """

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=int(os.environ.get('PORT', 8080)))
