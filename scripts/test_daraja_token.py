import os, sys
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
from flask import Flask
from config import Config
from utils.mpesa import get_access_token

app = Flask(__name__)
app.config.from_object(Config)

with app.app_context():
    try:
        token = get_access_token(timeout=20)
        print('OK', token[:6] + '...')
    except Exception as e:
        print('ERR', str(e))
