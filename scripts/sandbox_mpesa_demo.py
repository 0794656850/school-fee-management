import os, sys, time
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from flask import Flask
from config import Config
from utils.mpesa import stk_push


def main():
    # Force stub for local testing unless explicitly disabled
    os.environ.setdefault('DARAJA_STUB', '1')
    app = Flask(__name__)
    app.config.from_object(Config)
    with app.app_context():
        try:
            res = stk_push(phone='0712345678', amount=100, account_ref='DEMO', trans_desc='Sandbox Test')
            print('OK', res)
        except Exception as e:
            print('ERR', str(e))


if __name__ == '__main__':
    main()

