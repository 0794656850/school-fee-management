import os

class Config:
    # MySQL connection string using pymysql
    SQLALCHEMY_DATABASE_URI = "mysql+pymysql://root:9133orerO@localhost/school_fee_db"
    SQLALCHEMY_TRACK_MODIFICATIONS = False
    SECRET_KEY = os.environ.get("SECRET_KEY") or "your_secret_key"
