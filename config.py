import os
try:
    from dotenv import load_dotenv  # type: ignore
    load_dotenv()
except Exception:
    # If python-dotenv isn't installed, continue; env vars can still come from OS
    pass

class Config:
    # --------------------------
    # 🔹 Flask Configuration
    # --------------------------
    SECRET_KEY = os.environ.get("SECRET_KEY", "secret123")
    PROPAGATE_EXCEPTIONS = True
    # Secure cookie/session defaults (tunable via env)
    SESSION_COOKIE_HTTPONLY = True
    SESSION_COOKIE_SAMESITE = os.environ.get("SESSION_COOKIE_SAMESITE", "Lax")
    SESSION_COOKIE_SECURE = (os.environ.get("SESSION_COOKIE_SECURE", "1").lower() not in ("0", "false", "no"))
    PERMANENT_SESSION_LIFETIME = int(os.environ.get("SESSION_LIFETIME_SECONDS", "1209600"))  # 14 days
    PREFERRED_URL_SCHEME = os.environ.get("PREFERRED_URL_SCHEME", "https" if SESSION_COOKIE_SECURE else "http")

    # --------------------------
    # 🔹 MySQL Database (SQLAlchemy)
    # --------------------------
    # Default to a local MySQL for dev; override with env in Docker/Prod
    SQLALCHEMY_DATABASE_URI = os.environ.get(
        "SQLALCHEMY_DATABASE_URI",
        "mysql+pymysql://root:9133orerO@localhost/school_fee_db",
    )
    SQLALCHEMY_TRACK_MODIFICATIONS = False

    # --------------------------
    # 🔹 Twilio SMS Configuration
    # --------------------------
    # (These should be stored securely as environment variables)
    TWILIO_ACCOUNT_SID = os.environ.get("TWILIO_ACCOUNT_SID", "ACxxxxxxxxxxxxxxxxxxxxxxxxxxxxx")
    TWILIO_AUTH_TOKEN = os.environ.get("TWILIO_AUTH_TOKEN", "your_auth_token_here")
    TWILIO_PHONE_NUMBER = os.environ.get("TWILIO_PHONE_NUMBER", "+2547XXXXXXX")

    # WhatsApp Cloud API (Meta)
    # Required for sending WhatsApp messages via Cloud API
    WHATSAPP_ACCESS_TOKEN = os.environ.get("WHATSAPP_ACCESS_TOKEN", "")
    WHATSAPP_PHONE_NUMBER_ID = os.environ.get("WHATSAPP_PHONE_NUMBER_ID", "")
    # Optional: default template config for business-initiated messages
    WHATSAPP_TEMPLATE_NAME = os.environ.get("WHATSAPP_TEMPLATE_NAME", "")  # e.g., 'fee_reminder'
    WHATSAPP_TEMPLATE_LANG = os.environ.get("WHATSAPP_TEMPLATE_LANG", "en_US")

    # --------------------------
    # 🔹 Other App Constants
    # --------------------------
    APP_NAME = "CS Fee Management"
    DEFAULT_COUNTRY_CODE = "+254"

    # Branding overrides (universal usage)
    # These allow white-labeling the portal for any school.
    # Default to a neutral brand until institution details are entered
    BRAND_NAME = os.environ.get("BRAND_NAME", "Fee Management System")
    PORTAL_TITLE = os.environ.get("PORTAL_TITLE", "Fee Management portal")
    # Composite app title used in receipts/headers (overrides earlier APP_NAME)
    APP_NAME = os.environ.get("APP_NAME", f"{BRAND_NAME} {PORTAL_TITLE}")
    # Static asset paths under the Flask `static/` folder
    LOGO_PRIMARY = os.environ.get("LOGO_PRIMARY", "css/lovato_logo.jpg")
    LOGO_SECONDARY = os.environ.get("LOGO_SECONDARY", "css/lovato_logo1.jpg")
    FAVICON = os.environ.get("FAVICON", LOGO_PRIMARY)
    # Support contact (used on login for password recovery)
    SUPPORT_PHONE = os.environ.get("SUPPORT_PHONE", "+254794656850")

    # --------------------------
    # Monetization / Billing
    # --------------------------
    # Simple license toggle; set via env for now. Advanced checks in utils.pro
    LICENSE_KEY = os.environ.get("LICENSE_KEY", "")
    PRO_ENABLED = bool(os.environ.get("PRO_ENABLED", "").strip() or LICENSE_KEY)
    BILLING_UPGRADE_URL = os.environ.get("BILLING_UPGRADE_URL", "https://buy.stripe.com/test_4gw6qk9Wg3oG3qYcMM")

    # --------------------------
    # Payments (QR/link)
    # --------------------------
    # If set, we render a QR code on receipts pointing to this payment link.
    # Example: a PayBill/Bank/M-Pesa/UPI checkout page you control.
    PAYMENT_LINK = os.environ.get("PAYMENT_LINK", "")

    # --------------------------
    # M-Pesa Daraja (STK Push)
    # --------------------------
    # Set via environment variables or .env file
    DARAJA_ENV = os.environ.get("DARAJA_ENV", "sandbox")  # sandbox or production
    DARAJA_CONSUMER_KEY = os.environ.get("DARAJA_CONSUMER_KEY", "")
    DARAJA_CONSUMER_SECRET = os.environ.get("DARAJA_CONSUMER_SECRET", "")
    DARAJA_SHORT_CODE = os.environ.get("DARAJA_SHORT_CODE", "")  # Paybill/Till (BusinessShortCode)
    DARAJA_PASSKEY = os.environ.get("DARAJA_PASSKEY", "")      # Lipa Na M-PESA Online Passkey
    DARAJA_CALLBACK_URL = os.environ.get("DARAJA_CALLBACK_URL", "")  # Public URL to /mpesa/callback
    DARAJA_ACCOUNT_REF = os.environ.get("DARAJA_ACCOUNT_REF", "FMS-PRO-2025T3")
    DARAJA_TRANSACTION_DESC = os.environ.get("DARAJA_TRANSACTION_DESC", "Fee Mgmt Pro Upgrade (2025 T3)")
    PRO_PRICE_KES = int(os.environ.get("PRO_PRICE_KES", "1500"))

    # --------------------------
    # App Login (simple session auth)
    # --------------------------
    LOGIN_USERNAME = os.environ.get("APP_LOGIN_USERNAME", "user")
    LOGIN_PASSWORD = os.environ.get("APP_LOGIN_PASSWORD", "9133")
