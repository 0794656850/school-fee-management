# Re-export blueprints from legacy routes package
from routes import gmail_oauth_routes as _m
from routes.gmail_oauth_routes import *  # noqa: F401,F403

