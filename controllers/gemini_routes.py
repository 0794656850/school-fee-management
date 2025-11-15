# Re-export blueprints from legacy routes package
from routes import gemini_routes as _m
from routes.gemini_routes import *  # noqa: F401,F403

