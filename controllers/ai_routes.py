# Re-export blueprints from legacy routes package
from routes import ai_routes as _m
from routes.ai_routes import *  # noqa: F401,F403

