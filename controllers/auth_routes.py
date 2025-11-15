# Re-export blueprints from legacy routes package
from routes import auth_routes as _m
from routes.auth_routes import *  # noqa: F401,F403

