# Re-export blueprints from legacy routes package
from routes import admin_routes as _m
from routes.admin_routes import *  # noqa: F401,F403

