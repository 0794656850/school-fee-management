# Re-export blueprints from legacy routes package
from routes import mpesa_routes as _m
from routes.mpesa_routes import *  # noqa: F401,F403

