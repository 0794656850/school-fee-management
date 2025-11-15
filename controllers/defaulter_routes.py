# Re-export blueprints from legacy routes package
from routes import defaulter_routes as _m
from routes.defaulter_routes import *  # noqa: F401,F403

