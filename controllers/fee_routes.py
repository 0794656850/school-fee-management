# Re-export blueprints from legacy routes package
from routes import fee_routes as _m
from routes.fee_routes import *  # noqa: F401,F403

