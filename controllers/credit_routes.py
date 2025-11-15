# Re-export blueprints from legacy routes package
from routes import credit_routes as _m
from routes.credit_routes import *  # noqa: F401,F403

