# Re-export blueprints from legacy routes package
from routes import reminder_routes as _m
from routes.reminder_routes import *  # noqa: F401,F403

