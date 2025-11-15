# Re-export blueprints from legacy routes package
from routes import newsletter_routes as _m
from routes.newsletter_routes import *  # noqa: F401,F403

