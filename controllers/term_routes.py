# Re-export blueprints from legacy routes package
from routes import term_routes as _m
from routes.term_routes import *  # noqa: F401,F403

