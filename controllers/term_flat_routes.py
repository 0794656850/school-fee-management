# Re-export blueprints from legacy routes package
from routes import term_flat_routes as _m
from routes.term_flat_routes import *  # noqa: F401,F403

