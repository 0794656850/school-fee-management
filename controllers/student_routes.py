# Re-export blueprints from legacy routes package
from routes import student_routes as _m
from routes.student_routes import *  # noqa: F401,F403

