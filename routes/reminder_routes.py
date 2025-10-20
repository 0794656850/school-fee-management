from flask import Blueprint

reminder_bp = Blueprint('reminders', __name__, url_prefix='/reminders')

@reminder_bp.route('/')
def reminders_home():
    return "Reminders (coming soon)"
