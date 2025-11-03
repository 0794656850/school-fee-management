from apscheduler.schedulers.background import BackgroundScheduler
from datetime import date, timedelta
from extensions import db
from models import Fee
from flask_mail import Message
from extensions import mail
from flask import current_app
import os
import subprocess
from pathlib import Path

def daily_job(app):
    with app.app_context():
        days = current_app.config.get('REMINDER_DAYS', 3)
        today = date.today()
        threshold = today + timedelta(days=days)
        fees = Fee.query.filter(Fee.due_date <= threshold, Fee.status == 'Pending').all()
        for fee in fees:
            # Use reminder logic from routes or duplicate simple send
            if fee.student and fee.student.parent_email:
                body = f"Reminder: {fee.student.name} has balance KES {fee.balance:.2f} due {fee.due_date}"
                msg = Message(subject=f"Fee reminder for {fee.student.name}",
                              sender=current_app.config.get('MAIL_SENDER'),
                              recipients=[fee.student.parent_email],
                              body=body)
                try:
                    mail.send(msg)
                    print("Sent reminder to", fee.student.parent_email)
                except Exception as e:
                    print("Failed to send:", e)

def start_scheduler(app):
    scheduler = BackgroundScheduler()
    scheduler.add_job(lambda: daily_job(app), 'interval', hours=24, id='daily_reminder', replace_existing=True)
    # Always keep the AI knowledge refreshed every ~6 hours by default
    try:
        interval_hours = int(os.environ.get('AI_LEARN_INTERVAL_HOURS', '6'))
        scheduler.add_job(_run_ai_learn, 'interval', hours=interval_hours, id='ai_learn_refresh', replace_existing=True)
    except Exception:
        pass
    # Optional: background knowledge base reindexing
    try:
        if os.environ.get('AI_REINDEX', '0') in ('1', 'true', 'True'):
            # Reindex project weekly
            scheduler.add_job(lambda: _run_ai_index(False), 'interval', days=7, id='kb_project_index', replace_existing=True)
            # Reindex user KB daily if paths provided
            if os.environ.get('AI_USER_KB_PATHS'):
                scheduler.add_job(lambda: _run_ai_index(True), 'interval', days=1, id='kb_user_index', replace_existing=True)
    except Exception:
        pass
    scheduler.start()
    return scheduler


def _run_ai_index(user: bool):
    """Run AI indexers in a subprocess to avoid blocking the scheduler thread."""
    try:
        repo_root = Path(__file__).resolve().parents[0]
        if user:
            paths = os.environ.get('AI_USER_KB_PATHS', '')
            if not paths:
                return
            cmd = ['python', str(repo_root / 'scripts' / 'ai_index_dirs.py'), '--paths', paths]
        else:
            cmd = ['python', str(repo_root / 'scripts' / 'ai_index.py')]
        subprocess.Popen(cmd)
    except Exception:
        pass


def _run_ai_learn():
    """Run the new ai_engine.learn pipeline in a subprocess."""
    try:
        repo_root = Path(__file__).resolve().parents[0]
        cmd = ['python', '-m', 'ai_engine.learn']
        subprocess.Popen(cmd, cwd=str(repo_root))
    except Exception:
        pass
