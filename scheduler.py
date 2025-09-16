from apscheduler.schedulers.background import BackgroundScheduler
from datetime import date, timedelta
from extensions import db
from models import Fee
from flask_mail import Message
from extensions import mail
from flask import current_app

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
    scheduler.start()
    return scheduler