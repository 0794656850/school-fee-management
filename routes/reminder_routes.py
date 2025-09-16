from flask import Blueprint, jsonify, current_app
from extensions import mail, db
from flask_mail import Message
from models import Fee, Student
from utils import login_required
from flask import request

reminder_bp = Blueprint("reminder_bp", __name__)

def send_email_reminder(student, fee):
    if not student.parent_email:
        return False
    body = f"""Dear {student.parent_name or 'Parent/Guardian'},

This is a friendly reminder that {student.name} (Class: {student.student_class}) has a fee balance of KES {fee.balance:.2f}
due on {fee.due_date.strftime('%Y-%m-%d')} (Term: {fee.term}).

Please settle promptly to avoid penalties.

Regards,
School Admin
"""
    msg = Message(subject=f"Fee reminder for {student.name}",
                  sender=current_app.config.get("MAIL_SENDER"),
                  recipients=[student.parent_email],
                  body=body)
    try:
        mail.send(msg)
        return True
    except Exception as e:
        print("Mail error:", e)
        return False

@reminder_bp.route('/send-reminder/<int:fee_id>', methods=['POST'])
@login_required
def send_reminder(fee_id):
    fee = Fee.query.get_or_404(fee_id)
    ok = send_email_reminder(fee.student, fee)
    return jsonify({'sent': ok})
