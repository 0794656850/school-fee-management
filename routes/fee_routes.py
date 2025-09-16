from flask import Blueprint, request, redirect, url_for, flash, render_template
from extensions import db
from models import Fee, Student
from utils import login_required
from datetime import datetime

fee_bp = Blueprint("fee_bp", __name__)

@fee_bp.route('/fees/add/<int:student_id>', methods=['POST'])
@login_required
def add_fee(student_id):
    term = request.form.get('term')
    amount_due = float(request.form.get('amount_due'))
    due_date = request.form.get('due_date')  # YYYY-MM-DD
    try:
        due_date_obj = datetime.strptime(due_date, "%Y-%m-%d").date()
    except:
        flash("Invalid date format", "danger")
        return redirect(url_for('student_bp.student_detail', student_id=student_id))
    fee = Fee(student_id=student_id, term=term, amount_due=amount_due, due_date=due_date_obj)
    db.session.add(fee)
    db.session.commit()
    flash("Fee added", "success")
    return redirect(url_for('student_bp.student_detail', student_id=student_id))

@fee_bp.route('/fees/pay/<int:fee_id>', methods=['POST'])
@login_required
def pay_fee(fee_id):
    fee = Fee.query.get_or_404(fee_id)
    amount = float(request.form.get('amount', 0))
    fee.amount_paid = (fee.amount_paid or 0) + amount
    if fee.amount_paid >= fee.amount_due:
        fee.status = "Paid"
    db.session.commit()
    flash("Payment recorded", "success")
    return redirect(url_for('student_bp.student_detail', student_id=fee.student_id))
