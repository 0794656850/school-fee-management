from flask import Blueprint, render_template, request, redirect, url_for, flash
from models import db, Student, Payment
from datetime import datetime

fee_bp = Blueprint('fees', __name__, url_prefix='/fees')

@fee_bp.route('/add', methods=['GET', 'POST'])
def add_payment():
    if request.method == 'POST':
        student_id = request.form['student_id']
        amount = float(request.form['amount'])
        payment_date = datetime.strptime(request.form['payment_date'], '%Y-%m-%d')
        method = request.form['method']
        reference = request.form.get('reference')

        new_payment = Payment(
            student_id=student_id,
            amount=amount,
            payment_date=payment_date,
            method=method,
            reference=reference
        )

        student = Student.query.get(student_id)
        if student:
            student.balance -= amount

        db.session.add(new_payment)
        db.session.commit()
        flash("✅ Payment recorded successfully!", "success")
        return redirect(url_for('students.view_students'))

    students = Student.query.all()
    return render_template('add_payment.html', students=students)
