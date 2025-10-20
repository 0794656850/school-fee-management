from flask import Blueprint, render_template, request, redirect, url_for, flash
from models import db, Student

student_bp = Blueprint('students', __name__, url_prefix='/students')

@student_bp.route('/')
def view_students():
    students = Student.query.all()
    return render_template('view_students.html', students=students)

@student_bp.route('/add', methods=['GET', 'POST'])
def add_student():
    if request.method == 'POST':
        name = request.form['name']
        admission_no = request.form['admission_no']
        class_name = request.form['class_name']
        balance = float(request.form.get('balance', 0))

        new_student = Student(
            name=name,
            admission_no=admission_no,
            class_name=class_name,
            balance=balance
        )

        db.session.add(new_student)
        db.session.commit()
        flash('✅ Student added successfully!', 'success')
        return redirect(url_for('students.view_students'))

    return render_template('add_student.html')
