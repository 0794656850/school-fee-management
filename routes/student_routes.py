from flask import Blueprint, render_template, request, redirect, url_for, flash, session
from extensions import db
from models import Student

student_bp = Blueprint("students", __name__)

# ✅ Protect routes (only logged-in users can access)
@student_bp.before_request
def check_login():
    if "user" not in session:
        return redirect(url_for("auth.login"))

# ✅ List all students
@student_bp.route("/students")
def list_students():
    students = Student.query.all()
    return render_template("students.html", students=students)

# ✅ Add new student
@student_bp.route("/students/add", methods=["POST"])
def add_student():
    name = request.form.get("name")
    email = request.form.get("email")
    phone = request.form.get("phone")

    if not name or not email:
        flash("Name and Email are required!", "danger")
        return redirect(url_for("students.list_students"))

    new_student = Student(name=name, email=email, phone=phone)
    db.session.add(new_student)
    db.session.commit()

    flash("Student added successfully!", "success")
    return redirect(url_for("students.list_students"))

# ✅ Delete student
@student_bp.route("/students/delete/<int:student_id>")
def delete_student(student_id):
    student = Student.query.get_or_404(student_id)
    db.session.delete(student)
    db.session.commit()

    flash("Student deleted successfully!", "info")
    return redirect(url_for("students.list_students"))

# ✅ Student details
@student_bp.route("/students/<int:student_id>")
def student_detail(student_id):
    student = Student.query.get_or_404(student_id)
    return render_template("student_detail.html", student=student)
