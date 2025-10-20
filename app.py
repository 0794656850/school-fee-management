from flask import Flask, render_template, request, redirect, url_for, jsonify, flash
import mysql.connector
from datetime import datetime

app = Flask(__name__)
app.secret_key = "secret123"
app.config["PROPAGATE_EXCEPTIONS"] = True


# ---------- DATABASE CONNECTION ----------
def get_db_connection():
    """Establish a connection to the MySQL database."""
    return mysql.connector.connect(
        host="localhost",
        user="root",
        password="9133orerO",
        database="school_fee_db"
    )


# ---------- DASHBOARD ----------
@app.route("/")
def dashboard():
    """Main dashboard with summary cards and recent payments."""
    db = get_db_connection()
    cursor = db.cursor(dictionary=True)

    # Totals
    cursor.execute("SELECT COUNT(*) AS total FROM students")
    total_students = cursor.fetchone()["total"]

    cursor.execute("SELECT COALESCE(SUM(amount), 0) AS total_collected FROM payments")
    total_collected = cursor.fetchone()["total_collected"]

    # Detect correct balance column
    cursor.execute("SHOW COLUMNS FROM students LIKE 'balance'")
    has_balance = bool(cursor.fetchone())
    column = "balance" if has_balance else "fee_balance"

    cursor.execute(f"SELECT COALESCE(SUM({column}), 0) AS total_balance FROM students")
    total_balance = cursor.fetchone()["total_balance"]

    # Total credit
    cursor.execute("SELECT COALESCE(SUM(credit), 0) AS total_credit FROM students")
    total_credit = cursor.fetchone()["total_credit"]

    # Recent payments
    cursor.execute("""
        SELECT p.id, s.name, s.class_name, p.amount, p.method, p.date
        FROM payments p
        JOIN students s ON p.student_id = s.id
        ORDER BY p.date DESC
        LIMIT 5
    """)
    recent_payments = cursor.fetchall()

    db.close()
    return render_template(
        "dashboard.html",
        total_students=total_students,
        total_fees_collected=total_collected,
        pending_balance=total_balance,
        total_credit=total_credit,
        recent_payments=recent_payments
    )


# ---------- REAL-TIME DASHBOARD API ----------
@app.route("/api/dashboard_data")
def dashboard_data():
    """Return real-time dashboard totals."""
    db = get_db_connection()
    cursor = db.cursor(dictionary=True)

    cursor.execute("SHOW COLUMNS FROM students LIKE 'balance'")
    has_balance = bool(cursor.fetchone())
    column = "balance" if has_balance else "fee_balance"

    cursor.execute("SELECT COUNT(*) AS total_students FROM students")
    total_students = cursor.fetchone()["total_students"]

    cursor.execute("SELECT COALESCE(SUM(amount), 0) AS total_collected FROM payments")
    total_collected = cursor.fetchone()["total_collected"]

    cursor.execute(f"SELECT COALESCE(SUM({column}), 0) AS total_balance FROM students")
    total_balance = cursor.fetchone()["total_balance"]

    cursor.execute("SELECT COALESCE(SUM(credit), 0) AS total_credit FROM students")
    total_credit = cursor.fetchone()["total_credit"]

    db.close()
    return jsonify({
        "total_students": total_students,
        "total_collected": float(total_collected or 0),
        "total_balance": float(total_balance or 0),
        "total_credit": float(total_credit or 0)
    })


# ---------- STUDENTS ----------
@app.route("/students")
def students():
    db = get_db_connection()
    cursor = db.cursor(dictionary=True)
    cursor.execute("SELECT * FROM students ORDER BY id DESC")
    students = cursor.fetchall()
    db.close()
    return render_template("students.html", students=students)


@app.route("/add_student", methods=["GET", "POST"])
def add_student():
    """Add a new student (with duplicate prevention)."""
    if request.method == "POST":
        name = request.form["name"].strip()
        admission_no = request.form.get("admission_no", "").strip()
        class_name = request.form["class_name"].strip()
        total_fees = float(request.form.get("total_fees", 0))

        db = get_db_connection()
        cursor = db.cursor(dictionary=True)

        # Detect correct column
        cursor.execute("SHOW COLUMNS FROM students LIKE 'balance'")
        has_balance = bool(cursor.fetchone())
        cursor.execute("SHOW COLUMNS FROM students LIKE 'fee_balance'")
        has_fee_balance = bool(cursor.fetchone())

        # Prevent duplicates
        cursor.execute("""
            SELECT id FROM students 
            WHERE LOWER(name) = LOWER(%s) OR admission_no = %s
        """, (name, admission_no))
        existing = cursor.fetchone()
        if existing:
            db.close()
            flash("⚠️ Student already exists.", "warning")
            return redirect(url_for("students"))

        # Insert
        if has_balance:
            sql = "INSERT INTO students (name, admission_no, class_name, balance, credit) VALUES (%s, %s, %s, %s, 0)"
        elif has_fee_balance:
            sql = "INSERT INTO students (name, admission_no, class_name, fee_balance, credit) VALUES (%s, %s, %s, %s, 0)"
        else:
            db.close()
            flash("❌ No valid balance column found in 'students' table!", "error")
            return redirect(url_for("students"))

        try:
            cursor.execute(sql, (name, admission_no, class_name, total_fees))
            db.commit()
            flash(f"✅ Student '{name}' added successfully!", "success")
        except Exception as e:
            db.rollback()
            flash(f"❌ Error adding student: {e}", "error")
        finally:
            db.close()

        return redirect(url_for("students"))

    return render_template("add_student.html")


@app.route("/delete_student/<int:student_id>", methods=["POST"])
def delete_student(student_id):
    """Delete student and related payments."""
    db = get_db_connection()
    cursor = db.cursor()
    cursor.execute("DELETE FROM payments WHERE student_id = %s", (student_id,))
    cursor.execute("DELETE FROM students WHERE id = %s", (student_id,))
    db.commit()
    db.close()
    flash("🗑️ Student deleted successfully!", "success")
    return redirect(url_for("students"))


# ---------- SEARCH ----------
@app.route("/search_student")
def search_student():
    """Search by name, class, or admission number."""
    query = request.args.get("query", "").strip()
    db = get_db_connection()
    cursor = db.cursor(dictionary=True)

    if query:
        like = f"%{query}%"
        cursor.execute("""
            SELECT * FROM students
            WHERE name LIKE %s OR class_name LIKE %s OR admission_no LIKE %s
            ORDER BY id DESC
        """, (like, like, like))
    else:
        cursor.execute("SELECT * FROM students ORDER BY id DESC")

    students = cursor.fetchall()
    db.close()
    return jsonify(students)


# ---------- STUDENT DETAIL ----------
@app.route("/student/<int:student_id>")
def student_detail(student_id):
    """View student profile and payments."""
    db = get_db_connection()
    cursor = db.cursor(dictionary=True)

    cursor.execute("SELECT * FROM students WHERE id = %s", (student_id,))
    student = cursor.fetchone()

    cursor.execute("""
        SELECT * FROM payments
        WHERE student_id = %s
        ORDER BY date DESC
    """, (student_id,))
    payments = cursor.fetchall()
    db.close()

    if not student:
        flash("⚠️ Student not found.", "error")
        return redirect(url_for("students"))

    return render_template("view_student.html", student=student, payments=payments)


# ---------- PAYMENTS ----------
@app.route("/payments", methods=["GET", "POST"])
def payments():
    """Add or list payments."""
    db = get_db_connection()
    cursor = db.cursor(dictionary=True)

    if request.method == "POST":
        student_id = request.form["student_id"]
        amount = float(request.form["amount"])
        method = request.form["method"]
        reference = request.form["reference"]
        payment_date = datetime.now()

        # Detect correct balance column
        cursor.execute("SHOW COLUMNS FROM students LIKE 'balance'")
        has_balance = bool(cursor.fetchone())
        column = "balance" if has_balance else "fee_balance"

        cursor.execute(f"SELECT {column}, credit FROM students WHERE id = %s", (student_id,))
        student = cursor.fetchone()

        if not student:
            db.close()
            flash("⚠️ Student not found!", "error")
            return redirect(url_for("payments"))

        current_balance = float(student[column] or 0)
        current_credit = float(student["credit"] or 0)

        if amount > current_balance:
            # Overpayment: pay balance, add extra to credit
            overpaid = amount - current_balance
            new_balance = 0
            new_credit = current_credit + overpaid
        else:
            # Normal payment: reduce balance
            new_balance = current_balance - amount
            new_credit = current_credit

        cursor.execute("""
            INSERT INTO payments (student_id, amount, method, reference, date)
            VALUES (%s, %s, %s, %s, %s)
        """, (student_id, amount, method, reference, payment_date))

        cursor.execute(f"UPDATE students SET {column} = %s, credit = %s WHERE id = %s",
                       (new_balance, new_credit, student_id))
        db.commit()
        db.close()

        flash(f"💰 Payment of KES {amount:,.2f} recorded! Remaining balance: KES {new_balance:,.2f}, Credit: KES {new_credit:,.2f}", "success")
        return redirect(url_for("payments"))

    # GET
    cursor.execute("""
        SELECT p.*, s.name AS student_name, s.class_name
        FROM payments p
        JOIN students s ON p.student_id = s.id
        ORDER BY p.date DESC
    """)
    payments = cursor.fetchall()

    cursor.execute("SELECT id, name FROM students ORDER BY name ASC")
    students = cursor.fetchall()
    db.close()
    return render_template("payments.html", payments=payments, students=students)


# ---------- ANALYTICS ----------
@app.route("/analytics")
def analytics():
    """Render analytics dashboard."""
    db = get_db_connection()
    cursor = db.cursor(dictionary=True)

    cursor.execute("SHOW COLUMNS FROM students LIKE 'balance'")
    has_balance = bool(cursor.fetchone())
    column = "balance" if has_balance else "fee_balance"

    cursor.execute("SELECT COUNT(*) AS total FROM students")
    total_students = cursor.fetchone()["total"]

    cursor.execute("SELECT COALESCE(SUM(amount), 0) AS total_collected FROM payments")
    total_collected = cursor.fetchone()["total_collected"]

    cursor.execute(f"SELECT COALESCE(SUM({column}), 0) AS total_balance FROM students")
    total_balance = cursor.fetchone()["total_balance"]

    cursor.execute("SELECT COALESCE(SUM(credit), 0) AS total_credit FROM students")
    total_credit = cursor.fetchone()["total_credit"]

    db.close()
    return render_template(
        "analytics.html",
        total_students=total_students,
        total_collected=total_collected,
        total_balance=total_balance,
        total_credit=total_credit
    )


# ---------- ANALYTICS DATA (LIVE) ----------
@app.route("/api/analytics_data")
def analytics_data():
    """Provide live analytics for charts and class summary."""
    db = get_db_connection()
    cursor = db.cursor(dictionary=True)

    # Monthly totals
    cursor.execute("""
        SELECT DATE_FORMAT(MIN(date), '%b %Y') AS month, SUM(amount) AS total
        FROM payments
        GROUP BY YEAR(date), MONTH(date)
        ORDER BY YEAR(date), MONTH(date)
    """)
    monthly_data = cursor.fetchall()

    # Class summary
    cursor.execute("""
        SELECT 
            s.class_name,
            COUNT(s.id) AS total_students,
            COALESCE(SUM(p.amount), 0) AS total_paid,
            COALESCE(SUM(COALESCE(s.balance, s.fee_balance)), 0) AS total_pending,
            COALESCE(SUM(s.credit), 0) AS total_credit
        FROM students s
        LEFT JOIN payments p ON s.id = p.student_id
        GROUP BY s.class_name
        ORDER BY s.class_name
    """)
    class_summary = cursor.fetchall()
    db.close()

    for row in class_summary:
        paid = float(row["total_paid"] or 0)
        pending = float(row["total_pending"] or 0)
        total = paid + pending
        row["percent_paid"] = round((paid / total * 100), 1) if total > 0 else 0

    return jsonify({
        "monthly_data": monthly_data,
        "class_summary": class_summary
    })


# ---------- RUN ----------
if __name__ == "__main__":
    app.run(debug=True)
