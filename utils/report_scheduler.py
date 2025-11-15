from apscheduler.schedulers.background import BackgroundScheduler
from flask import current_app
from flask_mail import Message
from utils.pro import is_pro_enabled
from utils.settings import get_setting
from io import BytesIO, StringIO
import csv
from datetime import datetime

# New job: send weekly/monthly full report to school email (Pro only)
def _build_full_report_bytes(app):
    from app import get_db_connection  # lazy import to avoid circular at import time
    db = get_db_connection()
    cur = db.cursor(dictionary=True)
    sid = current_app.session_interface.open_session(current_app, current_app.request_context.environ.get('werkzeug.request')) if False else None
    # We cannot access session here; use school_id from settings (single-tenant) or skip if unknown
    try:
        school_id = None
        try:
            school_id = int(get_setting('SCHOOL_ID') or 0) or None
        except Exception:
            school_id = None
        if school_id is None:
            # Fallback: best effort, pick from env
            import os
            school_id = int(os.environ.get('DEFAULT_SCHOOL_ID','0') or 0) or None
    except Exception:
        school_id = None
    if not school_id:
        db.close(); return None

    # Students
    cur.execute("SELECT name AS Name, admission_no AS `Admission No`, class_name AS Class, COALESCE(balance,fee_balance) AS `Balance (KES)`, COALESCE(credit,0) AS `Credit (KES)` FROM students WHERE school_id=%s ORDER BY class_name, name", (school_id,))
    students = cur.fetchall() or []

    # Payments
    cur.execute("SELECT s.name AS `Student Name`, s.admission_no AS `Admission No`, s.class_name AS Class, p.year AS Year, p.term AS Term, p.amount AS `Amount (KES)`, p.method AS Method, p.reference AS Reference, p.date AS Date FROM payments p JOIN students s ON s.id=p.student_id WHERE p.school_id=%s ORDER BY p.date DESC", (school_id,))
    payments = cur.fetchall() or []

    # Class summary
    cur.execute("SELECT class_name AS Class, COUNT(*) AS `Total Students`, COALESCE(SUM(COALESCE(balance,fee_balance)),0) AS `Total Pending (KES)`, COALESCE(SUM(credit),0) AS `Total Credit (KES)` FROM students WHERE school_id=%s GROUP BY class_name ORDER BY class_name", (school_id,))
    class_summary = cur.fetchall() or []

    # Term summary
    cur.execute("SELECT p.year AS Year, p.term AS Term, COALESCE(SUM(p.amount),0) AS `Total Collected (KES)` FROM payments p WHERE p.school_id=%s AND p.method <> 'Credit Transfer' GROUP BY p.year, p.term ORDER BY p.year, p.term", (school_id,))
    term_summary = cur.fetchall() or []

    # Method breakdown
    cur.execute("SELECT p.method AS Method, COUNT(*) AS Count, COALESCE(SUM(p.amount),0) AS `Total (KES)` FROM payments p WHERE p.school_id=%s AND p.method <> 'Credit Transfer' GROUP BY p.method ORDER BY 3 DESC", (school_id,))
    method_breakdown = cur.fetchall() or []

    db.close()

    import zipfile
    mem = BytesIO()
    with zipfile.ZipFile(mem, 'w', zipfile.ZIP_DEFLATED) as z:
        def to_csv_bytes(rows):
            sio = StringIO();
            if rows:
                w = csv.DictWriter(sio, fieldnames=rows[0].keys()); w.writeheader(); w.writerows(rows)
            else:
                w = csv.writer(sio); w.writerow(['No data'])
            return sio.getvalue().encode('utf-8')
        z.writestr('students.csv', to_csv_bytes(students))
        z.writestr('payments.csv', to_csv_bytes(payments))
        z.writestr('class_summary.csv', to_csv_bytes(class_summary))
        z.writestr('term_summary.csv', to_csv_bytes(term_summary))
        z.writestr('method_breakdown.csv', to_csv_bytes(method_breakdown))
    mem.seek(0)
    return mem.getvalue()


def schedule_reports(scheduler, app):
    def _send_report():
        with app.app_context():
            try:
                if not is_pro_enabled(app):
                    return
                school_email = get_setting('SCHOOL_EMAIL') or app.config.get('SCHOOL_EMAIL')
                if not school_email:
                    return
                data = _build_full_report_bytes(app)
                if not data:
                    return
                msg = Message(subject='Weekly Fee Report', sender=app.config.get('MAIL_SENDER'), recipients=[school_email])
                msg.body = 'Attached is your weekly full fee report.'
                from flask_mail import Attachment
                ts = datetime.now().strftime('%Y%m%d_%H%M%S')
                msg.attach(filename=f'fees_full_report_{ts}.zip', content_type='application/zip', data=data)
                from extensions import mail
                mail.send(msg)
            except Exception as e:
                print('[scheduler] report email failed:', e)

    # Weekly on  Monday 08:00 server time
    try:
        scheduler.add_job(_send_report, 'interval', days=7, id='weekly_fee_report', replace_existing=True)
    except Exception:
        pass

    # Monthly (approx every 30 days)
    try:
        scheduler.add_job(_send_report, 'interval', days=30, id='monthly_fee_report', replace_existing=True)
    except Exception:
        pass
