from __future__ import annotations

from flask import Blueprint, render_template, request, redirect, url_for, flash, current_app, session, jsonify
from extensions import limiter
import os
import mysql.connector
from urllib.parse import urlparse
import uuid
from pathlib import Path
from datetime import datetime

from utils.tenant import slugify_code
from routes.student_portal import _sign_token, ensure_mpesa_student_table, _verify_token  # reuse same token scheme
from routes.term_routes import get_or_seed_current_term, ensure_academic_terms_table
from utils.security import verify_password, hash_password
from utils.document_qr import build_document_qr
from utils.db_helpers import ensure_guardian_receipts_table
from werkzeug.utils import secure_filename
import base64
import requests


guardian_bp = Blueprint("guardian", __name__, url_prefix="/g")


def _db():
    cfg = current_app.config
    host = os.environ.get("DB_HOST", "localhost")
    user = os.environ.get("DB_USER", "root")
    password = os.environ.get("DB_PASSWORD", "")
    database = os.environ.get("DB_NAME", "school_fee_db")

    uri = cfg.get("SQLALCHEMY_DATABASE_URI", "")
    if uri and uri.startswith("mysql"):
        try:
            parsed = urlparse(uri)
            host = parsed.hostname or host
            user = parsed.username or user
            password = parsed.password or password
            if parsed.path and len(parsed.path) > 1:
                database = parsed.path.lstrip("/")
        except Exception:
            pass
    return mysql.connector.connect(host=host, user=user, password=password, database=database)


def _guardian_upload_path(school_id: int) -> Path:
    relative = current_app.config.get("GUARDIAN_RECEIPT_UPLOADS_DIR", "uploads/guardian_receipts")
    root = Path(current_app.root_path) / "static" / relative
    target = root / str(school_id)
    target.mkdir(parents=True, exist_ok=True)
    return target


def _allowed_receipt_file(filename: str) -> bool:
    allowed = {".png", ".jpg", ".jpeg", ".pdf"}
    ext = Path(filename).suffix.lower()
    return ext in allowed


@guardian_bp.route("", strict_slashes=False)
@guardian_bp.route("/", strict_slashes=False)
def guardian_index():
    """Guardian landing page -> render login directly."""
    return render_template("guardian_login.html")


@guardian_bp.route("/login", methods=["GET", "POST"])
@limiter.limit("8 per minute", methods=["POST"])  # throttle brute-force
def guardian_login():
    """Secure guardian/parent login by school, last name and admission number."""
    if request.method == "POST":
        school_raw = (request.form.get("school") or "").strip()
        candidate_pw = (request.form.get("admission_no") or request.form.get("regNo") or "").strip()
        provided_name = (request.form.get("student_last_name") or request.form.get("last_name") or request.form.get("student_name") or request.form.get("student_full_name") or request.form.get("name") or "").strip()
        # Accept either last name or full name; derive last token for matching
        last_name = provided_name.split()[-1] if provided_name else ""

        if not school_raw or not candidate_pw or not last_name:
            flash("Enter school name/code, student's last name and admission number.", "warning")
            return redirect(url_for("guardian.guardian_login"))

        code = slugify_code(school_raw)
        db = _db(); cur = db.cursor(dictionary=True)
        try:
            # Resolve school by code OR exact name (case-insensitive)
            cur.execute(
                "SELECT id FROM schools WHERE code=%s OR LOWER(TRIM(name)) = LOWER(TRIM(%s)) LIMIT 1",
                (code, school_raw,)
            )
            row = cur.fetchone()
            if not row:
                flash("School not found. Please confirm the school name.", "error")
                return redirect(url_for("guardian.guardian_login"))
            school_id = int(row.get("id") if isinstance(row, dict) else row[0])

            # Ensure password column exists
            try:
                from routes.student_auth import ensure_student_portal_columns  # lazy import
                ensure_student_portal_columns(db)
            except Exception:
                pass
            cur.execute(
                """
                SELECT id, name, admission_no AS regNo, portal_password_hash
                FROM students
                WHERE school_id=%s AND LOWER(TRIM(SUBSTRING_INDEX(name, ' ', -1))) = LOWER(TRIM(%s))
                ORDER BY id ASC
                """,
                (school_id, last_name,),
            )
            candidates = cur.fetchall() or []
            if not candidates:
                flash("Invalid details. Please confirm your school name and admission number.", "error")
                return redirect(url_for("guardian.guardian_login"))

            student_row = None
            for s in candidates:
                stored = s.get("portal_password_hash")
                ok = False
                if stored:
                    ok = verify_password(stored, candidate_pw)
                else:
                    ok = (str(s.get("regNo") or "").strip() == candidate_pw)
                    if ok:
                        try:
                            cur2 = db.cursor()
                            cur2.execute(
                                "UPDATE students SET portal_password_hash=%s WHERE id=%s",
                                (hash_password(candidate_pw), int(s.get("id"))),
                            )
                            db.commit()
                        except Exception:
                            try: db.rollback()
                            except Exception: pass
                if ok:
                    student_row = s
                    break

            if not student_row:
                flash("Invalid details. Please confirm your school name and admission number.", "error")
                return redirect(url_for("guardian.guardian_login"))

            sid = int(student_row.get("id"))
            token = _sign_token(sid)
            session["guardian_logged_in"] = True
            session["guardian_student_id"] = sid
            session["guardian_token"] = token
            session["school_id"] = school_id
            return redirect(url_for("guardian.guardian_dashboard"))
        finally:
            try: db.close()
            except Exception: pass

    return render_template("guardian_login.html")


@guardian_bp.route("/dashboard", methods=["GET"])
def guardian_dashboard():
    """Guardian dashboard with student, fees and payments overview."""
    token = (request.args.get("token") or session.get("guardian_token") or "").strip()
    if not token:
        return redirect(url_for("guardian.guardian_login"))

    from routes.student_portal import _verify_token  # avoid cycle at import time
    student_id = _verify_token(token)
    if not student_id:
        flash("Session expired. Please login again.", "warning")
        return redirect(url_for("guardian.guardian_login"))

    db = _db(); cur = db.cursor(dictionary=True)
    cur.execute("SELECT * FROM students WHERE id=%s", (student_id,))
    student = cur.fetchone() or {}
    # Add admission number alias for templates expecting 'regNo'
    try:
        if student and ('regNo' not in student):
            _v = student.get('admission_no') or student.get('reg_no')
            if _v is not None:
                student['regNo'] = _v
    except Exception:
        pass

    ensure_academic_terms_table(db)
    year, term = get_or_seed_current_term(db)

    cur.execute(
        "SELECT id, amount, method, reference, date FROM payments WHERE student_id=%s ORDER BY date DESC, id DESC LIMIT 10",
        (student_id,)
    )
    payments = cur.fetchall() or []

    # Fee summary
    cur.execute("SELECT COALESCE(SUM(amount),0) AS total FROM payments WHERE student_id=%s AND year=%s AND term=%s", (student_id, year, term))
    row = cur.fetchone(); paid_term = float((row.get("total") if isinstance(row, dict) else (row[0] if row else 0)) or 0)
    expected = 0.0
    try:
        cur.execute("SELECT COALESCE(SUM(amount),0) FROM student_term_fee_items WHERE student_id=%s AND year=%s AND term=%s", (student_id, year, term))
        r1 = cur.fetchone(); expected = float((r1[0] if isinstance(r1, (list, tuple)) else (r1 or 0)) or 0)
        if expected <= 0:
            cur.execute("SELECT COALESCE(SUM(fee_amount),0) FROM term_fees WHERE student_id=%s AND year=%s AND term=%s", (student_id, year, term))
            r2 = cur.fetchone(); expected = float((r2[0] if isinstance(r2, (list, tuple)) else (r2 or 0)) or 0)
    except Exception:
        expected = 0.0
    try:
        bal = float(student.get("balance") or student.get("fee_balance") or 0)
    except Exception:
        bal = 0.0
    if expected <= 0 and (paid_term or bal):
        expected = paid_term + bal
    pct = int(round((paid_term / expected) * 100)) if expected > 0 else None

    try:
        from utils.settings import get_setting
        bursar_email = get_setting("SCHOOL_EMAIL") or get_setting("ACCOUNTS_EMAIL")
        bursar_phone = get_setting("SCHOOL_PHONE") or get_setting("ACCOUNTS_PHONE")
    except Exception:
        bursar_email = None; bursar_phone = None

    # Recent STK activity (pending/success/failed). Also map success rows to payment receipts.
    try:
        # Build a quick index by reference -> payment_id for linking receipts
        cur.execute(
            "SELECT id, reference FROM payments WHERE student_id=%s ORDER BY id DESC LIMIT 100",
            (student_id,),
        )
        _pindex = cur.fetchall() or []
        ref_to_pid = {}
        for r in _pindex:
            rid = r["id"] if isinstance(r, dict) else r[0]
            rref = r["reference"] if isinstance(r, dict) else r[1]
            if rref:
                ref_to_pid[str(rref)] = int(rid)

        cur.execute(
            """
            SELECT checkout_request_id, merchant_request_id, amount, phone, result_code, result_desc, mpesa_receipt, updated_at
            FROM mpesa_student_payments
            WHERE student_id=%s
            ORDER BY updated_at DESC, id DESC
            LIMIT 10
            """,
            (student_id,),
        )
        _stk_all = cur.fetchall() or []
        stk_activity = []
        for r in _stk_all:
            rc = r.get("result_code") if isinstance(r, dict) else None
            receipt = r.get("mpesa_receipt") if isinstance(r, dict) else None
            crid = r.get("checkout_request_id") if isinstance(r, dict) else None
            status = "pending" if (rc is None) else ("success" if str(rc) == "0" else "failed")
            # Derive reference for linking receipts
            ref = receipt or (f"MP_{crid}" if crid else None)
            pid = ref_to_pid.get(str(ref)) if ref else None
            stk_activity.append({
                "checkout_request_id": crid,
                "amount": r.get("amount") if isinstance(r, dict) else None,
                "phone": r.get("phone") if isinstance(r, dict) else None,
                "result_code": rc,
                "result_desc": r.get("result_desc") if isinstance(r, dict) else None,
                "updated_at": r.get("updated_at") if isinstance(r, dict) else None,
                "status": status,
                "payment_id": pid,
            })
    except Exception:
        stk_activity = []

    # Build multi-child list: match by parent email/phone when present; fallback to same last name
    siblings: list[dict] = []
    try:
        # Try parent email/phone columns
        key_email = None; key_phone = None
        try:
            cur2 = db.cursor()
            cur2.execute("SHOW COLUMNS FROM students LIKE 'parent_email'");
            has_parent_email = bool(cur2.fetchone())
            cur2.execute("SHOW COLUMNS FROM students LIKE 'parent_phone'");
            has_parent_phone = bool(cur2.fetchone())
        except Exception:
            has_parent_email = False; has_parent_phone = False
        if has_parent_email:
            key_email = (student.get('parent_email') or student.get('email') or '').strip()
        if has_parent_phone:
            key_phone = (student.get('parent_phone') or student.get('phone') or '').strip()
        if key_email or key_phone:
            q = ["SELECT id, name, admission_no AS regNo FROM students WHERE school_id=%s AND id<>%s"]
            ps = [int(student.get('school_id') or session.get('school_id') or 0), int(student_id)]
            if key_email:
                q.append("AND parent_email=%s")
                ps.append(key_email)
            if key_phone:
                q.append("AND parent_phone=%s")
                ps.append(key_phone)
            cur.execute(" ".join(q), tuple(ps))
            siblings = cur.fetchall() or []
        # Fallback to same last name within school
        if not siblings:
            last = str(student.get('name') or '').split()[-1] if (student and student.get('name')) else ''
            if last:
                cur.execute(
                    """
                    SELECT id, name, admission_no AS regNo FROM students
                    WHERE school_id=%s AND id<>%s AND LOWER(TRIM(SUBSTRING_INDEX(name,' ', -1))) = LOWER(TRIM(%s))
                    ORDER BY name
                    """,
                    (int(student.get('school_id') or session.get('school_id') or 0), int(student_id), last),
                )
                siblings = cur.fetchall() or []
    except Exception:
        siblings = []

    # Notices & announcements (premium): reuse newsletters as announcements
    announcements: list[dict] = []
    try:
        cur2 = db.cursor(dictionary=True)
        sid = int(student.get('school_id') or session.get('school_id') or 0)
        cur2.execute(
            """
            SELECT id, category, title, subject, html, created_at
            FROM newsletters
            WHERE (school_id=%s OR school_id IS NULL)
            ORDER BY id DESC
            LIMIT 8
            """,
            (sid,)
        )
        announcements = cur2.fetchall() or []
    except Exception:
        announcements = []

    db.close()
    return render_template(
        "guardian_dashboard.html",
        student=student,
        payments=payments,
        year=year,
        term=term,
        expected=expected,
        paid=paid_term,
        balance=bal if bal else max(expected - paid_term, 0.0),
        percent=pct,
        token=token,
        bursar_email=bursar_email,
        bursar_phone=bursar_phone,
        siblings=siblings,
        stk_activity=stk_activity,
        announcements=announcements,
        paypal_client_id=current_app.config.get("PAYPAL_CLIENT_ID") or "",
        paypal_currency=current_app.config.get("PAYPAL_CURRENCY") or "USD",
    )


@guardian_bp.route("/switch")
def guardian_switch():
    """Switch current child to another allowed student id, then redirect to dashboard.

    Allows switching among siblings (same parent email/phone) or same last name within same school.
    """
    try:
        sid = int(request.args.get("sid", "0"))
    except Exception:
        sid = 0
    token = (session.get("guardian_token") or request.args.get("token") or "").strip()
    if not sid or not token:
        return redirect(url_for("guardian.guardian_login"))
    from routes.student_portal import _verify_token
    cur_sid = _verify_token(token)
    if not cur_sid:
        return redirect(url_for("guardian.guardian_login"))

    db = _db(); cur = db.cursor(dictionary=True)
    # Fetch current + target
    cur.execute("SELECT id, name, school_id, parent_email, parent_phone, email, phone FROM students WHERE id=%s", (cur_sid,))
    a = cur.fetchone() or {}
    cur.execute("SELECT id, name, school_id, parent_email, parent_phone, email, phone FROM students WHERE id=%s", (sid,))
    b = cur.fetchone() or {}
    allowed = False
    try:
        if a and b and int(a.get('school_id') or 0) == int(b.get('school_id') or 0):
            last_a = str((a.get('name') or '').split()[-1]).strip().lower()
            last_b = str((b.get('name') or '').split()[-1]).strip().lower()
            if last_a and last_a == last_b:
                allowed = True
            ea = (a.get('parent_email') or a.get('email') or '').strip()
            eb = (b.get('parent_email') or b.get('email') or '').strip()
            pa = (a.get('parent_phone') or a.get('phone') or '').strip()
            pb = (b.get('parent_phone') or b.get('phone') or '').strip()
            if ea and eb and ea == eb:
                allowed = True
            if pa and pb and pa == pb:
                allowed = True
    except Exception:
        allowed = False
    if not allowed:
        db.close()
        flash("Not allowed to switch to this student.", "warning")
        return redirect(url_for("guardian.guardian_dashboard"))

    # Issue new token for the target student
    new_token = _sign_token(int(b.get('id')))
    session["guardian_token"] = new_token
    session["guardian_student_id"] = int(b.get('id'))
    session["school_id"] = int(b.get('school_id') or session.get('school_id') or 0)
    db.close()
    return redirect(url_for("guardian.guardian_dashboard", token=new_token))


@guardian_bp.route("/receipt/<int:payment_id>")
def guardian_receipt(payment_id: int):
    """Printable HTML receipt for a payment belonging to the logged-in student."""
    tok = (request.args.get("token") or session.get("guardian_token") or "").strip()
    if not tok:
        return redirect(url_for("guardian.guardian_login"))
    from routes.student_portal import _verify_token
    sid = _verify_token(tok)
    if not sid:
        return redirect(url_for("guardian.guardian_login"))

    db = _db(); cur = db.cursor(dictionary=True)
    cur.execute(
        """
        SELECT p.id, p.student_id AS sid, s.name AS student_name, s.class_name, p.amount, p.method,
               p.reference, p.date, p.term, p.year
        FROM payments p
        JOIN students s ON s.id = p.student_id
        WHERE p.id = %s AND p.student_id = %s
        """,
        (payment_id, sid),
    )
    payment = cur.fetchone()
    if not payment:
        db.close()
        flash("Payment not found for your account.", "error")
        return redirect(url_for("guardian.guardian_dashboard"))

    # Balance snapshot for display
    try:
        cur.execute("SELECT COALESCE(balance, fee_balance) AS balance, COALESCE(credit,0) AS credit FROM students WHERE id=%s", (sid,))
        srow = cur.fetchone() or {}
        bal = float(srow.get("balance") or 0)
        cred = float(srow.get("credit") or 0)
    except Exception:
        bal = None; cred = None
    db.close()

    brand = (current_app.config.get("APP_NAME") or f"{current_app.config.get('BRAND_NAME','Lovato_Tech')} {current_app.config.get('PORTAL_TITLE','Fee Management portal')}").strip()

    # Render a minimal, printable page (reusing receipt.html but without PDF link logic)
    try:
        verify_url = None
    except Exception:
        verify_url = None
    date_str = ""
    try:
        pdate = payment.get("date")
        if hasattr(pdate, "strftime"):
            date_str = pdate.strftime("%Y-%m-%d %H:%M")
        else:
            date_str = str(pdate or "")
    except Exception:
        date_str = ""
    auth_qr_data = build_document_qr(
        "guardian_receipt",
        {
            "rid": int(payment.get("id")),
            "sid": int(payment.get("sid")),
            "amt": round(float(payment.get("amount") or 0.0), 2),
            "cur": "KES",
            "name": payment.get("student_name") or "",
            "cls": payment.get("class_name") or "",
            "dt": date_str,
            "m": payment.get("method") or "",
            "ref": payment.get("reference") or "",
            "term": payment.get("term") or "",
            "year": payment.get("year") or "",
            "school_id": session.get("school_id"),
        },
    )
    return render_template(
        "guardian_receipt.html",
        brand=brand,
        payment=payment,
        current_balance=bal,
        current_credit=cred,
        payment_link=None,
        verify_url=verify_url,
        auth_qr_data=auth_qr_data,
    )


@guardian_bp.route("/upload-receipt", methods=["GET", "POST"])
def guardian_receipt_upload():
    token = (request.args.get("token") or session.get("guardian_token") or "").strip()
    if not token:
        return redirect(url_for("guardian.guardian_login"))
    student_id = _verify_token(token)
    if not student_id:
        return redirect(url_for("guardian.guardian_login"))

    if request.method == "POST":
        file = request.files.get("receipt")
        if not file or not file.filename:
            flash("Select a file to upload.", "warning")
            return redirect(url_for("guardian.guardian_receipt_upload"))
        if not _allowed_receipt_file(file.filename):
            flash("Unsupported file type. Use PNG/JPG/PDF.", "error")
            return redirect(url_for("guardian.guardian_receipt_upload"))
        school_id = session.get("school_id") or 0
        dest_dir = _guardian_upload_path(int(school_id))
        filename = f"{uuid.uuid4().hex}_{secure_filename(file.filename)}"
        target_path = dest_dir / filename
        db_conn = None
        try:
            file.save(target_path)
            db_conn = _db()
            ensure_guardian_receipts_table(db_conn)
            now = datetime.utcnow()
            rel = os.path.join(str(current_app.config.get("GUARDIAN_RECEIPT_UPLOADS_DIR", "uploads/guardian_receipts")), str(school_id), filename).replace("\\", "/")
            cur = db_conn.cursor()
            cur.execute(
                """
                INSERT INTO guardian_receipts
                    (school_id, student_id, guardian_name, guardian_email, guardian_phone, description, file_path, created_at, updated_at)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
                """,
                (
                    int(school_id),
                    student_id,
                    (request.form.get("guardian_name") or "").strip(),
                    (request.form.get("guardian_email") or "").strip(),
                    (request.form.get("guardian_phone") or "").strip(),
                    (request.form.get("description") or "").strip(),
                    rel,
                    now,
                    now,
                ),
            )
            db_conn.commit()
            flash("Receipt uploaded and pending verification.", "success")
            return redirect(url_for("guardian.guardian_receipt_upload"))
        except Exception:
            flash("Upload failed; please try again.", "error")
        finally:
            if db_conn:
                try:
                    db_conn.close()
                except Exception:
                    pass
    return render_template("guardian_receipt_upload.html")


@guardian_bp.route("/make_payment", methods=["POST"])
@limiter.limit("3 per minute")
def guardian_make_payment():
    """Initiate M-Pesa STK push for the guardian context."""
    from utils.mpesa import stk_push, DarajaError
    token = (request.json.get("token") if request.is_json else (request.form.get("token") or None)) or session.get("guardian_token")
    if not token:
        return jsonify({"ok": False, "error": "Not logged in"}), 401

    from routes.student_portal import _verify_token
    student_id = _verify_token(token)
    if not student_id:
        return jsonify({"ok": False, "error": "Invalid or expired session"}), 403

    phone = (request.json.get("phone") if request.is_json else request.form.get("phone") or "").strip()
    try:
        amount = int((request.json.get("amount") if request.is_json else request.form.get("amount") or 0))
    except Exception:
        amount = 0
    if not phone or amount <= 0:
        return jsonify({"ok": False, "error": "Phone and amount are required"}), 400

    db = _db(); ensure_academic_terms_table(db); ensure_mpesa_student_table(db)
    cur = db.cursor(dictionary=True)
    cur.execute("SELECT school_id, name FROM students WHERE id=%s", (student_id,))
    srow = cur.fetchone() or {}
    school_id = srow.get("school_id")
    y, t = get_or_seed_current_term(db)

    account_ref = (srow.get("name") or f"STUDENT-{student_id}")[:20]
    trans_desc = f"Fees payment T{t}/{y}"
    try:
        res = stk_push(phone=phone, amount=amount, account_ref=account_ref, trans_desc=trans_desc)
    except DarajaError as e:
        db.close()
        return jsonify({"ok": False, "error": str(e)}), 400

    from datetime import datetime as _dt
    now = _dt.now()
    cur2 = db.cursor()
    cur2.execute(
        """
        INSERT INTO mpesa_student_payments
            (student_id, school_id, year, term, merchant_request_id, checkout_request_id, amount, phone, created_at, updated_at)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        """,
        (
            student_id,
            school_id,
            y,
            t,
            res.get("MerchantRequestID"),
            res.get("CheckoutRequestID"),
            amount,
            phone,
            now,
            now,
        ),
    )
    db.commit(); db.close()
    return jsonify({
        "ok": True,
        "message": "STK push sent. Check your phone to authorize.",
        "checkout_request_id": res.get("CheckoutRequestID"),
    })


@guardian_bp.route("/status", methods=["GET"])
def guardian_payment_status():
    """Check status of an M-Pesa checkout for the logged-in guardian.

    Query string: crid=<CheckoutRequestID>
    Returns {ok:bool, status:str, result_code:int|None, receipt:str|None}
    """
    token = (request.args.get("token") or session.get("guardian_token") or "").strip()
    crid = (request.args.get("crid") or "").strip()
    if not token or not crid:
        return jsonify({"ok": False, "error": "Missing token or crid"}), 400
    from routes.student_portal import _verify_token
    sid = _verify_token(token)
    if not sid:
        return jsonify({"ok": False, "error": "Invalid session"}), 401

    db = _db(); cur = db.cursor(dictionary=True)
    cur.execute(
        "SELECT result_code, result_desc, mpesa_receipt, amount, phone, updated_at FROM mpesa_student_payments WHERE checkout_request_id=%s AND student_id=%s",
        (crid, sid),
    )
    row = cur.fetchone()
    db.close()
    if not row:
        return jsonify({"ok": True, "status": "pending", "result_code": None})
    rc = row.get("result_code")
    if rc is None:
        return jsonify({"ok": True, "status": "pending", "result_code": None})
    if str(rc) == "0":
        return jsonify({"ok": True, "status": "success", "result_code": 0, "receipt": row.get("mpesa_receipt")})
    return jsonify({"ok": True, "status": "failed", "result_code": int(rc), "message": row.get("result_desc")})


# --------- PayPal helper functions ---------
def _paypal_base_url() -> str:
    env = (current_app.config.get("PAYPAL_ENV") or "sandbox").strip().lower()
    return "https://api-m.paypal.com" if env == "live" else "https://api-m.sandbox.paypal.com"


def _paypal_access_token() -> str | None:
    cid = (current_app.config.get("PAYPAL_CLIENT_ID") or "").strip()
    sec = (current_app.config.get("PAYPAL_SECRET") or "").strip()
    if not cid or not sec:
        return None
    url = _paypal_base_url() + "/v1/oauth2/token"
    auth = base64.b64encode(f"{cid}:{sec}".encode("utf-8")).decode("ascii")
    try:
        res = requests.post(url, headers={"Authorization": f"Basic {auth}"}, data={"grant_type": "client_credentials"}, timeout=15)
        if res.ok:
            return res.json().get("access_token")
    except Exception:
        return None
    return None


@guardian_bp.route("/paypal/create-order", methods=["POST"])  # POST /g/paypal/create-order
@limiter.limit("6 per minute")
def guardian_paypal_create_order():
    token = (request.json.get("token") if request.is_json else request.form.get("token")) or session.get("guardian_token")
    if not token:
        return jsonify({"ok": False, "error": "Not logged in"}), 401
    try:
        amount = str(request.json.get("amount") if request.is_json else request.form.get("amount"))
        if not amount or float(amount) <= 0:
            return jsonify({"ok": False, "error": "Invalid amount"}), 400
    except Exception:
        return jsonify({"ok": False, "error": "Invalid amount"}), 400
    currency = (current_app.config.get("PAYPAL_CURRENCY") or "USD").strip().upper()
    at = _paypal_access_token()
    if not at:
        return jsonify({"ok": False, "error": "PayPal not configured"}), 400
    url = _paypal_base_url() + "/v2/checkout/orders"
    body = {
        "intent": "CAPTURE",
        "purchase_units": [
            {
                "amount": {"currency_code": currency, "value": str(amount)},
                "description": "School fees payment",
            }
        ],
        "application_context": {"shipping_preference": "NO_SHIPPING"},
    }
    try:
        res = requests.post(url, json=body, headers={"Authorization": f"Bearer {at}", "Content-Type": "application/json"}, timeout=20)
        if not res.ok:
            return jsonify({"ok": False, "error": res.text}), 400
        data = res.json()
        return jsonify({"ok": True, "id": data.get("id")})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500


@guardian_bp.route("/paypal/capture", methods=["POST"])  # POST /g/paypal/capture
@limiter.limit("8 per minute")
def guardian_paypal_capture():
    token = (request.json.get("token") if request.is_json else request.form.get("token")) or session.get("guardian_token")
    order_id = (request.json.get("order_id") if request.is_json else request.form.get("order_id"))
    if not token or not order_id:
        return jsonify({"ok": False, "error": "Missing token or order id"}), 400
    from routes.student_portal import _verify_token
    student_id = _verify_token(token)
    if not student_id:
        return jsonify({"ok": False, "error": "Invalid session"}), 403
    at = _paypal_access_token()
    if not at:
        return jsonify({"ok": False, "error": "PayPal not configured"}), 400
    # Capture
    url = _paypal_base_url() + f"/v2/checkout/orders/{order_id}/capture"
    try:
        res = requests.post(url, headers={"Authorization": f"Bearer {at}", "Content-Type": "application/json"}, timeout=20)
        if not res.ok:
            return jsonify({"ok": False, "error": res.text}), 400
        cap = res.json()
        status = cap.get("status")
        if status != "COMPLETED":
            return jsonify({"ok": False, "error": f"Status {status}"}), 400
        # Determine amount captured
        try:
            pu = (cap.get("purchase_units") or [{}])[0]
            capr = ((pu.get("payments") or {}).get("captures") or [{}])[0]
            amount_val = float(((capr.get("amount") or {}).get("value")) or 0)
            reference = capr.get("id") or order_id
        except Exception:
            amount_val = 0.0
            reference = order_id
        if amount_val <= 0:
            return jsonify({"ok": False, "error": "Zero amount"}), 400
        # Record payment and update student balance/credit
        db = _db(); cur = db.cursor(dictionary=True)
        cur.execute("SELECT school_id, COALESCE(balance, fee_balance) AS balance, COALESCE(credit,0) AS credit FROM students WHERE id=%s", (student_id,))
        srow = cur.fetchone() or {}
        school_id = int(srow.get("school_id") or 0)
        bal_before = float(srow.get("balance") or 0)
        cred_before = float(srow.get("credit") or 0)
        y, t = get_or_seed_current_term(db)
        # Insert payment
        cur2 = db.cursor()
        cur2.execute(
            "INSERT INTO payments (student_id, amount, method, term, year, reference, date, school_id) VALUES (%s,%s,%s,%s,%s,%s,NOW(),%s)",
            (student_id, amount_val, "PayPal", t, y, reference, school_id),
        )
        # Update balance and credit (cap at expected per student context is not known here; use direct balance reduction)
        overpay = max(amount_val - bal_before, 0.0)
        new_bal = max(bal_before - amount_val, 0.0)
        new_credit = cred_before + overpay
        try:
            cur2.execute("UPDATE students SET balance=%s, credit=%s WHERE id=%s", (new_bal, new_credit, student_id))
        except Exception:
            # fallback legacy column name
            cur2.execute("UPDATE students SET fee_balance=%s, credit=%s WHERE id=%s", (new_bal, new_credit, student_id))
        db.commit(); db.close()
        return jsonify({"ok": True, "status": "COMPLETED", "payment_reference": reference})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500




@guardian_bp.route("/logout")
def guardian_logout():
    """Clear guardian session and return to login."""
    try:
        session.pop("guardian_logged_in", None)
        session.pop("guardian_student_id", None)
        session.pop("guardian_token", None)
    except Exception:
        pass
    return redirect(url_for("guardian.guardian_login"))


@guardian_bp.route("/analytics", methods=["GET"])
def guardian_analytics():
    """Return monthly payment analytics for a guardian-linked student."""
    token = (request.args.get("token") or session.get("guardian_token") or "").strip()
    if not token:
        return jsonify({"ok": False, "error": "Missing token"}), 400
    from routes.student_portal import _verify_token
    sid = _verify_token(token)
    if not sid:
        return jsonify({"ok": False, "error": "Invalid token"}), 403

    from datetime import datetime as _dt
    now = _dt.now(); year_now = now.year; year_prev = year_now - 1
    labels = ["Jan","Feb","Mar","Apr","May","Jun","Jul","Aug","Sep","Oct","Nov","Dec"]
    cur_year = [0.0]*12; prev_year = [0.0]*12
    db = _db(); cur = db.cursor()
    try:
        # Current year
        cur.execute("SELECT MONTH(date) AS m, COALESCE(SUM(amount),0) FROM payments WHERE student_id=%s AND YEAR(date)=%s GROUP BY MONTH(date)", (sid, year_now))
        for m, s in cur.fetchall() or []:
            if m and 1 <= int(m) <= 12:
                cur_year[int(m)-1] = float(s or 0)
        # Previous year
        cur.execute("SELECT MONTH(date) AS m, COALESCE(SUM(amount),0) FROM payments WHERE student_id=%s AND YEAR(date)=%s GROUP BY MONTH(date)", (sid, year_prev))
        for m, s in cur.fetchall() or []:
            if m and 1 <= int(m) <= 12:
                prev_year[int(m)-1] = float(s or 0)
    except Exception:
        pass

    # Term summary
    y, t = get_or_seed_current_term(db)
    expected = paid = bal = pct = 0
    try:
        cur2 = db.cursor()
        cur2.execute("SELECT COALESCE(SUM(amount),0) FROM student_term_fee_items WHERE student_id=%s AND year=%s AND term=%s", (sid, y, t))
        r = cur2.fetchone(); expected = float((r[0] if isinstance(r,(list,tuple)) else r) or 0)
    except Exception:
        expected = 0
    try:
        cur2 = db.cursor()
        cur2.execute("SELECT COALESCE(SUM(amount),0) FROM payments WHERE student_id=%s AND year=%s AND term=%s", (sid, y, t))
        r = cur2.fetchone(); paid = float((r[0] if isinstance(r,(list,tuple)) else r) or 0)
    except Exception:
        paid = 0
    try:
        cur2 = db.cursor()
        cur2.execute("SELECT COALESCE(balance, fee_balance) FROM students WHERE id=%s", (sid,))
        r = cur2.fetchone(); bal = float((r[0] if isinstance(r,(list,tuple)) else r) or 0)
    except Exception:
        bal = 0
    if expected <= 0:
        expected = paid + bal
    pct = int(round((paid/expected)*100)) if expected > 0 else 0
    db.close()
    return jsonify({
        "ok": True,
        "labels": labels,
        "current": cur_year,
        "previous": prev_year,
        "term": {"expected": expected, "paid": paid, "balance": bal, "percent": pct, "year": y, "t": t},
    })

def ensure_events_table(db) -> None:
    cur = db.cursor()
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS calendar_events (
            id INT AUTO_INCREMENT PRIMARY KEY,
            school_id INT NULL,
            title VARCHAR(200) NOT NULL,
            description TEXT NULL,
            category VARCHAR(40) NULL,
            start_date DATE NOT NULL,
            end_date DATE NULL,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            INDEX idx_events_school (school_id),
            INDEX idx_events_dates (start_date)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
        """
    )
    db.commit()

@guardian_bp.route("/events", methods=["GET"])
def guardian_events():
    """List calendar events for the student's school for a specific month.

    Query: token, y, m
    """
    token = (request.args.get("token") or session.get("guardian_token") or "").strip()
    if not token:
        return jsonify({"ok": False, "error": "Missing token"}), 400
    from routes.student_portal import _verify_token
    sid = _verify_token(token)
    if not sid:
        return jsonify({"ok": False, "error": "Invalid token"}), 403
    try:
        y = int(request.args.get("y") or 0)
        m = int(request.args.get("m") or 0)
    except Exception:
        y = 0; m = 0
    from datetime import date
    today = date.today()
    if not y: y = today.year
    if not m or m < 1 or m > 12: m = today.month

    db = _db(); ensure_events_table(db)
    cur = db.cursor(dictionary=True)
    # Resolve school id
    cur.execute("SELECT school_id FROM students WHERE id=%s", (sid,))
    srow = cur.fetchone() or {}
    school_id = srow.get('school_id')
    # Build month range
    from calendar import monthrange
    last_day = monthrange(y, m)[1]
    start = f"{y:04d}-{m:02d}-01"; end = f"{y:04d}-{m:02d}-{last_day:02d}"
    cur.execute(
        """
        SELECT id, title, category, description, start_date, end_date
        FROM calendar_events
        WHERE (school_id=%s OR school_id IS NULL)
          AND start_date <= %s AND (end_date IS NULL OR end_date >= %s)
        ORDER BY start_date ASC, id ASC
        """,
        (school_id, end, start)
    )
    rows = cur.fetchall() or []
    db.close()
    return jsonify({"ok": True, "items": rows, "y": y, "m": m})


@guardian_bp.route("/ai_assistant", methods=["POST"])
def guardian_ai_assistant():
    """Guardian AI: if a provider is configured, route to it with context; otherwise fallback to built-ins."""
    data = request.get_json(silent=True) or {}
    token = (data.get("token") or session.get("guardian_token") or "").strip()
    q = (data.get("question") or "").strip().lower()
    if not token or not q:
        return jsonify({"ok": False, "error": "Missing token or question"}), 400
    from routes.student_portal import _verify_token
    sid = _verify_token(token)
    if not sid:
        return jsonify({"ok": False, "error": "Invalid token"}), 403

    # If AI provider is configured, use it
    try:
        from utils.ai import ai_is_configured
        from ai_engine.query import handle_query as _handle_query
        if ai_is_configured():
            # Build a contextualized prompt
            db = _db(); cur = db.cursor(dictionary=True)
            cur.execute("SELECT name, admission_no AS regNo FROM students WHERE id=%s", (sid,))
            s = cur.fetchone() or {}
            context = f"[Guardian Portal | Student: {s.get('name','')} ({s.get('regNo','')})] "
            payload = {"question": context + (data.get("question") or ""), "school_id": session.get("school_id")}
            try:
                res = _handle_query(payload) or {}
            finally:
                db.close()
            # Normalize answer field
            ans = res.get("answer") or res.get("text") or res.get("response") or "I could not find an answer right now."
            return jsonify({"ok": True, "answer": ans, **{k:v for k,v in res.items() if k not in {"answer"}}})
    except Exception:
        pass

    db = _db(); cur = db.cursor(dictionary=True)
    # Basic balance intent
    if any(k in q for k in ["balance", "fee balance", "how much do i owe", "outstanding"]):
        bal = 0.0
        paid = 0.0
        expected = 0.0
        y, t = get_or_seed_current_term(db)
        try:
            cur.execute("SELECT COALESCE(balance, fee_balance) AS bal FROM students WHERE id=%s", (sid,))
            r = cur.fetchone() or {}
            bal = float(r.get('bal') or 0.0)
        except Exception:
            bal = 0.0
        try:
            cur.execute("SELECT COALESCE(SUM(amount),0) AS total FROM payments WHERE student_id=%s AND year=%s AND term=%s", (sid, y, t))
            p = cur.fetchone() or {}
            paid = float(p.get('total') or 0.0)
        except Exception:
            paid = 0.0
        try:
            cur.execute("SELECT COALESCE(SUM(amount),0) AS tot FROM student_term_fee_items WHERE student_id=%s AND year=%s AND term=%s", (sid, y, t))
            e = cur.fetchone() or {}
            expected = float(e.get('tot') or 0.0)
        except Exception:
            expected = 0.0
        if expected <= 0 and bal:
            expected = bal + paid
        pct = int(round((paid / expected) * 100)) if expected > 0 else 0
        db.close()
        return jsonify({
            "ok": True,
            "answer": f"Your child's current balance is KES {bal:,.0f}. Paid this term: KES {paid:,.0f}. Completion: {pct}%.",
        })

    # Next exam intent -> scan newsletters/announcements for keywords
    if any(k in q for k in ["exam", "test", "assessment"]):
        try:
            # Look for latest newsletter/memo mentioning exam
            cur.execute(
                """
                SELECT title, subject, created_at
                FROM newsletters
                WHERE (title LIKE %s OR subject LIKE %s)
                ORDER BY id DESC LIMIT 1
                """,
                ("%exam%", "%exam%"),
            )
            r = cur.fetchone()
            if r:
                db.close()
                return jsonify({"ok": True, "answer": f"Latest exam notice: {r.get('title') or r.get('subject')}. Please check Notices for details."})
        except Exception:
            pass
        db.close()
        return jsonify({"ok": True, "answer": "No upcoming exam was found in the notices. Please check with the class teacher."})

    # Receipt help intent
    if any(k in q for k in ["receipt", "proof", "download receipt"]):
        db.close()
        return jsonify({"ok": True, "answer": "Open Recent Payments and click Print to get a downloadable receipt for any payment."})

    db.close()
    return jsonify({
        "ok": True,
        "answer": "I can help with: fee balance, next exam date, and how to get receipts. Try asking: 'What is my child's fee balance?'.",
    })


@guardian_bp.route("/notifications", methods=["GET"])
def guardian_notifications():
    """Return latest announcements/notices for the student's school."""
    token = (request.args.get("token") or session.get("guardian_token") or "").strip()
    if not token:
        return jsonify({"ok": False, "error": "Missing token"}), 400
    from routes.student_portal import _verify_token
    sid = _verify_token(token)
    if not sid:
        return jsonify({"ok": False, "error": "Invalid token"}), 403
    db = _db(); cur = db.cursor(dictionary=True)
    cur.execute("SELECT school_id FROM students WHERE id=%s", (sid,))
    s = cur.fetchone() or {}
    sid_school = s.get('school_id')
    rows: list[dict] = []
    try:
        cur.execute(
            "SELECT id, category, title, subject, created_at FROM newsletters WHERE (school_id=%s OR school_id IS NULL) ORDER BY id DESC LIMIT 12",
            (sid_school,),
        )
        rows = cur.fetchall() or []
    except Exception:
        rows = []
    db.close()
    return jsonify({"ok": True, "items": rows})

