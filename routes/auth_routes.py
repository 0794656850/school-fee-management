from flask import Blueprint, render_template, request, redirect, url_for, session, flash, current_app
from utils.settings import get_setting, set_school_setting
from utils.security import verify_password, hash_password, is_hashed
from utils.tenant import slugify_code, get_or_create_school, bootstrap_new_school
# Audit removed
from utils.users import (
    ensure_user_tables,
    create_user,
    ensure_school_user,
    get_user_by_username,
    get_user_school_role,
)
from extensions import limiter

auth_bp = Blueprint('auth', __name__, url_prefix='/auth')


@auth_bp.route('/', methods=['GET'])
def entry():
    """Entry screen with School vs Parent login options."""
    return render_template('entry.html')


@auth_bp.route('/register', methods=['GET'])
def register():
    """Render modern registration page for creating a new school profile."""
    return render_template('register.html')


@auth_bp.route('/login', methods=['GET', 'POST'])
# Rate limit login POSTs only to avoid blocking navigation in FREE plan
@limiter.limit('10 per minute', methods=['POST'])
def login():
    """Login now accepts School Name/Code and auto-creates schools if missing.

    - On POST, we resolve or create the school before verifying credentials.
    - New schools are bootstrapped with default credentials: user / 9133.
    - After first successful login for a newly created school, redirect to
      Access Settings to change credentials.
    """
    if request.method == 'POST':
        # Resolve school from input (create if not exists)
        raw_name = (request.form.get('school_name') or '').strip()
        raw_code = (request.form.get('school_code') or '').strip()
        code = slugify_code(raw_code or raw_name)
        if not code:
            flash('Enter your school name or code.', 'warning')
            return redirect(url_for('auth.login', next=request.form.get('next')))

        import mysql.connector
        created_school = False
        try:
            from app import get_db_connection  # type: ignore
            db = get_db_connection()
            sid = get_or_create_school(db, code=code, name=raw_name or code)
            # If we just created it, bootstrap with defaults (terms will be missing)
            cur = db.cursor()
            cur.execute("SELECT COUNT(*) FROM academic_terms WHERE school_id=%s", (sid,))
            count_terms = (cur.fetchone() or [0])[0]
            if int(count_terms or 0) == 0:
                created_school = True
                try:
                    bootstrap_new_school(db, sid, raw_name or code, code)
                except Exception:
                    pass
        finally:
            try:
                db.close()
            except Exception:
                pass

        # Bind school into session context for per-school auth
        session['school_id'] = sid
        session['school_code'] = code

        username = (request.form.get('username') or '').strip()
        password = (request.form.get('password') or '').strip()
        remember = True if request.form.get('remember') in ('on','1','true','yes') else False
        next_url = request.args.get('next') or request.form.get('next')

        # First try: user directory (multi-user) if present
        try:
            db = current_app.config.get('_raw_db_conn')  # not used; open fresh
        except Exception:
            db = None
        try:
            from app import get_db_connection  # type: ignore
            db = get_db_connection()
            ensure_user_tables(db)
            user = get_user_by_username(db, username)
            if user and int(user.get('is_active', 1)) == 1:
                stored_hash = user.get('password_hash') or ''
                if verify_password(stored_hash, password):
                    role = get_user_school_role(db, int(user['id']), int(session.get('school_id')))
                    if role:
                        session['user_logged_in'] = True
                        session['user_id'] = int(user['id'])
                        session['username'] = user['username']
                        session['role'] = role
                        try:
                            # Respect "Remember me" to persist session cookie
                            session.permanent = remember
                        except Exception:
                            pass
                        # Record school's first admin login timestamp if not already set
                        try:
                            cur = db.cursor()
                            cur.execute("SELECT first_login_at FROM schools WHERE id=%s", (session.get('school_id'),))
                            row = cur.fetchone()
                            first_login_at = None
                            if row is not None:
                                try:
                                    first_login_at = row[0] if not isinstance(row, dict) else row.get('first_login_at')
                                except Exception:
                                    first_login_at = None
                            if not first_login_at:
                                cur.execute("UPDATE schools SET first_login_at=NOW() WHERE id=%s AND first_login_at IS NULL", (session.get('school_id'),))
                                db.commit()
                        except Exception:
                            pass
                        # Audit removed: no login event logging
                        flash('Welcome back!', 'success')
                        db.close()
                        return redirect(next_url or url_for('dashboard'))
        except Exception:
            try:
                db and db.close()
            except Exception:
                pass

        # Fallback: simple per-school credential (legacy)
        cfg_user = (
            (get_setting('APP_LOGIN_USERNAME') or '').strip()
            or current_app.config.get('LOGIN_USERNAME', 'user')
        )
        cfg_pass_val = (get_setting('APP_LOGIN_PASSWORD') or '').strip()
        if not cfg_pass_val:
            cfg_pass_val = current_app.config.get('LOGIN_PASSWORD', '9133')

        # Verify password (supports hashed or plain in settings)
        valid = verify_password(cfg_pass_val, password)

        # Allow password-only login by treating missing username as configured one
        if ((not username) or username == cfg_user) and valid:
            session['user_logged_in'] = True
            session['username'] = (username or cfg_user)
            session['role'] = 'owner'
            try:
                session.permanent = remember
            except Exception:
                pass
            flash('Welcome back!', 'success')
            # Silent upgrade: if stored password is plain, replace with hash per-school
            try:
                if not is_hashed(get_setting('APP_LOGIN_PASSWORD')):
                    sid = session.get('school_id')
                    set_school_setting('APP_LOGIN_PASSWORD', hash_password(password), school_id=sid)
            except Exception:
                pass
            # Mark first admin login for this school if not set
            try:
                from app import get_db_connection  # type: ignore
                _db = get_db_connection()
                cur = _db.cursor()
                cur.execute("UPDATE schools SET first_login_at=NOW() WHERE id=%s AND first_login_at IS NULL", (session.get('school_id'),))
                _db.commit()
                _db.close()
            except Exception:
                pass
            return redirect(next_url or url_for('dashboard'))
        flash('Invalid credentials.', 'error')
        return redirect(url_for('auth.login', next=next_url))

    # GET
    next_url = request.args.get('next', '')
    return render_template('login.html', next_url=next_url)


@auth_bp.route('/forgot', methods=['POST'])
@limiter.limit('4 per minute')
def forgot_password():
    """Compatibility endpoint. Delegates to the simplified reset flow."""
    return forgot_password_simple()


@auth_bp.route('/forgot/simple', methods=['POST'])
@limiter.limit('4 per minute')
def forgot_password_simple():
    """Single-step password reset: generate a new temp password and email it
    to the school's registered email address. No OTP is required.

    Input: school_code or school_name
    Output: flashes success/error and redirects back to login
    """
    from utils.gmail_api import send_email_html as gmail_send_email_html  # type: ignore
    try:
        from utils.gmail_api import send_email as gmail_send_email  # type: ignore
    except Exception:
        def gmail_send_email(*args, **kwargs):  # type: ignore
            return False

    raw = (request.form.get('school_code') or request.form.get('school_name') or '').strip()
    code = slugify_code(raw)
    if not code:
        flash('Enter your school code.', 'warning')
        return redirect(url_for('auth.login'))

    # Open DB
    try:
        from app import get_db_connection  # type: ignore
        db = get_db_connection()
    except Exception:
        flash('Unable to access database.', 'error')
        return redirect(url_for('auth.login'))
    try:
        cur = db.cursor(dictionary=True)
        cur.execute("SELECT id, name FROM schools WHERE code=%s OR LOWER(TRIM(name))=LOWER(TRIM(%s)) LIMIT 1", (code, raw))
        row = cur.fetchone()
        if not row:
            flash('School not found. Check the code.', 'error')
            return redirect(url_for('auth.login'))
        school_id = int(row['id'])

        # Destination email
        cur2 = db.cursor()
        cur2.execute("SELECT `value` FROM school_settings WHERE school_id=%s AND `key` IN ('SCHOOL_EMAIL','ACCOUNTS_EMAIL') ORDER BY FIELD(`key`,'SCHOOL_EMAIL','ACCOUNTS_EMAIL') LIMIT 1", (school_id,))
        r = cur2.fetchone(); to_email = (r[0] if r else '') or ''
        if not to_email:
            flash('School email is not set. Ask support to update SCHOOL_EMAIL.', 'warning')
            return redirect(url_for('auth.login'))

        # Generate new temporary password and store hashed
        import secrets, string
        alphabet = string.ascii_letters + string.digits
        temp = ''.join(secrets.choice(alphabet) for _ in range(10))
        cur2.execute(
            "INSERT INTO school_settings(school_id, `key`, `value`) VALUES(%s,'APP_LOGIN_PASSWORD',%s) ON DUPLICATE KEY UPDATE `value`=VALUES(`value`)",
            (school_id, hash_password(temp))
        )
        db.commit()

        subject = 'Your New School Admin Password'
        body = f"""
        <p>Hello,</p>
        <p>We received a password reset request for school <strong>{row.get('name','')}</strong> (code <strong>{code}</strong>).</p>
        <p>Your new login password is:</p>
        <div style='font-size:22px;font-weight:700;letter-spacing:1px'>{temp}</div>
        <p>Use this on the admin login screen. After signing in, please change it in Access Settings.</p>
        <p>- SmartEduPay</p>
        """
        ok = False
        try:
            ok = gmail_send_email_html(to_email, subject, body)
        except Exception:
            ok = False
        if not ok:
            # Plain text fallback via Gmail API
            try:
                ok = gmail_send_email(to_email, subject, f"Your new admin password: {temp}")
            except Exception:
                ok = False
        if not ok:
            # SMTP fallback if configured (Flask-Mail)
            try:
                cfg = current_app.config
                server = (cfg.get('MAIL_SERVER') or '').strip()
                username = (cfg.get('MAIL_USERNAME') or '').strip()
                password = (cfg.get('MAIL_PASSWORD') or '').strip()
                if server and username and password:
                    from flask_mail import Message  # type: ignore
                    from extensions import mail  # type: ignore
                    sender = (
                        cfg.get('MAIL_SENDER')
                        or cfg.get('MAIL_DEFAULT_SENDER')
                        or get_setting('SCHOOL_EMAIL')
                        or cfg.get('MAIL_USERNAME')
                        or None
                    )
                    m = Message(subject=subject, sender=sender, recipients=[to_email], body=f"Your new admin password: {temp}")
                    mail.send(m)
                    ok = True
            except Exception:
                ok = False
        flash('A new password has been emailed to the school address.' if ok else 'Failed to send new password. Configure Gmail OAuth or SMTP and try again.', 'success' if ok else 'error')
        return redirect(url_for('auth.login'))
    finally:
        try:
            db.close()
        except Exception:
            pass
@auth_bp.route('/register_school', methods=['POST'])
def register_school():
    """Create a new school profile from the registration form on login page."""
    raw_name = (request.form.get('school_name') or '').strip()
    # Allow only small learning institutions: Kindergarten -> High School
    category = (request.form.get('school_category') or '').strip()
    allowed_categories = {"Kindergarten", "Primary", "Junior Secondary", "High School"}
    if category not in allowed_categories:
        category = None
    phone = (request.form.get('school_phone') or '').strip() or None
    address = (request.form.get('school_address') or '').strip() or None
    admin_name = (request.form.get('admin_name') or '').strip() or None
    admin_email = (request.form.get('admin_email') or '').strip() or None
    username = (request.form.get('username') or '').strip() or 'user'
    password = (request.form.get('password') or '').strip() or '9133'
    confirm = (request.form.get('confirm_password') or '').strip()
    if not raw_name:
        flash('School name is required.', 'warning')
        return redirect(url_for('auth.login'))
    if password != confirm:
        flash('Passwords do not match.', 'warning')
        return redirect(url_for('auth.login'))
    code = slugify_code(raw_name)
    try:
        from app import get_db_connection  # type: ignore
        db = get_db_connection()
        sid = get_or_create_school(db, code=code, name=raw_name)
        # Store profile details into settings
        try:
            set_school_setting('SCHOOL_NAME', raw_name, school_id=sid)
            if category:
                set_school_setting('SCHOOL_CATEGORY', category, school_id=sid)
            if phone:
                set_school_setting('SCHOOL_PHONE', phone, school_id=sid)
            if address:
                set_school_setting('SCHOOL_ADDRESS', address, school_id=sid)
            if admin_email:
                set_school_setting('SCHOOL_EMAIL', admin_email, school_id=sid)
        except Exception:
            pass
        # Create admin user and map to school
        ensure_user_tables(db)
        uid = create_user(db, username, admin_email, hash_password(password))
        ensure_school_user(db, uid, sid, role='owner')
        # Mark first login timestamp
        cur = db.cursor()
        cur.execute("UPDATE schools SET first_login_at=NOW() WHERE id=%s AND first_login_at IS NULL", (sid,))
        db.commit()
        # No audit events on register
        # Session
        session['school_id'] = sid
        session['school_code'] = code
        session['user_logged_in'] = True
        session['username'] = username
        session['role'] = 'owner'
        flash('School registered. Welcome!', 'success')
        return redirect(url_for('dashboard'))
    except Exception as e:
        flash(f'Error registering school: {e}', 'error')
        return redirect(url_for('auth.login'))


@auth_bp.route('/logout')
def logout():
    # No audit on logout
    session.pop('user_logged_in', None)
    session.pop('username', None)
    flash('Signed out.', 'info')
    return redirect(url_for('auth.login'))

