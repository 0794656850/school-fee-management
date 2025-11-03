from flask import Blueprint, render_template, request, redirect, url_for, session, flash, current_app
from utils.settings import get_setting, set_school_setting
from utils.security import verify_password, hash_password, is_hashed
from utils.tenant import slugify_code, get_or_create_school, bootstrap_new_school
from utils.audit import ensure_audit_table, log_event
from utils.users import (
    ensure_user_tables,
    create_user,
    ensure_school_user,
    get_user_by_username,
    get_user_school_role,
)
from extensions import limiter

auth_bp = Blueprint('auth', __name__, url_prefix='/auth')


@auth_bp.route('/register', methods=['GET'])
def register():
    """Render modern registration page for creating a new school profile."""
    return render_template('register.html')


@auth_bp.route('/login', methods=['GET', 'POST'])
@limiter.limit('5 per minute')
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
                        # Audit
                        try:
                            ensure_audit_table(db)
                            log_event(db, int(session.get('school_id')), user['username'], 'login', 'user', int(user['id']), None)
                        except Exception:
                            pass
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
                    set_school_setting('APP_LOGIN_PASSWORD', hash_password(password))
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
        # Audit
        try:
            ensure_audit_table(db)
            log_event(db, sid, username, 'register_school', 'school', sid, {'category': category})
        except Exception:
            pass
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
    session.pop('user_logged_in', None)
    session.pop('username', None)
    flash('Signed out.', 'info')
    return redirect(url_for('auth.login'))

