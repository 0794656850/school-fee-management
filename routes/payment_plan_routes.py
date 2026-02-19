from __future__ import annotations

from datetime import datetime, date
from typing import Optional

import mysql.connector
from flask import Blueprint, render_template, request, redirect, url_for, flash, session, current_app

from utils.db_helpers import ensure_payment_plan_tables
from utils.payment_plans import create_payment_plan
from utils.audit import log_event
from routes.term_routes import get_or_seed_current_term


plans_bp = Blueprint("plans", __name__, url_prefix="/plans")


def _db():
    cfg = current_app.config
    from urllib.parse import urlparse
    host = "localhost"; user = "root"; password = ""; database = "school_fee_db"
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
    import os
    host = os.environ.get("DB_HOST", host)
    user = os.environ.get("DB_USER", user)
    password = os.environ.get("DB_PASSWORD", password)
    database = os.environ.get("DB_NAME", database)
    return mysql.connector.connect(host=host, user=user, password=password, database=database)


def _parse_date(raw: Optional[str]) -> Optional[date]:
    if not raw:
        return None
    try:
        return datetime.strptime(raw.strip(), "%Y-%m-%d").date()
    except Exception:
        return None


@plans_bp.route("/", methods=["GET", "POST"])
def plans_home():
    db = _db()
    try:
        ensure_payment_plan_tables(db)
        cur = db.cursor(dictionary=True)
        if request.method == "POST":
            student_id = request.form.get("student_id", type=int)
            total_amount = request.form.get("total_amount", type=float)
            installments = request.form.get("installments", type=int) or 1
            frequency = request.form.get("frequency") or "monthly"
            start_date = _parse_date(request.form.get("start_date"))
            year = request.form.get("year", type=int)
            term = request.form.get("term", type=int)
            if not student_id or not total_amount:
                flash("Provide a student and total amount.", "warning")
            else:
                plan_id = create_payment_plan(
                    db,
                    int(session.get("school_id")),
                    student_id,
                    float(total_amount),
                    int(installments),
                    frequency,
                    start_date,
                    year,
                    term,
                    session.get("username"),
                )
                try:
                    log_event("payment_plan_create", target=f"plan:{plan_id}", detail=f"Student {student_id}, total {total_amount}")
                except Exception:
                    pass
                flash("Payment plan created.", "success")
                return redirect(url_for("plans.plan_detail", plan_id=plan_id))

        cur.execute(
            """
            SELECT p.*, s.name AS student_name, s.class_name
            FROM payment_plans p
            JOIN students s ON p.student_id=s.id
            WHERE p.school_id=%s
            ORDER BY p.id DESC
            LIMIT 50
            """,
            (session.get("school_id"),),
        )
        plans = cur.fetchall() or []
        cur.execute(
            "SELECT id, name, class_name FROM students WHERE school_id=%s ORDER BY name ASC",
            (session.get("school_id"),),
        )
        students = cur.fetchall() or []
        try:
            cy, ct = get_or_seed_current_term(db)
        except Exception:
            cy, ct = None, None
    finally:
        db.close()
    return render_template("payment_plans.html", plans=plans, students=students, current_year=cy, current_term=ct)


@plans_bp.route("/<int:plan_id>", methods=["GET"])
def plan_detail(plan_id: int):
    db = _db()
    try:
        ensure_payment_plan_tables(db)
        cur = db.cursor(dictionary=True)
        cur.execute(
            """
            SELECT p.*, s.name AS student_name, s.class_name
            FROM payment_plans p
            JOIN students s ON p.student_id=s.id
            WHERE p.id=%s AND p.school_id=%s
            """,
            (plan_id, session.get("school_id")),
        )
        plan = cur.fetchone()
        if not plan:
            flash("Plan not found.", "warning")
            return redirect(url_for("plans.plans_home"))
        cur.execute(
            "SELECT * FROM payment_plan_items WHERE plan_id=%s ORDER BY due_date ASC, id ASC",
            (plan_id,),
        )
        items = cur.fetchall() or []
    finally:
        db.close()
    return render_template("payment_plan_detail.html", plan=plan, items=items)


@plans_bp.route("/<int:plan_id>/cancel", methods=["POST"])
def plan_cancel(plan_id: int):
    db = _db()
    try:
        ensure_payment_plan_tables(db)
        cur = db.cursor()
        cur.execute(
            "UPDATE payment_plans SET status='cancelled', updated_at=%s WHERE id=%s AND school_id=%s",
            (datetime.utcnow(), plan_id, session.get("school_id")),
        )
        db.commit()
        flash("Plan cancelled.", "info")
    finally:
        db.close()
    return redirect(url_for("plans.plan_detail", plan_id=plan_id))
