def collections_overview():
    from flask import redirect, url_for
    try:
        if is_pro_enabled(app):
            return redirect(url_for('my_analytics'))
        return redirect(url_for('monetization.index'))
    except Exception:
        return redirect('/')


# ---------- ANALYTICS DATA (LIVE) ----------
@app.route("/api/analytics_data")
def analytics_data():
    """Provide live analytics for charts and class summary.

    Returns keys:
      - monthly_data: [{month, total}]
      - daily_trend: [{day, total}] last 30 days
      - class_summary: [{class_name, total_students, total_paid, total_pending, total_credit, percent_paid}]
      - method_breakdown: [{method, count, total}]
      - top_debtors: [{name, class_name, balance}]
      - mom: {current_month_total, prev_month_total, percent_change}
      - meta: {active_classes}
    """
    # Allow analytics for all plans so charts always show real-time data.
    # Historically this endpoint returned 403 on non-Pro which blanked charts.
    # We keep the is_pro_enabled call for compatibility but do not enforce.
    try:
        _ = is_pro_enabled(app)
    except Exception:
        pass

    db = get_db_connection()
    cursor = db.cursor(dictionary=True)

    # Resolve current academic context
    try:
        cy, ct = get_or_seed_current_term(db)
    except Exception:
        cy, ct = None, None

    # Monthly totals (by first day label for readability)
    if cy and ct in (1, 2, 3):
        cursor.execute(
            """
            SELECT DATE_FORMAT(MIN(date), '%b %Y') AS month, SUM(amount) AS total
            FROM payments
            WHERE method <> 'Credit Transfer' AND school_id=%s AND year=%s AND term=%s
            GROUP BY YEAR(date), MONTH(date)
            ORDER BY YEAR(date), MONTH(date)
            """,
            (session.get("school_id"), cy, ct),
        )
    else:
        cursor.execute(
            """
            SELECT DATE_FORMAT(MIN(date), '%b %Y') AS month, SUM(amount) AS total
            FROM payments
            WHERE method <> 'Credit Transfer' AND school_id=%s
            GROUP BY YEAR(date), MONTH(date)
            ORDER BY YEAR(date), MONTH(date)
            """,
            (session.get("school_id"),),
        )
    monthly_data = cursor.fetchall()

    # Daily trend - last 30 days
    if cy and ct in (1, 2, 3):
        cursor.execute(
            """
            SELECT DATE(date) AS day, SUM(amount) AS total
            FROM payments
            WHERE date >= (CURRENT_DATE - INTERVAL 29 DAY)
              AND method <> 'Credit Transfer' AND school_id=%s AND year=%s AND term=%s
            GROUP BY DATE(date)
            ORDER BY DATE(date)
            """,
            (session.get("school_id"), cy, ct),
        )
    else:
        cursor.execute(
            """
            SELECT DATE(date) AS day, SUM(amount) AS total
            FROM payments
            WHERE date >= (CURRENT_DATE - INTERVAL 29 DAY)
              AND method <> 'Credit Transfer' AND school_id=%s
            GROUP BY DATE(date)
            ORDER BY DATE(date)
            """,
            (session.get("school_id"),),
        )
    daily_trend = cursor.fetchall()

    # Class summary
    if cy and ct in (1, 2, 3):
        cursor.execute(
            """
            SELECT 
                s.class_name,
                COUNT(s.id) AS total_students,
                COALESCE(SUM(p.amount), 0) AS total_paid,
                COALESCE(SUM(COALESCE(s.balance, s.fee_balance)), 0) AS total_pending,
                COALESCE(SUM(s.credit), 0) AS total_credit
            FROM students s
            LEFT JOIN payments p ON s.id = p.student_id AND p.method <> 'Credit Transfer' AND p.school_id=%s AND p.year=%s AND p.term=%s
            WHERE s.school_id=%s
            GROUP BY s.class_name
            ORDER BY s.class_name
            """,
            (session.get("school_id"), cy, ct, session.get("school_id")),
        )
    else:
        cursor.execute(
            """
            SELECT 
                s.class_name,
                COUNT(s.id) AS total_students,
                COALESCE(SUM(p.amount), 0) AS total_paid,
                COALESCE(SUM(COALESCE(s.balance, s.fee_balance)), 0) AS total_pending,
                COALESCE(SUM(s.credit), 0) AS total_credit
            FROM students s
            LEFT JOIN payments p ON s.id = p.student_id AND p.method <> 'Credit Transfer' AND p.school_id=%s
            WHERE s.school_id=%s
            GROUP BY s.class_name
            ORDER BY s.class_name
            """,
            (session.get("school_id"), session.get("school_id")),
        )
    class_summary = cursor.fetchall()

    # Payment method breakdown
    if cy and ct in (1, 2, 3):
        cursor.execute(
            """
            SELECT method, COUNT(*) AS count, COALESCE(SUM(amount), 0) AS total
            FROM payments
            WHERE school_id=%s AND year=%s AND term=%s
            GROUP BY method
            ORDER BY total DESC
            """,
            (session.get("school_id"), cy, ct),
        )
    else:
        cursor.execute(
            """
            SELECT method, COUNT(*) AS count, COALESCE(SUM(amount), 0) AS total
            FROM payments
            WHERE school_id=%s
            GROUP BY method
            ORDER BY total DESC
            """,
            (session.get("school_id"),),
        )
    method_breakdown = cursor.fetchall()

    # Top debtors (highest balances)
    cursor.execute("SHOW COLUMNS FROM students LIKE 'balance'")
    has_balance = bool(cursor.fetchone())
    balance_col = "balance" if has_balance else "fee_balance"

    cursor.execute(
        f"""
        SELECT name, class_name, COALESCE({balance_col}, 0) AS balance
        FROM students
        WHERE school_id=%s
        ORDER BY COALESCE({balance_col}, 0) DESC
        LIMIT 5
        """,
        (session.get("school_id"),),
    )
    top_debtors = cursor.fetchall()

    # Month-over-month change
    if cy and ct in (1, 2, 3):
        cursor.execute(
            """
            SELECT 
                SUM(CASE WHEN YEAR(date) = YEAR(CURRENT_DATE) AND MONTH(date) = MONTH(CURRENT_DATE) THEN amount ELSE 0 END) AS current_month_total,
                SUM(CASE WHEN DATE_FORMAT(date, '%Y-%m') = DATE_FORMAT(DATE_SUB(CURRENT_DATE, INTERVAL 1 MONTH), '%Y-%m') THEN amount ELSE 0 END) AS prev_month_total
            FROM payments
            WHERE method <> 'Credit Transfer' AND school_id=%s AND year=%s AND term=%s
            """,
            (session.get("school_id"), cy, ct),
        )
    else:
        cursor.execute(
            """
            SELECT 
                SUM(CASE WHEN YEAR(date) = YEAR(CURRENT_DATE) AND MONTH(date) = MONTH(CURRENT_DATE) THEN amount ELSE 0 END) AS current_month_total,
                SUM(CASE WHEN DATE_FORMAT(date, '%Y-%m') = DATE_FORMAT(DATE_SUB(CURRENT_DATE, INTERVAL 1 MONTH), '%Y-%m') THEN amount ELSE 0 END) AS prev_month_total
            FROM payments
            WHERE method <> 'Credit Transfer' AND school_id=%s
            """,
            (session.get("school_id"),),
        )
    mom_row = cursor.fetchone() or {"current_month_total": 0, "prev_month_total": 0}
    current_month_total = float(mom_row.get("current_month_total") or 0)
    prev_month_total = float(mom_row.get("prev_month_total") or 0)
    percent_change = (
        round(((current_month_total - prev_month_total) / prev_month_total) * 100, 1)
        if prev_month_total > 0
        else (100.0 if current_month_total > 0 else 0.0)
    )

    # Meta: active classes
    cursor.execute("SELECT COUNT(DISTINCT class_name) AS active_classes FROM students WHERE school_id=%s", (session.get("school_id"),))
    active_classes = (cursor.fetchone() or {}).get("active_classes", 0)

    db.close()

    # Normalize rows to JSON-safe primitives (avoid Decimal/date serialization issues)
    try:
        from datetime import date, datetime  # type: ignore
    except Exception:  # pragma: no cover
        date, datetime = None, None

    def _to_float(v):
        try:
            return float(v or 0)
        except Exception:
            return 0.0

    def _to_int(v):
        try:
            return int(v or 0)
        except Exception:
            return 0

    # Monthly totals
    monthly_data = [
        {"month": (r.get("month") if isinstance(r, dict) else r[0]), "total": _to_float((r.get("total") if isinstance(r, dict) else r[1]))}
        for r in (monthly_data or [])
    ]

    # Daily trend with ISO day
    _daily = []
    for r in (daily_trend or []):
        day_val = (r.get("day") if isinstance(r, dict) else r[0])
