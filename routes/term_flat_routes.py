from __future__ import annotations

from flask import request, redirect, url_for, flash, session

# Reuse existing term blueprint and DB helpers
from routes.term_routes import term_bp, _db, ensure_term_fees_table


@term_bp.route("/fees/apply_flat", methods=["POST"])
def apply_flat_fee_all():
    """Apply same flat fee amount to every student for a selected year/term.

    Optionally filter by class_name if provided (applies only to students in that class).
    Updates each student's balance by the delta between previous flat fee and the new flat fee.

    This operates on legacy flat term_fees and does not touch itemized fee items.
    """
    year = request.form.get("year", type=int)
    term = request.form.get("term", type=int)
    amount = request.form.get("amount", type=float)
    class_name = (request.form.get("class_name") or "").strip() or None
    if not (year and term in (1, 2, 3)):
        flash("Provide a valid year and term.", "warning")
        return redirect(url_for("terms.manage_term_fees", year=year, term=term))
    if amount is None or amount < 0:
        flash("Provide a valid non-negative amount.", "warning")
        return redirect(url_for("terms.manage_term_fees", year=year, term=term))

    db = _db()
    try:
        ensure_term_fees_table(db)
        cur = db.cursor(dictionary=True)
        # Determine balance column
        cur.execute("SHOW COLUMNS FROM students LIKE 'balance'")
        has_balance = bool(cur.fetchone())
        bal_col = "balance" if has_balance else "fee_balance"

        # Fetch student IDs (optionally by class) scoped to school
        sid = session.get("school_id")
        if class_name:
            cur.execute(
                "SELECT id FROM students WHERE class_name=%s AND school_id=%s",
                (class_name, sid),
            )
        else:
            cur.execute("SELECT id FROM students WHERE school_id=%s", (sid,))
        ids = [r["id"] for r in (cur.fetchall() or [])]
        if not ids:
            flash("No students found to apply the flat fee.", "info")
            return redirect(url_for("terms.manage_term_fees", year=year, term=term))

        # Fetch existing flat fees for those students
        ph = ",".join(["%s"] * len(ids))
        cur.execute(
            f"SELECT student_id, fee_amount FROM term_fees WHERE year=%s AND term=%s AND student_id IN ({ph})",
            (year, term, *ids),
        )
        prev_map = {r["student_id"]: float(r.get("fee_amount") or 0) for r in (cur.fetchall() or [])}

        # Apply upserts and adjust balances
        cur2 = db.cursor()
        updated = 0
        total_delta = 0.0
        for sid in ids:
            prev_amt = prev_map.get(sid, 0.0)
            delta = float(amount) - prev_amt
            # Upsert into term_fees
            cur2.execute(
                "INSERT INTO term_fees (student_id, year, term, fee_amount, school_id) VALUES (%s,%s,%s,%s,%s)"
                " ON DUPLICATE KEY UPDATE fee_amount=VALUES(fee_amount), school_id=VALUES(school_id)",
                (sid, year, term, amount, session.get("school_id")),
            )
            # Adjust balance by delta
            if abs(delta) > 0:
                cur2.execute(
                    f"UPDATE students SET {bal_col} = COALESCE({bal_col},0) + %s WHERE id=%s AND school_id=%s",
                    (delta, sid, session.get("school_id")),
                )
                total_delta += delta
            updated += 1
        db.commit()
        flash(
            f"Applied flat fee KES {amount:,.2f} to {updated} student(s) for {year} T{term}. Balance delta total: KES {total_delta:,.2f}",
            "success",
        )
    except Exception as e:
        try:
            db.rollback()
        except Exception:
            pass
        flash(f"Error applying flat fee: {e}", "error")
    finally:
        db.close()
    return redirect(url_for("terms.manage_term_fees", year=year, term=term))
