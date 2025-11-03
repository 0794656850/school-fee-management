from __future__ import annotations

from flask import request, redirect, url_for, flash, session

# Reuse existing term blueprint and DB helpers
from routes.term_routes import term_bp, _db, ensure_term_fees_table, ensure_discounts_table
from routes.credit_routes import ensure_students_credit_column


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
        ensure_students_credit_column(db)
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

        # Fetch existing flat fees for those students (prefer stored final_fee when present)
        ph = ",".join(["%s"] * len(ids))
        cur.execute(
            f"SELECT student_id, COALESCE(final_fee, fee_amount) AS fee_amount, initial_fee, adjusted_fee, discount, final_fee FROM term_fees WHERE year=%s AND term=%s AND student_id IN ({ph})",
            (year, term, *ids),
        )
        prev_rows = {r["student_id"]: r for r in (cur.fetchall() or [])}
        is_adjustment = len(prev_rows) > 0

        # Load per-student discounts for this term (Pro feature table). If not present for a student,
        # we'll fallback to any per-row discount stored in term_fees.
        ensure_discounts_table(db)
        disc_map = {}
        cur.execute(
            f"SELECT student_id, kind, value FROM discounts WHERE year=%s AND term=%s AND student_id IN ({ph})",
            (year, term, *ids),
        )
        for r in (cur.fetchall() or []):
            disc_map[r["student_id"]] = {"kind": r.get("kind"), "value": float(r.get("value") or 0)}

        # Current balances and credits to prevent negative balances; any over-reduction becomes credit
        cur.execute(
            f"SELECT id, COALESCE({bal_col},0) AS bal, COALESCE(credit,0) AS credit FROM students WHERE school_id=%s AND id IN ({ph})",
            (session.get("school_id"), *ids),
        )
        bc_map = {r["id"]: {"bal": float(r.get("bal") or 0.0), "credit": float(r.get("credit") or 0.0)} for r in (cur.fetchall() or [])}

        # Apply upserts and adjust balances, computing final fees per student
        cur2 = db.cursor()
        updated = 0
        total_delta = 0.0
        any_discount_used = False
        for sid in ids:
            prow = prev_rows.get(sid) or {}
            prev_final = float(prow.get("final_fee") if prow.get("final_fee") is not None else prow.get("fee_amount") or 0.0)

            drow = disc_map.get(sid)
            disc_value = 0.0
            if drow:
                if drow.get("kind") == "percent":
                    disc_value = round(float(amount) * (drow.get("value", 0.0) / 100.0), 2)
                else:
                    disc_value = float(drow.get("value") or 0.0)
            else:
                disc_value = float(prow.get("discount") or 0.0)

            if disc_value > float(amount):
                disc_value = float(amount)

            new_final = max(float(amount) - disc_value, 0.0)
            if disc_value > 0:
                any_discount_used = True

            # Upsert into term_fees; mirror new_final into fee_amount for backward compatibility
            cur2.execute(
                "INSERT INTO term_fees (student_id, year, term, fee_amount, initial_fee, final_fee, school_id) VALUES (%s,%s,%s,%s,%s,%s,%s)"
                " ON DUPLICATE KEY UPDATE fee_amount=VALUES(fee_amount), adjusted_fee=VALUES(initial_fee), final_fee=VALUES(final_fee), school_id=VALUES(school_id)",
                (sid, year, term, new_final, amount, new_final, session.get("school_id")),
            )

            delta = new_final - prev_final
            if abs(delta) > 0:
                bal = bc_map.get(sid, {}).get("bal", 0.0)
                credit = bc_map.get(sid, {}).get("credit", 0.0)
                new_bal = (bal or 0.0) + delta
                if new_bal < 0:
                    # Move over-reduction into credit and clamp balance at zero
                    add_credit = abs(new_bal)
                    cur2.execute(
                        f"UPDATE students SET {bal_col} = 0, credit = COALESCE(credit,0) + %s WHERE id=%s AND school_id=%s",
                        (add_credit, sid, session.get("school_id")),
                    )
                    # Update local map for any subsequent logic (though not strictly needed)
                    bc_map[sid] = {"bal": 0.0, "credit": (credit or 0.0) + add_credit}
                    total_delta += delta
                else:
                    cur2.execute(
                        f"UPDATE students SET {bal_col} = %s WHERE id=%s AND school_id=%s",
                        (new_bal, sid, session.get("school_id")),
                    )
                    bc_map[sid] = {"bal": new_bal, "credit": credit or 0.0}
                    total_delta += delta
            updated += 1
        db.commit()
        note = " Discounts applied where present." if any_discount_used else ""
        kind = "adjusted" if is_adjustment else "initial"
        flash(f"Applied {kind} flat KES {amount:,.2f} to {updated} student(s).{note} Total balance delta: KES {total_delta:,.2f}", "success")
    except Exception as e:
        try:
            db.rollback()
        except Exception:
            pass
        flash(f"Error applying flat fee: {e}", "error")
    finally:
        db.close()
    return redirect(url_for("terms.manage_term_fees", year=year, term=term))
