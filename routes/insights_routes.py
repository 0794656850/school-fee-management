from __future__ import annotations

from flask import Blueprint, render_template, current_app, session, request, flash, redirect, url_for

from utils.alerts import detect_anomalies, summarize_alerts
from utils.notifications import send_alert_email
from utils.audit import log_event

insights_bp = Blueprint("insights", __name__, url_prefix="/admin/insights")


def _db_conn():
    try:
        from app import get_db_connection  # noqa: W0611 - lazy to avoid circular import

        return get_db_connection()
    except Exception:
        return None


def _ensure_access():
    try:
        from routes.admin_routes import _require_admin

        guard = _require_admin()
        return guard
    except Exception:
        return redirect(url_for("admin.login"))


@insights_bp.route("", methods=["GET", "POST"])
def insights_dashboard():
    guard = _ensure_access()
    if guard is not None:
        return guard
    sid = session.get("school_id")
    if not sid:
        flash("Select a school before viewing insights.", "warning")
        return redirect(url_for("choose_school"))
    db = _db_conn()
    metrics = {}
    anomalies = []
    try:
        if db:
            metrics = detect_anomalies(db, int(sid))
            thresholds = {
                "collection_drop": current_app.config.get("ALERT_COLLECTION_DROP_PERCENT", 30),
                "failed_ratio": current_app.config.get("ALERT_FAILED_PAYMENT_RATIO", 1.5),
                "unused_credit": current_app.config.get("ALERT_UNUSED_CREDITS_THRESHOLD", 5000),
            }
            anomalies = list(summarize_alerts(metrics, thresholds))
    finally:
        if db:
            try:
                db.close()
            except Exception:
                pass

    if request.method == "POST":
        recipients = current_app.config.get("ALERT_EMAIL_RECIPIENTS", ())
        if not anomalies:
            flash("No anomalies detected; nothing to alert.", "info")
        elif not recipients:
            flash("No alert recipients configured.", "error")
        else:
            subject = f"{current_app.config.get('BRAND_NAME', 'Fee Management')} | Audit alerts"
            body_lines = [
                "Anomalies detected in the last 7 days:",
            ] + [f"- {title}: {desc}" for title, desc in anomalies]
            body = "\n".join(body_lines)
            results = send_alert_email(subject, body, recipients)
            success = all(results.get(recipient, False) for recipient in recipients)
            if success:
                log_event(
                    "insights_alert",
                    "alert_sent",
                    detail=f"Sent alerts to {', '.join(recipients)}",
                )
                flash("Alert email sent to configured recipients.", "success")
            else:
                log_event("insights_alert", "alert_failed", detail=f"Recipients: {results}")
                flash("Failed to send alert email. Check logs for details.", "error")
        return redirect(url_for("insights.insights_dashboard"))

    failed_ratio_display = "N/A"
    failed_ratio = metrics.get("failed_payments", {}).get("ratio")
    if failed_ratio is not None:
        if failed_ratio == float("inf"):
            failed_ratio_display = "∞"
        else:
            failed_ratio_display = f"{failed_ratio:.1f}x"
    return render_template(
        "admin/insights.html",
        metrics=metrics,
        anomalies=anomalies,
        recipients=current_app.config.get("ALERT_EMAIL_RECIPIENTS", ()),
        failed_ratio_display=failed_ratio_display,
    )
