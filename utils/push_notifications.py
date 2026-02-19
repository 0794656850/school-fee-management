from __future__ import annotations

import json
import os
from typing import Any, Dict

from flask import current_app

from utils.db_helpers import ensure_guardian_push_table

try:
    from pywebpush import webpush, WebPushException  # type: ignore
except Exception:  # pragma: no cover - optional dependency
    webpush = None
    WebPushException = Exception


def _vapid_config() -> Dict[str, str]:
    cfg = current_app.config if current_app else {}
    public_key = (cfg.get("VAPID_PUBLIC_KEY") or os.environ.get("VAPID_PUBLIC_KEY") or "").strip()
    private_key = (cfg.get("VAPID_PRIVATE_KEY") or os.environ.get("VAPID_PRIVATE_KEY") or "").strip()
    subject = (cfg.get("VAPID_SUBJECT") or os.environ.get("VAPID_SUBJECT") or "mailto:admin@example.com").strip()
    return {"public": public_key, "private": private_key, "subject": subject}


def send_web_push_to_student(db, school_id: int, student_id: int, payload: Dict[str, Any]) -> int:
    if webpush is None:
        return 0
    cfg = _vapid_config()
    if not cfg["public"] or not cfg["private"]:
        return 0
    ensure_guardian_push_table(db)
    cur = db.cursor(dictionary=True)
    cur.execute(
        """
        SELECT id, endpoint, p256dh, auth
        FROM guardian_push_subscriptions
        WHERE school_id=%s AND student_id=%s
        """,
        (school_id, student_id),
    )
    subs = cur.fetchall() or []
    sent = 0
    for s in subs:
        sub = {
            "endpoint": s["endpoint"],
            "keys": {"p256dh": s["p256dh"], "auth": s["auth"]},
        }
        try:
            webpush(
                subscription_info=sub,
                data=json.dumps(payload),
                vapid_private_key=cfg["private"],
                vapid_claims={"sub": cfg["subject"]},
            )
            sent += 1
        except WebPushException as exc:
            try:
                status = getattr(exc.response, "status_code", None)
            except Exception:
                status = None
            if status in (404, 410):
                try:
                    cur2 = db.cursor()
                    cur2.execute("DELETE FROM guardian_push_subscriptions WHERE id=%s", (s["id"],))
                    db.commit()
                except Exception:
                    pass
            continue
    return sent
