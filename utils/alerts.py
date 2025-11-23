from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any, Dict, Iterable, Tuple


def _sum_payments(cur, school_id: int, start: datetime, end: datetime | None = None) -> float:
    q = "SELECT COALESCE(SUM(amount), 0) FROM payments WHERE school_id=%s AND date >= %s"
    params: list[Any] = [school_id, start]
    if end is not None:
        q += " AND date < %s"
        params.append(end)
    cur.execute(q, tuple(params))
    return float((cur.fetchone() or [0])[0] or 0)


def _count_failed_payments(cur, school_id: int, start: datetime, end: datetime | None = None) -> int:
    q = "SELECT COUNT(*) FROM mpesa_student_payments WHERE school_id=%s AND result_code IS NOT NULL AND result_code <> '0' AND updated_at >= %s"
    params: list[Any] = [school_id, start]
    if end is not None:
        q += " AND updated_at < %s"
        params.append(end)
    cur.execute(q, tuple(params))
    return int((cur.fetchone() or [0])[0] or 0)


def detect_anomalies(db, school_id: int) -> Dict[str, Any]:
    now = datetime.utcnow()
    current_start = now - timedelta(days=7)
    previous_start = now - timedelta(days=14)

    cur = db.cursor()
    current_collection = _sum_payments(cur, school_id, current_start)
    prev_collection = _sum_payments(cur, school_id, previous_start, current_start)
    current_failed = _count_failed_payments(cur, school_id, current_start)
    prev_failed = _count_failed_payments(cur, school_id, previous_start, current_start)
    cur.execute(
        "SELECT COUNT(*), COALESCE(SUM(credit),0) FROM students WHERE school_id=%s AND credit > 0",
        (school_id,),
    )
    unused_count, unused_total = cur.fetchone() or (0, 0)
    unused_total = float(unused_total or 0)
    return {
        "collections": {
            "current": current_collection,
            "previous": prev_collection,
            "drop_pct": (
                (prev_collection - current_collection) / prev_collection * 100
                if prev_collection > 0
                else 0.0
            ),
        },
        "failed_payments": {
            "current": current_failed,
            "previous": prev_failed,
            "ratio": (current_failed / prev_failed) if prev_failed > 0 else float("inf") if current_failed else 0,
        },
        "unused_credits": {
            "count": int(unused_count or 0),
            "total": unused_total,
        },
        "timestamp": now.isoformat(),
    }


def summarize_alerts(metrics: Dict[str, Any], thresholds: Dict[str, float]) -> Iterable[Tuple[str, str]]:
    alerts = []
    coll = metrics["collections"]
    if coll["drop_pct"] >= thresholds.get("collection_drop", 0):
        alerts.append(
            (
                "Sudden collections drop",
                f"Collections over the last 7 days ({coll['current']:.2f}) dropped by "
                f"{coll['drop_pct']:.1f}% vs the previous week ({coll['previous']:.2f}).",
            )
        )
    failed = metrics["failed_payments"]
    if failed["ratio"] >= thresholds.get("failed_ratio", float("inf")):
        alerts.append(
            (
                "Failed payments spike",
                f"{failed['current']} failed callbacks this week vs {failed['previous']} the previous week.",
            )
        )
    unused = metrics["unused_credits"]
    if unused["total"] >= thresholds.get("unused_credit", float("inf")):
        alerts.append(
            (
                "Unused credit buildup",
                f"{unused['count']} students hold a total of KES {unused['total']:.2f} in unused credit.",
            )
        )
    return alerts
