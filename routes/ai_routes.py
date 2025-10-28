from __future__ import annotations

from flask import Blueprint, render_template, request, jsonify, session
from datetime import datetime
from typing import Any, Dict, List

from utils.ai import classify_intent, answer_with_ai
from utils.settings import get_settings
from utils.settings import _db as _get_conn
from utils.pro import is_pro_enabled, upgrade_url


ai_bp = Blueprint("ai", __name__, url_prefix="/ai")


def _ensure_ai_tables(db) -> None:
    cur = db.cursor()
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS ai_chats (
            id INT AUTO_INCREMENT PRIMARY KEY,
            title VARCHAR(200) NOT NULL,
            created_at DATETIME NOT NULL,
            updated_at DATETIME NOT NULL
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
        """
    )
    # Ensure tenant scoping column/index
    try:
        cur.execute("SHOW COLUMNS FROM ai_chats LIKE 'school_id'")
        if not cur.fetchone():
            cur.execute("ALTER TABLE ai_chats ADD COLUMN school_id INT NULL")
            try:
                cur.execute("CREATE INDEX idx_ai_chats_school ON ai_chats(school_id)")
            except Exception:
                pass
            db.commit()
    except Exception:
        try:
            db.rollback()
        except Exception:
            pass
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS ai_messages (
            id INT AUTO_INCREMENT PRIMARY KEY,
            chat_id INT NOT NULL,
            role ENUM('user','assistant') NOT NULL,
            content MEDIUMTEXT NOT NULL,
            created_at DATETIME NOT NULL,
            INDEX idx_chat (chat_id),
            CONSTRAINT fk_chat FOREIGN KEY (chat_id) REFERENCES ai_chats(id) ON DELETE CASCADE
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
        """
    )
    db.commit()


def _list_chats(db) -> List[Dict[str, Any]]:
    _ensure_ai_tables(db)
    cur = db.cursor(dictionary=True)
    sid = session.get("school_id") if session else None
    if sid:
        cur.execute("SELECT id, title, updated_at FROM ai_chats WHERE school_id=%s ORDER BY updated_at DESC", (sid,))
    else:
        cur.execute("SELECT id, title, updated_at FROM ai_chats ORDER BY updated_at DESC")
    return cur.fetchall() or []


@ai_bp.route("/")
def ai_home():
    settings = get_settings([
        "OPENAI_API_KEY",
        "AZURE_OPENAI_API_KEY",
    ])
    db = _get_conn()
    try:
        chats = _list_chats(db)
    finally:
        db.close()
    return render_template(
        "ai.html",
        settings=settings,
        is_pro=is_pro_enabled(),
        upgrade_link=upgrade_url(),
        chats=chats,
    )


def _find_student_by_hint(db, name: str | None, admission_no: str | None) -> Dict[str, Any] | None:
    cur = db.cursor(dictionary=True)
    sid = session.get("school_id") if session else None
    if admission_no:
        if sid:
            cur.execute("SELECT * FROM students WHERE LOWER(admission_no)=LOWER(%s) AND school_id=%s LIMIT 1", (admission_no, sid))
        else:
            cur.execute("SELECT * FROM students WHERE LOWER(admission_no)=LOWER(%s) LIMIT 1", (admission_no,))
        row = cur.fetchone()
        if row:
            return row
    if name:
        if sid:
            cur.execute("SELECT * FROM students WHERE LOWER(name)=LOWER(%s) AND school_id=%s LIMIT 1", (name, sid))
        else:
            cur.execute("SELECT * FROM students WHERE LOWER(name)=LOWER(%s) LIMIT 1", (name,))
        row = cur.fetchone()
        if row:
            return row
        if sid:
            cur.execute("SELECT * FROM students WHERE name LIKE %s AND school_id=%s ORDER BY id DESC LIMIT 1", (f"%{name}%", sid))
        else:
            cur.execute("SELECT * FROM students WHERE name LIKE %s ORDER BY id DESC LIMIT 1", (f"%{name}%",))
        row = cur.fetchone()
        if row:
            return row
    return None


@ai_bp.route("/api/chats", methods=["GET"])
def list_chats_api():
    db = _get_conn()
    try:
        chats = _list_chats(db)
        return jsonify({"ok": True, "chats": chats})
    finally:
        db.close()


@ai_bp.route("/api/new_chat", methods=["POST"])
def new_chat_api():
    db = _get_conn()
    try:
        _ensure_ai_tables(db)
        now = datetime.now()
        # Prefer provided title; otherwise generate timestamped default
        title = (request.json or {}).get("title") or now.strftime("Chat %Y-%m-%d %H:%M")
        cur = db.cursor()
        sid = session.get("school_id") if session else None
        if sid:
            cur.execute(
                "INSERT INTO ai_chats (title, created_at, updated_at, school_id) VALUES (%s,%s,%s,%s)",
                (title, now, now, sid),
            )
        else:
            cur.execute(
                "INSERT INTO ai_chats (title, created_at, updated_at) VALUES (%s,%s,%s)",
                (title, now, now),
            )
        db.commit()
        return jsonify({"ok": True, "chat_id": cur.lastrowid, "title": title})
    finally:
        db.close()


@ai_bp.route("/api/chats/<int:chat_id>", methods=["DELETE"])
def delete_chat_api(chat_id: int):
    db = _get_conn()
    try:
        _ensure_ai_tables(db)
        cur = db.cursor()
        sid = session.get("school_id") if session else None
        if sid:
            cur.execute("DELETE FROM ai_chats WHERE id=%s AND school_id=%s", (chat_id, sid))
        else:
            cur.execute("DELETE FROM ai_chats WHERE id=%s", (chat_id,))
        db.commit()
        return jsonify({"ok": True})
    finally:
        db.close()


@ai_bp.route("/api/messages", methods=["GET"])
def get_messages_api():
    chat_id = request.args.get("chat_id", type=int)
    if not chat_id:
        return jsonify({"ok": False, "error": "chat_id is required"}), 400
    db = _get_conn()
    try:
        _ensure_ai_tables(db)
        cur = db.cursor(dictionary=True)
        # Ensure requested chat belongs to current school
        sid = session.get("school_id") if session else None
        if sid:
            cur.execute("SELECT id FROM ai_chats WHERE id=%s AND school_id=%s", (chat_id, sid))
            if not cur.fetchone():
                return jsonify({"ok": False, "error": "Not found"}), 404
        cur.execute(
            "SELECT role, content, created_at FROM ai_messages WHERE chat_id=%s ORDER BY id ASC",
            (chat_id,),
        )
        return jsonify({"ok": True, "messages": cur.fetchall() or []})
    finally:
        db.close()


@ai_bp.route("/api/chat", methods=["POST"])
def ai_chat():
    data = request.get_json(silent=True) or {}
    query = (data.get("message") or "").strip()
    chat_id = data.get("chat_id")
    if not query:
        return jsonify({"ok": False, "answer": "Please provide a message."}), 400

    intent, entities = classify_intent(query)

    try:
        db = _get_conn()
        _ensure_ai_tables(db)
        cur = db.cursor(dictionary=True)
        # If no chat provided, create one using first words of the query as title
        if not chat_id:
            t = (query[:40] + ("..." if len(query) > 40 else "")).strip() or "New Chat"
            cur2 = db.cursor()
            now = datetime.now()
            sid = session.get("school_id") if session else None
            if sid:
                cur2.execute("INSERT INTO ai_chats (title, created_at, updated_at, school_id) VALUES (%s,%s,%s,%s)", (t, now, now, sid))
            else:
                cur2.execute("INSERT INTO ai_chats (title, created_at, updated_at) VALUES (%s,%s,%s)", (t, now, now))
            db.commit()
            chat_id = cur2.lastrowid
        else:
            # Validate chat belongs to school
            sid = session.get("school_id") if session else None
            if sid:
                cur.execute("SELECT id FROM ai_chats WHERE id=%s AND school_id=%s", (chat_id, sid))
                if not cur.fetchone():
                    return jsonify({"ok": False, "error": "Not found"}), 404
        # Store user message
        now = datetime.now()
        cur.execute(
            "INSERT INTO ai_messages (chat_id, role, content, created_at) VALUES (%s,%s,%s,%s)",
            (chat_id, 'user', query, now),
        )

        if intent == "student_balance":
            name = (entities.get("student_name") or "").strip() if isinstance(entities, dict) else ""
            adm = (entities.get("admission_no") or "").strip() if isinstance(entities, dict) else ""
            student = _find_student_by_hint(db, name or None, adm or None)
            if not student:
                import re
                m = re.search(r"for\s+([a-zA-Z\s\-']{2,50})", query, re.IGNORECASE)
                hint = m.group(1).strip() if m else None
                student = _find_student_by_hint(db, hint, None) if hint else None
            if not student:
                answer = "I couldn't find that student. Provide name or admission number."
                cur.execute(
                    "INSERT INTO ai_messages (chat_id, role, content, created_at) VALUES (%s,%s,%s,%s)",
                    (chat_id, 'assistant', answer, now),
                )
                cur.execute("UPDATE ai_chats SET updated_at=%s WHERE id=%s", (now, chat_id))
                db.commit()
                return jsonify({"ok": True, "answer": answer, "chat_id": chat_id})
            cur.execute("SHOW COLUMNS FROM students LIKE 'balance'")
            has_balance = bool(cur.fetchone())
            bal_col = "balance" if has_balance else "fee_balance"
            balance = float(student.get(bal_col) or 0)
            credit = float(student.get("credit") or 0)
            cls = student.get("class_name")
            name_out = student.get("name")
            answer = (
                f"{name_out} ({cls}) currently owes KES {balance:,.2f}. "
                f"Credit on account: KES {credit:,.2f}."
            )
            cur.execute(
                "INSERT INTO ai_messages (chat_id, role, content, created_at) VALUES (%s,%s,%s,%s)",
                (chat_id, 'assistant', answer, now),
            )
            cur.execute("UPDATE ai_chats SET updated_at=%s WHERE id=%s", (now, chat_id))
            db.commit()
            return jsonify({"ok": True, "answer": answer, "chat_id": chat_id, "title": t})

        if intent == "top_debtors":
            n = int((entities or {}).get("count", 5))
            if n < 1:
                n = 5
            if n > 25:
                n = 25
            cur.execute("SHOW COLUMNS FROM students LIKE 'balance'")
            has_balance = bool(cur.fetchone())
            bal_col = "balance" if has_balance else "fee_balance"
            sid = session.get("school_id") if session else None
            if sid:
                cur.execute(
                    f"SELECT name, class_name, COALESCE({bal_col},0) AS balance FROM students WHERE school_id=%s ORDER BY COALESCE({bal_col},0) DESC LIMIT %s",
                    (sid, n),
                )
            else:
                cur.execute(
                    f"SELECT name, class_name, COALESCE({bal_col},0) AS balance FROM students ORDER BY COALESCE({bal_col},0) DESC LIMIT %s",
                    (n,),
                )
            rows = cur.fetchall() or []
            if not rows:
                answer = "No students found."
            else:
                lines = [f"{i+1}. {r['name']} ({r['class_name']}): KES {float(r['balance']):,.2f}" for i, r in enumerate(rows)]
                answer = "Top debtors:\n" + "\n".join(lines)
            cur.execute(
                "INSERT INTO ai_messages (chat_id, role, content, created_at) VALUES (%s,%s,%s,%s)",
                (chat_id, 'assistant', answer, now),
            )
            cur.execute("UPDATE ai_chats SET updated_at=%s WHERE id=%s", (now, chat_id))
            db.commit()
            return jsonify({"ok": True, "answer": answer, "chat_id": chat_id, "title": t if 't' in locals() else None})

        if intent == "analytics_summary":
            cur.execute("SHOW COLUMNS FROM students LIKE 'balance'")
            has_balance = bool(cur.fetchone())
            bal_col = "balance" if has_balance else "fee_balance"
            sid = session.get("school_id") if session else None
            if sid:
                cur.execute("SELECT COUNT(*) AS total FROM students WHERE school_id=%s", (sid,))
            else:
                cur.execute("SELECT COUNT(*) AS total FROM students")
            total_students = (cur.fetchone() or {}).get("total", 0)
            if sid:
                cur.execute("SELECT COALESCE(SUM(amount),0) AS total_collected FROM payments WHERE method <> 'Credit Transfer' AND school_id=%s", (sid,))
            else:
                cur.execute("SELECT COALESCE(SUM(amount),0) AS total_collected FROM payments WHERE method <> 'Credit Transfer'")
            total_collected = float((cur.fetchone() or {}).get("total_collected", 0))
            if sid:
                cur.execute(f"SELECT COALESCE(SUM({bal_col}),0) AS total_balance FROM students WHERE school_id=%s", (sid,))
            else:
                cur.execute(f"SELECT COALESCE(SUM({bal_col}),0) AS total_balance FROM students")
            total_balance = float((cur.fetchone() or {}).get("total_balance", 0))
            if sid:
                cur.execute("SELECT COALESCE(SUM(credit),0) AS total_credit FROM students WHERE school_id=%s", (sid,))
            else:
                cur.execute("SELECT COALESCE(SUM(credit),0) AS total_credit FROM students")
            total_credit = float((cur.fetchone() or {}).get("total_credit", 0))
            answer = (
                f"Students: {total_students}\n"
                f"Collected: KES {total_collected:,.2f}\n"
                f"Pending: KES {total_balance:,.2f}\n"
                f"Credit:  KES {total_credit:,.2f}"
            )
            cur.execute(
                "INSERT INTO ai_messages (chat_id, role, content, created_at) VALUES (%s,%s,%s,%s)",
                (chat_id, 'assistant', answer, now),
            )
            cur.execute("UPDATE ai_chats SET updated_at=%s WHERE id=%s", (now, chat_id))
            db.commit()
            return jsonify({"ok": True, "answer": answer, "chat_id": chat_id, "title": t if 't' in locals() else None})

        if intent == "generate_reminder":
            # Try infer student to personalize
            cur.execute("SHOW COLUMNS FROM students LIKE 'balance'")
            has_balance = bool(cur.fetchone())
            bal_col = "balance" if has_balance else "fee_balance"
            import re
            m = re.search(r"for\s+([a-zA-Z\s\-']{2,50})", query, re.IGNORECASE)
            hint = m.group(1).strip() if m else None
            student = _find_student_by_hint(db, hint, None) if hint else None
            if student:
                balance = float(student.get(bal_col) or 0)
                name = student.get("name")
                answer = (
                    f"Dear {name}, this is a friendly reminder that your outstanding "
                    f"fee balance is KES {balance:,.2f}. Kindly clear payment at your "
                    f"earliest convenience. Thank you."
                )
            else:
                answer = (
                    "Dear Parent/Guardian, this is a reminder that there is an outstanding "
                    "school fee balance. Kindly clear payment at your earliest convenience. Thank you."
                )
            cur.execute(
                "INSERT INTO ai_messages (chat_id, role, content, created_at) VALUES (%s,%s,%s,%s)",
                (chat_id, 'assistant', answer, now),
            )
            cur.execute("UPDATE ai_chats SET updated_at=%s WHERE id=%s", (now, chat_id))
            db.commit()
            return jsonify({"ok": True, "answer": answer, "chat_id": chat_id, "title": t if 't' in locals() else None})

        # Unknown intent -> general answer using AI with safe context (totals only)
        sid = session.get("school_id") if session else None
        if sid:
            cur.execute("SELECT COUNT(*) AS total FROM students WHERE school_id=%s", (sid,))
            total_students = (cur.fetchone() or {}).get("total", 0)
            cur.execute("SELECT COALESCE(SUM(amount),0) AS total_collected FROM payments WHERE school_id=%s", (sid,))
            total_collected = float((cur.fetchone() or {}).get("total_collected", 0))
        else:
            cur.execute("SELECT COUNT(*) AS total FROM students")
            total_students = (cur.fetchone() or {}).get("total", 0)
            cur.execute("SELECT COALESCE(SUM(amount),0) AS total_collected FROM payments")
            total_collected = float((cur.fetchone() or {}).get("total_collected", 0))
        context = f"Students: {total_students}\nTotal collected: {total_collected:,.2f}"
        answer = answer_with_ai(context, query)
        cur.execute(
            "INSERT INTO ai_messages (chat_id, role, content, created_at) VALUES (%s,%s,%s,%s)",
            (chat_id, 'assistant', answer, now),
        )
        cur.execute("UPDATE ai_chats SET updated_at=%s WHERE id=%s", (now, chat_id))
        db.commit()
        return jsonify({"ok": True, "answer": answer, "chat_id": chat_id, "title": t if 't' in locals() else None})
    finally:
        try:
            db and db.close()
        except Exception:
            pass
