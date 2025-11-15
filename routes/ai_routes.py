from __future__ import annotations

from flask import Blueprint, render_template, request, jsonify, session, Response, stream_with_context
import json
from datetime import datetime
from typing import Any, Dict, List

from utils.ai import classify_intent, answer_with_ai, answer_with_ai_rag, ai_is_configured, chat_anything, chat_anything_stream, rag_status, ai_provider
from ai_engine.query import handle_query
from utils.settings import get_settings
from utils.settings import _db as _get_conn
from utils.pro import is_pro_enabled, upgrade_url
from utils.audit import log_event


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
        "VERTEX_PROJECT_ID",
        "VERTEX_LOCATION",
        "GOOGLE_APPLICATION_CREDENTIALS",
    ])
    configured = False
    try:
        configured = ai_is_configured()
    except Exception:
        configured = False
    kb = {}
    try:
        kb = rag_status()
    except Exception:
        kb = {"project_index": {"available": False, "chunks": 0}, "user_index": {"available": False, "chunks": 0}}
    chats = []
    try:
        db = _get_conn()
        try:
            chats = _list_chats(db)
        finally:
            db.close()
    except Exception:
        chats = []
    # Determine active AI provider label
    try:
        provider = ai_provider()
    except Exception:
        provider = "none"
    insights = {"defaults": [], "strategies": [], "methods": [], "summary": {"collected": 0, "pending": 0, "credit": 0, "collection_rate": 0.0}}
    try:
        db = _get_conn()
        cursor = db.cursor(dictionary=True)
        sid = session.get("school_id") if session else None
        school_filter = "AND school_id=%s" if sid else ""
        params = (sid,) if sid else ()
        cursor.execute(
            f"SELECT id, name, class_name, COALESCE(balance, fee_balance,0) AS balance FROM students WHERE COALESCE(balance, fee_balance,0) > 0 {school_filter} ORDER BY balance DESC LIMIT 5",
            params,
        )
        insights["defaults"] = cursor.fetchall() or []
        cursor.execute(
            f"SELECT class_name, COUNT(*) AS due_students, SUM(COALESCE(balance, fee_balance,0)) AS total_due FROM students WHERE COALESCE(balance, fee_balance,0) > 0 {school_filter} GROUP BY class_name ORDER BY total_due DESC LIMIT 4",
            params,
        )
        insights["strategies"] = cursor.fetchall() or []
        cursor.execute(
            f"SELECT method, COUNT(*) AS count, SUM(amount) AS total FROM payments WHERE method <> 'Credit Transfer' {school_filter} GROUP BY method ORDER BY total DESC LIMIT 4",
            params,
        )
        insights["methods"] = cursor.fetchall() or []
        cursor.execute(
            f"SELECT COALESCE(SUM(amount),0) AS total_collected FROM payments WHERE method <> 'Credit Transfer' {school_filter}",
            params,
        )
        collected = float((cursor.fetchone() or {}).get("total_collected") or 0)
        cursor.execute(
            f"SELECT COALESCE(SUM(COALESCE(balance, fee_balance,0)),0) AS pending FROM students WHERE 1=1 {school_filter}",
            params,
        )
        pending = float((cursor.fetchone() or {}).get("pending") or 0)
        cursor.execute(
            f"SELECT COALESCE(SUM(credit),0) AS credit FROM students WHERE 1=1 {school_filter}",
            params,
        )
        credit = float((cursor.fetchone() or {}).get("credit") or 0)
        collection_rate = round((collected / (collected + pending) * 100) if (collected + pending) else 0.0, 1)
        insights["summary"] = {
            "collected": collected,
            "pending": pending,
            "credit": credit,
            "collection_rate": collection_rate,
        }
    except Exception:
        pass
    finally:
        try:
            db.close()
        except Exception:
            pass

    try:
        log_event("view_ai_insights", detail="AI insights dashboard accessed")
    except Exception:
        pass

    return render_template(
        "ai.html",
        settings=settings,
        ai_configured=configured,
        ai_provider=provider,
        is_pro=is_pro_enabled(),
        upgrade_link=upgrade_url(),
        chats=chats,
        kb_status=kb,
        insights=insights,
    )


@ai_bp.route("/query", methods=["POST"])
def ai_query():
    """Conversational backend endpoint for AI queries.

    Expects JSON: { question: str, allow_actions?: bool, model?: str }
    Returns JSON with answer and optional data.
    """
    try:
        payload = request.get_json(silent=True) or {}
        # propagate school_id if available for tenant-aware actions
        if session and session.get("school_id"):
            payload.setdefault("school_id", session.get("school_id"))
        result = handle_query(payload)
        return jsonify({"ok": True, **result})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500


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
    try:
        db = _get_conn()
        try:
            chats = _list_chats(db)
            return jsonify({"ok": True, "chats": chats})
        finally:
            db.close()
    except Exception:
        # Fallback: no DB available -> empty list but OK
        return jsonify({"ok": True, "chats": []})


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
    model = (data.get("model") or "").strip() or None
    redo = bool(data.get("redo"))
    if not query:
        return jsonify({"ok": False, "answer": "Please provide a message."}), 400

    intent, entities = classify_intent(query)

    # Try DB-backed flow first; if DB not available, run stateless fallback
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
            return jsonify({"ok": True, "answer": answer, "chat_id": chat_id, "title": t if 't' in locals() else None})

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
            answer = "Analytics are disabled."
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

        # Unknown or general question -> use full chat with history (ChatGPT-like)
        # Build recent conversation history for coherence (limit to last 40 messages)
        cur.execute(
            "SELECT role, content FROM ai_messages WHERE chat_id=%s ORDER BY id ASC",
            (chat_id,),
        )
        rows = cur.fetchall() or []
        # Limit and optionally drop the last assistant for regenerate
        history = [{"role": r["role"], "content": r["content"]} for r in rows][-40:]
        if redo and history and history[-1].get('role') == 'assistant':
            history = history[:-1]
        answer = chat_anything(history, model=model or None)
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

    # Stateless fallback (no DB): general chat without persistence
    try:
        answer = chat_anything([{"role": "user", "content": query}], model=model or None)
    except Exception:
        answer = "AI not available. Please configure API key or build the index."
    return jsonify({"ok": True, "answer": answer, "chat_id": None})


@ai_bp.route("/api/chat_stream", methods=["GET"])
def ai_chat_stream():
    """Server-Sent Events stream for ChatGPT-like typing.

    Query params: q (message), chat_id (optional int)
    Sends an initial meta event with chat_id/title, then streams data chunks.
    """
    q = (request.args.get('q') or '').strip()
    model = (request.args.get('model') or '').strip() or None
    redo = (request.args.get('redo') or '').strip() in ('1','true','True','yes')
    if not q:
        return jsonify({"ok": False, "error": "q is required"}), 400

    def sse_format(event: str | None, data: str):
        if event:
            return f"event: {event}\n" + "data: " + data + "\n\n"
        return "data: " + data + "\n\n"

    @stream_with_context
    def generate():
        buf = []
        title = None
        now = datetime.now()
        db = None
        try:
            db = _get_conn()
            _ensure_ai_tables(db)
            cur = db.cursor(dictionary=True)
            chat_id = request.args.get('chat_id', type=int)
            # Create chat if needed
            if not chat_id:
                title = (q[:40] + ("..." if len(q) > 40 else "")).strip() or "New Chat"
                cur2 = db.cursor()
                sid = session.get("school_id") if session else None
                if sid:
                    cur2.execute("INSERT INTO ai_chats (title, created_at, updated_at, school_id) VALUES (%s,%s,%s,%s)", (title, now, now, sid))
                else:
                    cur2.execute("INSERT INTO ai_chats (title, created_at, updated_at) VALUES (%s,%s,%s)", (title, now, now))
                db.commit()
                chat_id = cur2.lastrowid
            else:
                # Validate ownership and get title
                sid = session.get("school_id") if session else None
                if sid:
                    cur.execute("SELECT id, title FROM ai_chats WHERE id=%s AND school_id=%s", (chat_id, sid))
                    row = cur.fetchone()
                    if not row:
                        yield sse_format('error', 'Not found')
                        return
                    title = row.get('title')
            # Store user message immediately
            cur.execute("INSERT INTO ai_messages (chat_id, role, content, created_at) VALUES (%s,%s,%s,%s)", (chat_id, 'user', q, now))
            db.commit()
            # Send meta so client knows chat_id/title
            meta = {"chat_id": chat_id, "title": title}
            yield sse_format('meta', json.dumps(meta))

            # Build history (limit last 40) and stream
            cur.execute("SELECT role, content FROM ai_messages WHERE chat_id=%s ORDER BY id ASC", (chat_id,))
            rows = cur.fetchall() or []
            history = [{"role": r["role"], "content": r["content"]} for r in rows][-40:]
            if redo and history and history[ -1 ].get('role') == 'assistant':
                history = history[:-1]
            for delta in chat_anything_stream(history, model=model or None):
                buf.append(delta)
                yield sse_format(None, delta)
            # Persist assistant reply at the end
            answer_full = ''.join(buf)
            cur.execute("INSERT INTO ai_messages (chat_id, role, content, created_at) VALUES (%s,%s,%s,%s)", (chat_id, 'assistant', answer_full, datetime.now()))
            cur.execute("UPDATE ai_chats SET updated_at=%s WHERE id=%s", (datetime.now(), chat_id))
            db.commit()
        except Exception as e:
            try:
                err = str(e)
            except Exception:
                err = 'stream_error'
            yield sse_format('error', err)
        finally:
            try:
                db and db.close()
            except Exception:
                pass
            yield sse_format('done', 'true')

    resp = Response(generate(), mimetype='text/event-stream')
    try:
        resp.headers['Cache-Control'] = 'no-cache'
        resp.headers['X-Accel-Buffering'] = 'no'
    except Exception:
        pass
    return resp
