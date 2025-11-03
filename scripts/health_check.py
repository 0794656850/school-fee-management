import json
import sys
try:
    import app as appmod
    get_db_connection = appmod.get_db_connection
    ok = True
    db_ok = False
    try:
        conn = get_db_connection()
        try:
            cur = conn.cursor()
            cur.execute("SELECT 1")
            cur.fetchone()
            db_ok = True
        finally:
            conn.close()
    except Exception as e:
        db_ok = False
        ok = False
        err = str(e)
    out = {"ok": ok, "db": db_ok}
    if not ok:
        out["error"] = err
    print(json.dumps(out))
    sys.exit(0 if ok else 1)
except Exception as e:
    print(json.dumps({"ok": False, "error": str(e)}))
    sys.exit(1)