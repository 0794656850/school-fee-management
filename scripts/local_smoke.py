import sys
from flask import Response

try:
    import app as appmod
except Exception as e:
    print("import_error", str(e))
    sys.exit(1)

app = appmod.app
client = app.test_client()

def check(path: str, expect: int = 200):
    r = client.get(path)
    print(path, r.status_code)
    return r.status_code

if __name__ == "__main__":
    codes = [
        check('/auth/login'),
        check('/g/login'),
        check('/portal/invalid', 403),
    ]
    # Exit 0 if all are reachable (403 is acceptable for invalid token)
    ok = (codes[0] == 200) and (codes[1] == 200) and (codes[2] in (401,403))
    sys.exit(0 if ok else 2)
