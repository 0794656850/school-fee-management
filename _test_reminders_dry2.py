import sys, types
# Stub mysql.connector
mysql_module = types.ModuleType('mysql')
connector_module = types.ModuleType('mysql.connector')
class _Dummy:
    def cursor(self, *a, **k): return self
    def execute(self, *a, **k): pass
    def fetchone(self): return None
    def fetchall(self): return []
    def commit(self): pass
    def rollback(self): pass
    def close(self): pass

def connect(*a, **k): return _Dummy()
connector_module.connect = connect
mysql_module.connector = connector_module
sys.modules['mysql'] = mysql_module
sys.modules['mysql.connector'] = connector_module

# Stub flask_sqlalchemy and flask_migrate (not used in this test)
fsq = types.ModuleType('flask_sqlalchemy')
class SQLAlchemy: pass
fsq.SQLAlchemy = SQLAlchemy
sys.modules['flask_sqlalchemy'] = fsq
fmig = types.ModuleType('flask_migrate')
class Migrate: pass
fmig.Migrate = Migrate
sys.modules['flask_migrate'] = fmig

from app import app
with app.test_client() as c:
    with c.session_transaction() as sess:
        sess['user_logged_in'] = True
        sess['school_id'] = 1
        sess['username'] = 'tester'
    r = c.post('/reminders/test_email?dry=1', json={'to':'test@example.com','message':'Test via dry-run'})
    print('Status:', r.status_code)
    try:
        print('JSON:', r.get_json())
    except Exception:
        print('Body:', r.data[:200])
