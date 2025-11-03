import json
import app as appmod
app = appmod.app

with app.test_client() as c:
    r = c.get('/healthz')
    print('status', r.status_code)
    try:
        data = r.get_json()
        print('json', json.dumps(data))
    except Exception as e:
        print('json_error', e)