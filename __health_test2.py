import app as appmod
app = appmod.app

with app.test_client() as c:
    r = c.get('/healthz', follow_redirects=False)
    print('status', r.status_code)
    print('location', r.headers.get('Location'))
    print('data', r.data[:200])