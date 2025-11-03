from app import app

with app.test_client() as c:
    # Set session to pass auth guard
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
