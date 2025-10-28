import mysql.connector, json
keys = [
 'DARAJA_ENV','DARAJA_CONSUMER_KEY','DARAJA_CONSUMER_SECRET','DARAJA_SHORT_CODE','DARAJA_PASSKEY','DARAJA_CALLBACK_URL','DARAJA_ACCOUNT_REF','DARAJA_TRANSACTION_DESC']
try:
    db = mysql.connector.connect(host='localhost', user='root', password='9133orerO', database='school_fee_db')
    cur = db.cursor()
    cur.execute('SHOW TABLES LIKE %s', ('app_settings',))
    if not cur.fetchone():
        print('NO_TABLE')
    else:
        placeholders = ','.join(['%s']*len(keys))
        cur.execute(f"SELECT `key`,`value` FROM app_settings WHERE `key` IN ({placeholders})", tuple(keys))
        rows = dict(cur.fetchall()) if cur.rowcount else {}
        print(json.dumps(rows))
    db.close()
except Exception as e:
    print('ERROR', e)
