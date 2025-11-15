from pathlib import Path
p = Path('app.py')
s = p.read_text(encoding='utf-8')
# 1) Insert top_credit and expected/collected queries before db.close()
anchor_old = "method_rows = cur.fetchall() or []\n\n    db.close()\n"
anchor_new = (
    "method_rows = cur.fetchall() or []\n\n"
    "    # Top credit students\n"
    "    cur.execute(\"SELECT name, class_name, credit FROM students WHERE school_id=%s AND credit > 0 ORDER BY credit DESC LIMIT 10\", (sid,))\n"
    "    top_credit = cur.fetchall() or []\n\n"
    "    # Expected fee by term (items + legacy), Collected by term\n"
    "    cur.execute(\"""
        SELECT sti.year AS year, sti.term AS term, COALESCE(SUM(sti.amount),0) AS total
        FROM student_term_fee_items sti
        JOIN students s ON s.id = sti.student_id
        WHERE s.school_id=%s
        GROUP BY sti.year, sti.term
    """\", (sid,))\n"
    "    items_rows = cur.fetchall() or []\n"
    "    cur.execute(\"""
        SELECT tf.year AS year, tf.term AS term, COALESCE(SUM(tf.fee_amount),0) AS total
        FROM term_fees tf
        JOIN students s ON s.id = tf.student_id
        WHERE s.school_id=%s
        GROUP BY tf.year, tf.term
    """\", (sid,))\n"
    "    legacy_rows = cur.fetchall() or []\n"
    "    exp_map = {}\n"
    "    for r in items_rows:\n"
    "        key = (int(r.get('year') or 0), int(r.get('term') or 0)); exp_map[key] = float(r.get('total') or 0)\n"
    "    for r in legacy_rows:\n"
    "        key = (int(r.get('year') or 0), int(r.get('term') or 0)); exp_map[key] = exp_map.get(key,0.0) + float(r.get('total') or 0)\n"
    "    cur.execute(\"SELECT year, term, COALESCE(SUM(amount),0) AS total FROM payments WHERE method <> 'Credit Transfer' AND school_id=%s GROUP BY year, term ORDER BY year, term\", (sid,))\n"
    "    coll_rows = cur.fetchall() or []\n"
    "    exp_table = []\n"
    "    for r in coll_rows:\n"
    "        key = (int(r.get('year') or 0), int(r.get('term') or 0))\n"
    "        exp = float(exp_map.get(key, 0.0))\n"
    "        col = float(r.get('total') or 0)\n"
    "        exp_table.append({'year': key[0], 'term': key[1], 'expected': exp, 'collected': col, 'gap': exp - col})\n\n"
    "    db.close()\n"
)
if anchor_old in s:
    s = s.replace(anchor_old, anchor_new)

# 2) Insert logo after heading line
logo_anchor = "doc.add_heading(f\"{school_name} - Fee Report\", 0)\n"
if logo_anchor in s and 'add_picture(' not in s[ s.find(logo_anchor): s.find(logo_anchor)+200 ]:
    logo_code = (
        logo_anchor +
        "    try:\n"
        "        import os\n"
        "        logo_rel = get_setting('SCHOOL_LOGO_URL') or app.config.get('LOGO_PRIMARY')\n"
        "        if logo_rel:\n"
        "            lp = logo_rel\n"
        "            if not os.path.isabs(lp):\n"
        "                lp = os.path.join(app.root_path, 'static', lp)\n"
        "            if os.path.exists(lp):\n"
        "                doc.add_picture(lp, width=Inches(1.2))\n"
        "    except Exception:\n"
        "        pass\n"
    )
    s = s.replace(logo_anchor, logo_code)

# 3) Insert Top Credit and Expected vs Collected sections before return marker
ins_marker = "    # Return .docx\n"
if ins_marker in s and 'Top Credit' not in s:
    add_sections = (
        "    # Top Credit\n"
        "    doc.add_heading('Top Credit', level=1)\n"
        "    tct = doc.add_table(rows=1, cols=3); tct.style = 'Light Grid'\n"
        "    for j,h in enumerate(['Student','Class','Credit (KES)']): tct.rows[0].cells[j].text = h\n"
        "    for c in (top_credit or []):\n"
        "        r = tct.add_row().cells\n"
        "        r[0].text = str(c.get('name') or '')\n"
        "        r[1].text = str(c.get('class_name') or '')\n"
        "        r[2].text = str(round(float(c.get('credit') or 0),2))\n\n"
        "    # Expected vs Collected by Term\n"
        "    doc.add_heading('Expected vs Collected (by term)', level=1)\n"
        "    ect = doc.add_table(rows=1, cols=5); ect.style = 'Light Grid'\n"
        "    for j,h in enumerate(['Year','Term','Expected (KES)','Collected (KES)','Gap (KES)']): ect.rows[0].cells[j].text = h\n"
        "    for r in (exp_table or []):\n"
        "        row = ect.add_row().cells\n"
        "        row[0].text = str(r.get('year') or '')\n"
        "        row[1].text = str(r.get('term') or '')\n"
        "        row[2].text = str(round(float(r.get('expected') or 0),2))\n"
        "        row[3].text = str(round(float(r.get('collected') or 0),2))\n"
        "        row[4].text = str(round(float(r.get('gap') or 0),2))\n\n"
    )
    s = s.replace(ins_marker, add_sections + ins_marker)

# 4) Add email route
if '/email_school_profile_docx' not in s:
    email_block = '''

@app.route("/email_school_profile_docx", methods=["POST","GET"])
def email_school_profile_docx():
    if not is_pro_enabled(app):
        try:
            flash('Exports are available in Pro. Please upgrade.', 'warning')
        except Exception:
            pass
        return redirect(url_for('monetization.index'))
    from utils.settings import get_setting
    school_email = get_setting('SCHOOL_EMAIL') or app.config.get('SCHOOL_EMAIL')
    if not school_email:
        try:
            flash('School email not configured in settings.', 'warning')
        except Exception:
            pass
        return redirect(url_for('reports'))
    # Reuse generator by calling the export function and grabbing bytes
    resp = export_school_profile_docx()
    if getattr(resp, 'status_code', 200) != 200:
        return resp
    data = resp.get_data()
    try:
        from flask_mail import Message
        from extensions import mail
        msg = Message(subject='School Fee Report (.docx)', sender=app.config.get('MAIL_SENDER'), recipients=[school_email])
        msg.body = 'Attached is your fee report Word document.'
        msg.attach('school_profile.docx', 'application/vnd.openxmlformats-officedocument.wordprocessingml.document', data)
        mail.send(msg)
        try:
            flash('Report emailed to ' + school_email, 'success')
        except Exception:
            pass
    except Exception as e:
        try:
            flash('Failed to send email: ' + str(e), 'error')
        except Exception:
            pass
    return redirect(url_for('reports'))
'''
    s += email_block

p.write_text(s, encoding='utf-8')
print('Enhanced docx export and added email route')
