from pathlib import Path
p = Path('app.py')
s = p.read_text(encoding='utf-8')
# 1) Insert extra queries before db.close() in export_school_profile_docx
anchor = "paid_map = {r['class']: float(r.get('total_paid') or 0) for r in (cur.fetchall() or [])}\n\n    db.close()\n"
if anchor in s:
    extra = (
        "paid_map = {r['class']: float(r.get('total_paid') or 0) for r in (cur.fetchall() or [])}\n\n"
        "    # Extra sections: top debtors, recent payments, monthly totals, method breakdown\n"
        "    cur.execute(\"SELECT name, class_name, COALESCE(balance, fee_balance) AS balance FROM students WHERE school_id=%s ORDER BY balance DESC LIMIT 10\", (sid,))\n"
        "    top_debtors = cur.fetchall() or []\n"
        "    cur.execute(\"SELECT p.date, s.name, s.class_name, p.amount, p.method, p.reference FROM payments p JOIN students s ON s.id=p.student_id WHERE p.school_id=%s ORDER BY p.date DESC LIMIT 10\", (sid,))\n"
        "    recent_payments = cur.fetchall() or []\n"
        "    cur.execute(\"SELECT DATE_FORMAT(date, '%Y-%m') AS ym, SUM(amount) AS total FROM payments WHERE method <> 'Credit Transfer' AND school_id=%s GROUP BY DATE_FORMAT(date, '%Y-%m') ORDER BY ym DESC LIMIT 12\", (sid,))\n"
        "    monthly_rows = list(reversed(cur.fetchall() or []))\n"
        "    cur.execute(\"SELECT method, SUM(amount) AS total FROM payments WHERE method <> 'Credit Transfer' AND school_id=%s GROUP BY method ORDER BY total DESC\", (sid,))\n"
        "    method_rows = cur.fetchall() or []\n\n"
        "    db.close()\n"
    )
    s = s.replace(anchor, extra)

# 2) Inject document sections before "# Return .docx"
ins_marker = "    # Return .docx\n"
if ins_marker in s and 'Top Debtors' not in s:
    add = (
        "    # Top Debtors\n"
        "    doc.add_heading('Top Debtors', level=1)\n"
        "    tdt = doc.add_table(rows=1, cols=3); tdt.style = 'Light Grid'\n"
        "    for j,h in enumerate(['Student','Class','Balance (KES)']): tdt.rows[0].cells[j].text = h\n"
        "    for d in (top_debtors or []):\n"
        "        r = tdt.add_row().cells\n"
        "        r[0].text = str(d.get('name') or '')\n"
        "        r[1].text = str(d.get('class_name') or '')\n"
        "        r[2].text = str(round(float(d.get('balance') or 0),2))\n\n"
        "    # Recent Payments\n"
        "    doc.add_heading('Recent Payments', level=1)\n"
        "    rpt = doc.add_table(rows=1, cols=6); rpt.style = 'Light Grid'\n"
        "    for j,h in enumerate(['Date','Student','Class','Amount','Method','Ref']): rpt.rows[0].cells[j].text = h\n"
        "    for pmt in (recent_payments or []):\n"
        "        r = rpt.add_row().cells\n"
        "        r[0].text = str(pmt.get('date') or '')\n"
        "        r[1].text = str(pmt.get('name') or '')\n"
        "        r[2].text = str(pmt.get('class_name') or '')\n"
        "        r[3].text = str(round(float(pmt.get('amount') or 0),2))\n"
        "        r[4].text = str(pmt.get('method') or '')\n"
        "        r[5].text = str(pmt.get('reference') or '')\n\n"
        "    # Charts (optional)\n"
        "    try:\n"
        "        import matplotlib\n"
        "        matplotlib.use('Agg')\n"
        "        import matplotlib.pyplot as plt\n"
        "        from io import BytesIO as _BIO\n"
        "        # Monthly collections chart\n"
        "        if monthly_rows:\n"
        "            fig, ax = plt.subplots(figsize=(6,2.2))\n"
        "            labels = [r.get('ym') for r in monthly_rows]\n"
        "            vals = [float(r.get('total') or 0) for r in monthly_rows]\n"
        "            ax.plot(labels, vals, marker='o', color='#4f46e5'); ax.set_title('Monthly Collections (last 12)'); ax.tick_params(axis='x', rotation=45, labelsize=7); ax.grid(alpha=.2)\n"
        "            buf = _BIO(); plt.tight_layout(); fig.savefig(buf, format='png', dpi=200); plt.close(fig); buf.seek(0)\n"
        "            doc.add_picture(buf, width=Inches(6))\n"
        "        # Method breakdown pie\n"
        "        if method_rows:\n"
        "            fig, ax = plt.subplots(figsize=(5,3.2))\n"
        "            labels = [str(r.get('method') or 'N/A') for r in method_rows]\n"
        "            vals = [float(r.get('total') or 0) for r in method_rows]\n"
        "            if sum(vals) <= 0:\n"
        "                vals = [1]; labels = ['No Data']\n"
        "            wedges, texts, autotexts = ax.pie(vals, labels=None, autopct=lambda p: f'{p:.0f}%' if p >= 5 else '', startangle=90, pctdistance=.72, labeldistance=1.15, wedgeprops={'linewidth':1,'edgecolor':'#fff'})\n"
        "            ax.set_title('Method Breakdown')\n"
        "            ax.legend(wedges, labels, title='Method', loc='center left', bbox_to_anchor=(1.02, .5), frameon=False)\n"
        "            buf2 = _BIO(); plt.tight_layout(); fig.savefig(buf2, format='png', dpi=200, bbox_inches='tight'); plt.close(fig); buf2.seek(0)\n"
        "            doc.add_picture(buf2, width=Inches(5.5))\n"
        "    except Exception:\n"
        "        pass\n\n"
    )
    s = s.replace(ins_marker, add + ins_marker)

p.write_text(s, encoding='utf-8')
print('export_school_profile_docx extended with debtors, recent payments, and charts')
