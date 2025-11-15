from pathlib import Path
p = Path('templates/reports.html')
s = p.read_text(encoding='utf-8')
if 'School Profile (Word)' not in s:
    add = "\n  <div class=\"bg-white rounded-2xl p-6 shadow-sm border border-gray-100\">\n    <h3 class=\"text-lg font-semibold text-gray-800 flex items-center gap-2\"><i data-lucide=\"file\"></i> School Profile (Word)</h3>\n    <p class=\"text-sm text-gray-600 mt-1\">Per-school profile and fee summary as a Word document.</p>\n    <div class=\"mt-4\">\n      {% if plan_status and plan_status.plan_code != 'FREE' %}\n      <a href=\"{{ url_for('export_school_profile_docx') }}\" class=\"inline-flex items-center gap-2 px-4 py-2 rounded-xl bg-indigo-600 text-white hover:bg-indigo-700\"><i data-lucide=\"download\"></i> Download .docx</a>\n      {% else %}\n      <a href=\"{{ url_for('monetization.index') }}\" class=\"inline-flex items-center gap-2 px-4 py-2 rounded-xl bg-gray-200 text-gray-600 cursor-not-allowed\" title=\"Upgrade to Pro to export\">Download .docx</a>\n      {% endif %}\n    </div>\n  </div>\n"
    # Insert before the Full Fee Report section
    idx = s.find('Full Fee Report')
    if idx != -1:
        # find the container start for that card
        card_start = s.rfind('<div', 0, idx)
        if card_start != -1:
            s = s[:card_start] + add + s[card_start:]
    else:
        s += add
    p.write_text(s, encoding='utf-8')
    print('Added School Profile (Word) button to Reports page')
else:
    print('Reports page already has School Profile button')
