import re
from pathlib import Path
p = Path('templates/payments.html')
s = p.read_text(encoding='utf-8')
# Find the <a href="{{ url_for('export_payments') }}" ... </a> block
m = re.search(r"<a href=\"\{\{ url_for\('export_payments'\) \}\}\"[\s\S]*?</a>", s)
if m:
    rep = (
        "{% if plan_status and plan_status.plan_code != 'FREE' %}\n"
        "        <a href=\"{{ url_for('export_payments') }}\" \n"
        "           class=\"inline-flex items-center gap-2 px-4 py-2 rounded-xl text-white text-sm font-medium bg-gradient-to-r from-green-600 to-green-500 hover:from-green-700 hover:to-green-600 shadow-md hover:shadow-lg transform hover:-translate-y-0.5 transition\">\n"
        "          <i data-lucide=\"download\" class=\"w-4 h-4\"></i> Export CSV\n"
        "        </a>\n"
        "{% else %}\n"
        "        <a href=\"/admin/monetization\" \n"
        "           class=\"inline-flex items-center gap-2 px-4 py-2 rounded-xl text-white text-sm font-medium bg-gradient-to-r from-green-600 to-green-500 opacity-60 cursor-not-allowed\" title=\"Upgrade to Pro to export CSV\">\n"
        "          <i data-lucide=\"download\" class=\"w-4 h-4\"></i> Export CSV\n"
        "        </a>\n"
        "{% endif %}"
    )
    s = s[:m.start()] + rep + s[m.end():]
    p.write_text(s, encoding='utf-8')
    print('payments.html updated')
else:
    print('payments.html: export link not found')
