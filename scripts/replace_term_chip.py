# -*- coding: utf-8 -*-
from pathlib import Path
path = Path('templates/base.html')
text = path.read_text(encoding='utf-8')
old = "        <span id=\"currentTermChip\" class=\"hidden md:inline-flex items-center gap-2 text-xs px-3 py-1.5 rounded-full border border-indigo-200 bg-indigo-50 text-indigo-700\">\n          <i data-lucide=\"calendar\"></i>\n          <span id=\"currentTermText\">{{ CURRENT_YEAR or '�' }} � Term {{ CURRENT_TERM or '�' }}</span>\n        </span>\n"
new = "        <div id=\"currentTermChip\" class=\"hidden md:inline-flex items-center gap-2 text-xs px-3 py-1.5 rounded-full border border-indigo-200 bg-indigo-50 text-indigo-700\">\n          <span class=\"inline-flex h-6 w-6 items-center justify-center rounded-full bg-white text-indigo-600 shadow-sm\">\n            <i data-lucide=\"calendar\" class=\"w-4 h-4\"></i>\n          </span>\n          <span id=\"currentTermText\">{{ CURRENT_YEAR or '2027' }} · Term {{ CURRENT_TERM or '1' }}</span>\n        </div>\n"
if old not in text:
    raise SystemExit('old block not found')
text = text.replace(old, new, 1)
path.write_text(text, encoding='utf-8')
