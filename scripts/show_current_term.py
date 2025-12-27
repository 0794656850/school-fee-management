from pathlib import Path
text = Path('templates/base.html').read_text(encoding='utf-8')
start = text.index('<span id="currentTermChip"')
end = text.index('</span>', start)
print(repr(text[start:end+7]))
