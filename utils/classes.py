from __future__ import annotations

import re
from typing import Optional


_EXPLICIT_MAP = {
    # Early years / nursery synonyms
    "baby": "nursery",
    "nursery": "pp1",
    "pre-primary 1": "pp2",
    "pp1": "pp2",
    "pp2": "grade 1",
    "kg1": "kg2",
    "kg2": "grade 1",

    # Primary/Junior
    "std 1": "std 2",
    "std 2": "std 3",
    "std 3": "std 4",
    "std 4": "std 5",
    "std 5": "std 6",
    "std 6": "std 7",
    "std 7": "std 8",
    "std 8": "form 1",

    "class 1": "class 2",
    "class 2": "class 3",
    "class 3": "class 4",
    "class 4": "class 5",
    "class 5": "class 6",
    "class 6": "class 7",
    "class 7": "class 8",
    "class 8": "form 1",

    # High school
    "form 1": "form 2",
    "form 2": "form 3",
    "form 3": "form 4",
    # form 4 -> graduation (None)
}


def promote_class_name(current: Optional[str]) -> Optional[str]:
    """Return the next class label for a given class name.

    The mapping covers common Kenyan naming styles. If a class is the terminal
    level (e.g., "Form 4"), returns None to indicate graduation.
    """
    if not current:
        return current
    text = str(current).strip().lower()
    if not text:
        return current

    # Fast path via explicit mapping
    if text in _EXPLICIT_MAP:
        return _EXPLICIT_MAP[text].title()
    if text == "form 4":
        return None

    # Generic patterns
    m = re.match(r"^(grade|class|std)\s*(\d{1,2})$", text)
    if m:
        kind, num = m.group(1), int(m.group(2))
        if num >= 8:
            # Primary 8 moves to Form 1
            return "Form 1"
        return f"{kind.title()} {num + 1}".replace("Std", "Std")

    m = re.match(r"^form\s*(\d{1,2})$", text)
    if m:
        num = int(m.group(1))
        if num >= 4:
            return None
        return f"Form {num + 1}"

    m = re.match(r"^grade\s*(\d{1,2})$", text)
    if m:
        num = int(m.group(1))
        if num >= 8:
            return "Form 1"
        return f"Grade {num + 1}"

    # Try trailing number increment (e.g., "P{4}")
    m = re.match(r"^(.*?)(\d{1,2})$", text)
    if m:
        prefix, num = m.group(1).strip(), int(m.group(2))
        if prefix in ("f", "form") and num >= 4:
            return None
        return f"{prefix} {num + 1}".strip().title()

    # Unknown format: keep as-is
    return current

