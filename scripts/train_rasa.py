from __future__ import annotations

import subprocess
import sys
from pathlib import Path


def main() -> None:
    rasadir = Path(__file__).resolve().parent.parent / "rasa_bot"
    commands = (
        ["rasa", "train"],
        [sys.executable, "-m", "rasa", "train"],
    )
    last_error: Exception | None = None
    last_returncode: int | None = None
    for cmd in commands:
        try:
            result = subprocess.run(cmd, cwd=rasadir)
            if result.returncode == 0:
                sys.exit(0)
            last_returncode = result.returncode
            continue
        except FileNotFoundError as exc:
            last_error = exc
            continue
    print(
        "Could not run Rasa training. Install Rasa in this Python environment, then rerun `python scripts/train_rasa.py`.",
        file=sys.stderr,
    )
    if last_error:
        print(str(last_error), file=sys.stderr)
    sys.exit(last_returncode or 1)


if __name__ == "__main__":
    main()
