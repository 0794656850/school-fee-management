from __future__ import annotations

import subprocess
import sys
from pathlib import Path


def main() -> None:
    rasadir = Path(__file__).resolve().parent.parent / "rasa_bot"
    cmd = ["rasa", "train"]
    result = subprocess.run(cmd, cwd=rasadir)
    sys.exit(result.returncode)


if __name__ == "__main__":
    main()
