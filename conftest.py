import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent
SRC = ROOT / "src"

if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

existing = os.environ.get("PYTHONPATH")
if existing:
    paths = [p for p in existing.split(os.pathsep) if p]
    if str(SRC) not in paths:
        os.environ["PYTHONPATH"] = str(SRC) + os.pathsep + existing
else:
    os.environ["PYTHONPATH"] = str(SRC)
