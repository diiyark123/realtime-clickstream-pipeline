import os
import sys
import time
from pathlib import Path

# PySpark converts collected timestamps to the local time zone; pin to UTC so tests pass on any machine.
os.environ["TZ"] = "UTC"
time.tzset()

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "producer"))
sys.path.insert(0, str(ROOT / "spark"))
