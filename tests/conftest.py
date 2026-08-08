"""Make the package importable from its own repo root.

These tests live inside the package so they travel with it when it is split out. The
directory above `vn_market_data/` is the import root in both places — market-intel's
own directory here, the repository root there — so one line covers both.
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
