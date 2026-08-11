"""Make the package importable when the tests are run straight from a checkout.

These tests live inside the package so they travel with it when it is split out, and the
two layouts do not share an import root. In the development layout `vn_market_data/` is
a subdirectory of the host application, so the directory two levels up is the import
root and the insert below does the work. In the published repo the package *is* the repo
root (`package-dir = {"vn_market_data" = "."}`) — there is no enclosing `vn_market_data/`
directory to point at, the import comes from the editable install the README asks for,
and the guard makes this a deliberate no-op rather than a path that silently misses.
"""
import sys
from pathlib import Path

_root = Path(__file__).resolve().parents[2]
if (_root / "vn_market_data").is_dir():
    sys.path.insert(0, str(_root))
