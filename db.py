"""Connection and schema ownership for the market-data cache.

The package owns the ``md_*`` tables. It deliberately does **not** own the
connection: a host application that already has a SQLite database has its own path,
pragmas and lifecycle, and opening a second one beside it would split the cache and
double the WAL. Call :func:`set_connection_factory` once at startup to hand one over.

With no factory installed a default one opens ``$VN_MARKET_DATA_DB`` (or
``./vn_market_data.db``) and creates the schema on first use, so a bare
``pip install`` works with no setup at all.
"""
import os
import sqlite3
import threading
from pathlib import Path

_SCHEMA = Path(__file__).with_name("schema.sql")

_DB_ENV = "VN_MARKET_DATA_DB"
_DB_DEFAULT = "vn_market_data.db"

_factory = None
_initialised: set[str] = set()
_lock = threading.Lock()


def set_connection_factory(fn) -> None:
    """Route every store read and write through *fn*, a zero-arg callable returning
    an open ``sqlite3.Connection``.

    The host keeps ownership: the package borrows a connection, uses it, and closes
    it. A host that installs a factory is also responsible for calling
    :func:`init_schema` once — it knows when its own schema is ready, and the
    package must not guess.
    """
    global _factory
    _factory = fn


def get_connection_factory():
    """The installed factory, or ``None`` if the built-in default is in use.

    Here so a caller can install one temporarily and put back whatever was there —
    a test fixture, mostly — without reaching into module state.
    """
    return _factory


def _default_connect() -> sqlite3.Connection:
    path = Path(os.getenv(_DB_ENV, _DB_DEFAULT))
    if path.parent != Path(""):
        path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(path, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    # WAL admits many readers but one writer; wait out a held write lock rather than
    # failing the call outright with "database is locked".
    conn.execute("PRAGMA busy_timeout=10000")
    # Zero-config: a caller who has never heard of the schema still gets a working
    # store. Once per path per process — `CREATE TABLE IF NOT EXISTS` is cheap but
    # not free, and connect() runs on every adapter call.
    key = str(path.resolve())
    if key not in _initialised:
        with _lock:
            if key not in _initialised:
                init_schema(conn)
                _initialised.add(key)
    return conn


def connect() -> sqlite3.Connection:
    """An open connection from the installed factory, or the default one."""
    return (_factory or _default_connect)()


def init_schema(conn: sqlite3.Connection) -> None:
    """Create the ``md_*`` tables and apply in-place migrations. Idempotent."""
    conn.executescript(_SCHEMA.read_text())
    _migrate(conn)
    conn.commit()


def _migrate(conn: sqlite3.Connection) -> None:
    """Columns added after a table already existed — `CREATE TABLE IF NOT EXISTS`
    will not add them to a live table, so they go here."""
    bcols = {r[1] for r in conn.execute("PRAGMA table_info(md_board)")}
    if bcols and "traded_value" not in bcols:
        # Session turnover from the price board (full VND) — snapshots taken before
        # this column existed stay NULL; consumers treat NULL as "not covered".
        conn.execute("ALTER TABLE md_board ADD COLUMN traded_value REAL")
    if bcols and "traded_volume" not in bcols:
        conn.execute("ALTER TABLE md_board ADD COLUMN traded_volume REAL")
