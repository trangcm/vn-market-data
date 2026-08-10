"""Which source actually answers, and do they agree? Run this first when something looks
wrong, and paste the output into the issue.

    python examples/diagnose_sources.py            # default sample
    python examples/diagnose_sources.py HPG VCB    # your own symbols

It bypasses the store and the fallback chain entirely and asks **every** source directly,
because the whole point of the chain is that it hides which source answered — useful in
production, useless when you are trying to find out why a number is wrong.

Two things it checks:

- **Coverage** — which sources can answer for each capability, and which abstain. A
  capability with no source left is why the data is missing.
- **Agreement** — where two sources both return OHLCV, the overlapping closes and volumes
  must match within tolerance *and at the same scale*. The ×1000 trap is the one that
  matters: VCI quotes equities in thousands, and a source that forgets to normalize looks
  perfectly plausible until it is compared against another one.

Network required. Nothing is written to any database.
"""
import sys
from pathlib import Path

# Runs from a checkout without installing; from an installed package the guard is a
# no-op, since there is then no enclosing `vn_market_data/` directory to point at.
_root = Path(__file__).resolve().parents[2]
if (_root / "vn_market_data").is_dir():
    sys.path.insert(0, str(_root))

from vn_market_data import NotSupported, SourceUnavailable, build_sources  # noqa: E402

# A general company, a bank (different statement shape), a small cap, and the index.
DEFAULT_SAMPLE = [("HPG", False), ("VCB", False), ("REE", False), ("VNINDEX", True)]
START, END = "2026-03-01", "2026-06-23"   # a closed window every source should cover
CLOSE_TOL = 0.005   # 0.5% — providers differ slightly on adjusted closes
VOL_TOL   = 0.02    # 2%

CAPABILITIES = [
    ("get_ohlcv", lambda s, sym: s.get_ohlcv(sym, START, END)),
    ("get_index_live", lambda s, sym: s.get_index_live("VNINDEX")),
    ("get_board", lambda s, sym: s.get_board([sym])),
    ("get_statements", lambda s, sym: s.get_statements(sym)),
    ("get_events", lambda s, sym: s.get_events(sym)),
    ("get_market_turnover", lambda s, sym: s.get_market_turnover("VNINDEX", START, END)),
    ("get_index_constituents", lambda s, sym: s.get_index_constituents("VN30")),
]


def probe(sources, symbol):
    """Ask every source for every capability. NotSupported is a design decision, not a
    fault — it is how a source abstains so the next one gets the call."""
    print(f"\n── capabilities (probed with {symbol}) " + "─" * 30)
    for cap, call in CAPABILITIES:
        marks = []
        for src in sources:
            try:
                result = call(src, symbol)
            except NotSupported:
                marks.append(f"{src.name}:—")
            except SourceUnavailable as e:
                marks.append(f"{src.name}:DOWN({type(e).__name__})")
            except Exception as e:                       # noqa: BLE001 - a diagnostic
                marks.append(f"{src.name}:ERROR({type(e).__name__}: {e})")
            else:
                n = len(result) if hasattr(result, "__len__") else (1 if result else 0)
                marks.append(f"{src.name}:{'ok' if result else 'empty'}({n})")
        # `empty` still counts as an answer — "no session in progress" is one. The line
        # to worry about is the one where every source abstained, failed, or errored.
        reached = any(":ok" in m or ":empty" in m for m in marks)
        print(f"  {cap:24} {'  '.join(marks)}{'' if reached else '   ← no source left'}")


def fetch_ohlcv(src, sym, is_index):
    try:
        return {r["date"]: r for r in src.get_ohlcv(sym, START, END, is_index=is_index)}
    except NotSupported:
        return None
    except Exception as e:                               # noqa: BLE001 - a diagnostic
        print(f"  {src.name}: ERROR {type(e).__name__}: {e}")
        return {}


def compare(sources, sample):
    """Cross-check the overlapping bars of every pair of sources that answered."""
    ok_all = True
    for sym, is_index in sample:
        print(f"\n── {sym} {START}..{END} " + "─" * 40)
        series = {}
        for src in sources:
            rows = fetch_ohlcv(src, sym, is_index)
            if rows is None:
                continue                                 # abstained; not its job
            series[src.name] = rows
            print(f"  {src.name:10} {len(rows):4} bars")

        names = [n for n, r in series.items() if r]
        if len(names) < 2:
            print("  only one source answered — nothing to cross-check")
            continue

        head = names[0]
        for other in names[1:]:
            a, b = series[head], series[other]
            common = sorted(set(a) & set(b))
            if not common:
                print(f"  {head} vs {other}: NO OVERLAP — FAIL")
                ok_all = False
                continue

            close_bad = vol_bad = 0
            worst = 0.0
            for dt in common:
                ac, bc = a[dt]["close"], b[dt]["close"]
                if bc:
                    rel = abs(ac - bc) / abs(bc)
                    worst = max(worst, rel)
                    close_bad += rel > CLOSE_TOL
                av, bv = a[dt].get("volume") or 0, b[dt].get("volume") or 0
                if not is_index and bv:
                    vol_bad += abs(av - bv) / abs(bv) > VOL_TOL

            # A ×1000 scale slip blows past CLOSE_TOL on every single bar, so say so
            # outright rather than reporting 99900% and letting the reader work it out.
            hint = ""
            if worst > 0.5:
                last = common[-1]
                ratio = a[last]["close"] / b[last]["close"] if b[last]["close"] else 0
                hint = f"   ⚠ possible SCALE mismatch ({head}/{other} ≈ {ratio:.4g})"
            ok = not close_bad and not vol_bad
            ok_all &= ok
            print(f"  {head} vs {other}: overlap={len(common):3} close_mismatch={close_bad} "
                  f"vol_mismatch={vol_bad} worst={worst * 100:.3f}% "
                  f"{'PASS' if ok else 'FAIL'}{hint}")
    return ok_all


def main(argv):
    sources = build_sources()
    print("chain:", " → ".join(s.name for s in sources))
    if len(sources) < 2:
        print("  (only one source installed — try `pip install vn-market-data[vci]`)")

    sample = [(s.upper(), s.upper().endswith("INDEX")) for s in argv] or DEFAULT_SAMPLE
    probe(sources, sample[0][0])
    ok = compare(sources, sample)
    print("\nRESULT:", "sources agree" if ok else "DISAGREEMENTS — see above")
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
