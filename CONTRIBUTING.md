# Contributing

Thanks for looking. Before you spend time on a patch, there is one thing about this
repository you need to know, because it is unusual and it changes what a contribution
looks like.

## This repository is a mirror

`vn-market-data` is developed inside a larger private application and published here by
`git subtree split`. **The split is one-way.** This repo is a projection of the upstream
directory; nothing is ever pulled back from it.

The practical consequences:

- **A pull request opened here cannot be merged here.** Not "will not" — the next
  release regenerates this branch from upstream history and would run straight over the
  merge commit.
- **A commit pushed here is invisible upstream** and disappears at the next release.
- **Your commit will not appear in the published history**, even when your change ships.
  The published commits are the upstream ones, authored there.

This is not a judgement about outside contributions. It is the cost of having exactly
one copy of the code — two-way sync means two copies, and two copies drift.

## So how do I contribute?

**Open an issue.** That is the whole answer, and it is a real channel — issues are read.

- **Bugs:** include the output of `python examples/diagnose_sources.py <SYMBOL>`. It
  fetches the same symbol from every source in the chain and prints where they disagree,
  which is the single most useful thing to paste. Include your Python version and
  whether you installed the `[vci]` extra.
- **Patches:** put the diff in the issue, or open a PR and say in the description that
  you are fine with it being applied upstream rather than merged. Either way the change
  gets applied under the upstream prefix with **you credited in the commit message**,
  and it ships in the next release. You will be told when that happens.
- **Questions about whether something is in scope:** ask before building. The README's
  *Non-goals* section is the short version, and it is genuinely a list of things this
  package will not grow.

If a PR sits open, it is not being ignored — it is waiting to be applied upstream, and
it will be closed rather than merged once it has been. That close is not a rejection.

## Working on it locally

```bash
git clone https://github.com/trangcm/vn-market-data
cd vn-market-data
pip install -e ".[vci,dev]"       # ".[dev]" is enough for the tests
pytest tests/ -q                  # offline: fake sources, temp SQLite, no network
```

**The editable install is required, not a convenience.** The repository root *is* the
package directory (see the comment at the top of `pyproject.toml`), so a bare `pytest`
in a fresh clone fails with `ModuleNotFoundError: No module named 'vn_market_data'`.
That is the missing install step, not a broken checkout.

The test suite is **43 tests and passes without `vnstock`**. The `[vci]` extra adds the
price board, statements and corporate actions at runtime; it does not add tests.

Other things worth running:

```bash
python examples/quickstart.py                    # the whole API against real endpoints
python examples/diagnose_sources.py HPG          # do the sources agree?
python -m vn_market_data.benchmarks.bench_store  # store-read timings, JSON to stdout
```

## What CI checks, and what it does not

`.github/workflows/ci.yml` runs the tests on Python 3.10 and 3.12, builds the wheel and
imports it from outside the source tree, and runs the store benchmark.

The benchmark is gated on its **scaling ratios** only — "reading 5x the rows costs no
more than 6x" — because those are claims about the algorithm and hold on any machine.
The absolute timings in `benchmarks/baseline.json` are **not** gated: they were recorded
on one developer's machine and would fail on every CI runner for reasons that have
nothing to do with your patch. If you make something faster, that file is stale rather
than wrong; say so in the issue and it gets re-recorded upstream.

## Things that need more care than usual

- **Adding a module?** `pyproject.toml` lists packages explicitly
  (`packages = [...]`, `package-dir = {"vn_market_data" = "."}`). A new *subpackage*
  must be added there or it silently vanishes from the wheel while continuing to work
  in your editable install. CI's wheel-import job exists to catch exactly this.
- **A source must never return `{}` for a failed fetch.** An empty result is
  authoritative and stops the source chain. Failure raises `SourceUnavailable`. Getting
  this backwards turns an outage into silently missing data.
- **Treat every upstream response as untrusted.** These endpoints are unauthenticated,
  so they are not authenticated *to us* either. Pure-`httpx` fetches go through
  `sources/http.py`'s `get_capped`, and a malformed payload yields `[]` rather than
  letting an exception escape the source contract.
- **The `md_*` schema in `schema.sql` is public surface.** A store written by one
  version gets opened by the next, so dropping or renaming a column is a breaking
  change even though no Python signature moved.

## License

MIT. By contributing you agree your contribution is licensed under it.
