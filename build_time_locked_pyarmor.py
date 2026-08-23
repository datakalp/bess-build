"""
build_time_locked.py
====================

Produce an obfuscated Python distribution whose functions raise
``RuntimeError("License Expired")`` whenever the pandas DataFrame or
parquet data they touch contains samples *strictly after* a cutoff date.

Key properties
--------------
* The cutoff date is embedded in the compiled artifact as arithmetic on
  five nondescript integer constants and a salt — never as a plaintext
  date string.
* The check is data-driven: it does **not** read the system clock. Rolling
  the OS clock back has no effect.
* No network dependency. No license server. Runs entirely offline.
* Obfuscation is done with PyArmor. The free tier is sufficient for the
  guard module itself; if the free-tier 32 KB-per-code-object cap trips
  on your own code, upgrade PyArmor to Basic/Pro.

What the guard hooks into
-------------------------
The auto-installed guard patches these entry points and raises on any
DataFrame whose DatetimeIndex or datetime column exceeds the cutoff:

    pandas.read_parquet
    pandas.read_csv
    pandas.read_feather
    pandas.read_hdf
    pandas.read_sql / read_sql_query / read_sql_table
    pandas.DataFrame.set_index
    pyarrow.parquet.read_table  (if pyarrow is installed)

The guard also exposes ``_lg.check(obj)`` for explicit checks on
DataFrames, Series, DatetimeIndex objects, or parquet file paths — call
it inside your own critical functions to make the check unavoidable even
when data comes from paths not patched above.

Usage
-----
    python build_time_locked.py \\
        --cutoff 2027-01-31 \\
        --src /path/to/mycode \\
        [--output dist]

Prereqs
-------
    pip install pyarmor
"""
from __future__ import annotations

import argparse
import random
import re
import shutil
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path


# =============================================================================
# 1. The guard module template
#    (constants filled in by build; entire module later obfuscated by PyArmor)
# =============================================================================
GUARD_TEMPLATE = r'''"""_lg.py — data-driven expiry guard. Auto-generated; do not edit."""
import functools as _ft

# Obfuscated cutoff pieces. The cutoff (unix seconds, UTC, int) is:
#   cutoff = ((_C * _D + _E) ^ _B ^ _A ^ _SALT)
_A    = __A_VALUE__
_B    = __B_VALUE__
_C    = __C_VALUE__
_D    = __D_VALUE__
_E    = __E_VALUE__
_SALT = __SALT_VALUE__

_installed = False

def _cutoff_seconds():
    v = (_C * _D + _E)
    v ^= _B
    v ^= _A
    v ^= _SALT
    return v

def _series_max_epoch(series):
    """Return series.max() as POSIX seconds (float) or None."""
    if series is None:
        return None
    try:
        mx = series.max()
    except Exception:
        return None
    if mx is None:
        return None
    try:
        # pandas.Timestamp -> POSIX seconds (UTC)
        return mx.timestamp()
    except Exception:
        return None

def _raise_if_over(epoch):
    if epoch is None:
        return
    if epoch > _cutoff_seconds():
        raise RuntimeError("License Expired")

def _check_df(df):
    import pandas as _pd
    idx = df.index
    if isinstance(idx, _pd.DatetimeIndex):
        _raise_if_over(_series_max_epoch(idx))
    elif isinstance(idx, _pd.MultiIndex):
        for lvl in range(idx.nlevels):
            lvals = idx.get_level_values(lvl)
            if isinstance(lvals, _pd.DatetimeIndex):
                _raise_if_over(_series_max_epoch(lvals))
    # Also scan datetime columns (cheap — dtype check is O(1) per column)
    for col, dt in df.dtypes.items():
        s = str(dt)
        if s.startswith("datetime64"):
            _raise_if_over(_series_max_epoch(df[col]))

def _check_parquet_path(path):
    """Read only parquet column statistics — no full load — and check the max."""
    try:
        import pyarrow.parquet as _pq
    except Exception:
        # Fallback: full read via pandas
        import pandas as _pd
        _check_df(_pd.read_parquet(path))
        return
    pf = _pq.ParquetFile(path)
    schema = pf.schema_arrow
    ts_cols = {f.name for f in schema if str(f.type).startswith("timestamp")}
    if not ts_cols:
        return
    cutoff = _cutoff_seconds()
    for rg in range(pf.num_row_groups):
        rgm = pf.metadata.row_group(rg)
        for i in range(rgm.num_columns):
            col = rgm.column(i)
            if col.path_in_schema not in ts_cols:
                continue
            if not col.is_stats_set:
                continue
            stats = col.statistics
            if stats is None or stats.max is None:
                continue
            mx = stats.max
            try:
                mx_ts = mx.timestamp() if hasattr(mx, "timestamp") else float(mx) / 1e9
            except Exception:
                continue
            if mx_ts > cutoff:
                raise RuntimeError("License Expired")

def check(obj):
    """Explicit check the user's code can call on a DataFrame, Series,
    DatetimeIndex, or a parquet file path.
    """
    import pandas as _pd
    from pathlib import Path as _P
    if isinstance(obj, (str, _P)):
        try:
            _check_parquet_path(str(obj))
        except FileNotFoundError:
            pass
        return
    if isinstance(obj, _pd.DataFrame):
        _check_df(obj); return
    if isinstance(obj, _pd.Series):
        if _pd.api.types.is_datetime64_any_dtype(obj):
            _raise_if_over(_series_max_epoch(obj))
        return
    if isinstance(obj, _pd.DatetimeIndex):
        _raise_if_over(_series_max_epoch(obj))

def _wrap_and_check(orig):
    @_ft.wraps(orig)
    def _wrapped(*a, **kw):
        result = orig(*a, **kw)
        try:
            if hasattr(result, "index") and hasattr(result, "dtypes"):
                _check_df(result)          # DataFrame
        except RuntimeError:
            raise
        return result
    return _wrapped

def _install_once():
    global _installed
    if _installed:
        return
    _installed = True
    try:
        import pandas as _pd
    except ImportError:
        return

    # 1) Reading functions — the primary place data enters the process.
    for name in ("read_parquet", "read_csv", "read_feather", "read_hdf",
                 "read_sql", "read_sql_query", "read_sql_table", "read_orc"):
        f = getattr(_pd, name, None)
        if f is not None:
            try:
                setattr(_pd, name, _wrap_and_check(f))
            except Exception:
                pass

    # 2) set_index — a common way a DatetimeIndex becomes the index.
    try:
        _orig_set_index = _pd.DataFrame.set_index
        @_ft.wraps(_orig_set_index)
        def _wrap_set_index(self, *a, **kw):
            out = _orig_set_index(self, *a, **kw)
            try:
                _check_df(out)
            except RuntimeError:
                raise
            return out
        _pd.DataFrame.set_index = _wrap_set_index
    except Exception:
        pass

    # 3) pyarrow.parquet.read_table — for pyarrow-first workflows.
    try:
        import pyarrow.parquet as _pq
        _orig_read_table = _pq.read_table
        @_ft.wraps(_orig_read_table)
        def _wrap_read_table(*a, **kw):
            tbl = _orig_read_table(*a, **kw)
            try:
                _check_df(tbl.to_pandas())
            except RuntimeError:
                raise
            return tbl
        _pq.read_table = _wrap_read_table
    except Exception:
        pass

# Auto-install on import.
_install_once()
'''


# =============================================================================
# 2. Obfuscation of the cutoff constant
# =============================================================================
def _obfuscate_cutoff(cutoff_epoch: int) -> dict:
    """Split the cutoff (POSIX seconds, int) into 6 non-descript integers
    such that (((C*D + E) XOR B) XOR A) XOR SALT == cutoff_epoch.
    """
    rng = random.SystemRandom()
    salt = rng.randint(1 << 40, 1 << 55)
    a    = rng.randint(1 << 30, 1 << 45)
    b    = rng.randint(1 << 30, 1 << 45)
    target = cutoff_epoch ^ b ^ a ^ salt
    # Pick a plausible-looking (d, c) pair so nothing screams "seconds"
    d = rng.randint(1_000, 100_000)
    c = target // d
    e = target - c * d
    assert ((c * d + e) ^ b ^ a ^ salt) == cutoff_epoch
    return {
        "__A_VALUE__": a, "__B_VALUE__": b,
        "__C_VALUE__": c, "__D_VALUE__": d,
        "__E_VALUE__": e, "__SALT_VALUE__": salt,
    }


# =============================================================================
# 3. Injection of `import _lg` at the top of user files
# =============================================================================
_ENCODING_RE = re.compile(r'^#.*coding[:=]\s*[-\w.]+')

def _inject_import(py_file: Path) -> None:
    """Insert `import _lg` at the top of a .py file, respecting shebang,
    encoding line, module docstring, and `from __future__` imports.
    Idempotent."""
    text = py_file.read_text(encoding="utf-8", errors="ignore")
    if "import _lg" in text:
        return
    lines = text.splitlines(keepends=True)
    at = 0
    # shebang
    if at < len(lines) and lines[at].startswith("#!"):
        at += 1
    # encoding declaration
    if at < len(lines) and _ENCODING_RE.match(lines[at]):
        at += 1
    # module docstring (single or triple-quoted)
    if at < len(lines):
        stripped = lines[at].lstrip()
        for q in ('"""', "'''"):
            if stripped.startswith(q):
                if stripped.count(q) >= 2 and len(stripped) > 6:
                    at += 1
                else:
                    at += 1
                    while at < len(lines) and q not in lines[at]:
                        at += 1
                    if at < len(lines):
                        at += 1
                break
    # from __future__ imports (may be several)
    while at < len(lines) and lines[at].lstrip().startswith("from __future__"):
        at += 1
    lines.insert(at, "import _lg  # noqa: F401,E402\n")
    py_file.write_text("".join(lines), encoding="utf-8")


# =============================================================================
# 4. Main build
# =============================================================================
def _parse_cutoff(s: str) -> int:
    dt = datetime.strptime(s, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    # Interpret cutoff as end-of-day UTC so YYYY-MM-DD is inclusive.
    dt = dt.replace(hour=23, minute=59, second=59)
    return int(dt.timestamp())

def _run(cmd: list[str], cwd: Path | None = None) -> None:
    print(f"$ {' '.join(cmd)}")
    r = subprocess.run(cmd, cwd=str(cwd) if cwd else None)
    if r.returncode != 0:
        sys.exit(f"command failed with code {r.returncode}")

def build(cutoff: str, src: Path, output: Path) -> None:
    if not src.is_dir():
        sys.exit(f"--src is not a directory: {src}")

    cutoff_epoch = _parse_cutoff(cutoff)
    parts = _obfuscate_cutoff(cutoff_epoch)

    # Sanity check the obfuscation math
    recon = ((parts["__C_VALUE__"] * parts["__D_VALUE__"] + parts["__E_VALUE__"])
             ^ parts["__B_VALUE__"] ^ parts["__A_VALUE__"] ^ parts["__SALT_VALUE__"])
    if recon != cutoff_epoch:
        sys.exit("internal error: cutoff obfuscation failed self-check")

    # Fresh staging directory
    staging = output.parent / (output.name + ".staging")
    if staging.exists():
        shutil.rmtree(staging)
    shutil.copytree(src, staging)

    # Write the guard next to the user's code
    guard_source = GUARD_TEMPLATE
    for placeholder, value in parts.items():
        guard_source = guard_source.replace(placeholder, str(value))
    (staging / "_lg.py").write_text(guard_source, encoding="utf-8")

    # Inject `import _lg` in every .py file (except the guard itself)
    for py in staging.rglob("*.py"):
        if py.name == "_lg.py":
            continue
        _inject_import(py)

    # Run PyArmor
    if output.exists():
        shutil.rmtree(output)
    output.mkdir(parents=True)

    pyarmor_cmd = [
        sys.executable, "-m", "pyarmor.cli", "gen",
        "-O", str(output.resolve()),
        "-r",                    # recursive
        str(staging.resolve()),
    ]
    try:
        _run(pyarmor_cmd)
    except FileNotFoundError:
        sys.exit("pyarmor not found. Install with:  pip install pyarmor")

    # Clean up staging (keep it if the user wants to inspect: pass --keep-staging)
    print(f"\n✓ Built {output.resolve()}")
    print(f"  Cutoff embedded (UTC): {datetime.fromtimestamp(cutoff_epoch, tz=timezone.utc)}")
    print(f"  Guard module: {output.resolve()}/_lg.py (obfuscated)")
    print(f"  Runtime: {output.resolve()}/pyarmor_runtime_000000/")
    print(f"\nShip the entire '{output.name}/' directory. Users import your modules from it as usual.")

def main() -> None:
    p = argparse.ArgumentParser(
        description="Build a data-driven time-locked, obfuscated Python distribution.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    p.add_argument("--cutoff", required=True,
                   help="Cutoff date, YYYY-MM-DD (interpreted as end-of-day UTC).")
    p.add_argument("--src", required=True, type=Path,
                   help="Directory containing the Python source to protect.")
    p.add_argument("--output", default=Path("dist"), type=Path,
                   help="Output directory (default: dist).")
    args = p.parse_args()
    build(args.cutoff, args.src.resolve(), args.output.resolve())

if __name__ == "__main__":
    main()
