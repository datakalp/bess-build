"""
build_time_locked_nuitka.py
===========================

Same runtime behavior as build_time_locked.py, but uses **Nuitka Free**.
Your code is compiled to native extension modules; the auto-generated guard
`_lg` is compiled too and placed at the output root.

Compilation model (important)
-----------------------------
Nuitka compiles a *package* (a directory with __init__.py) as an indivisible
unit — you cannot compile its __init__.py or submodules individually. This
script therefore works at PACKAGE granularity:

  * a top-level package  -> one ``nuitka --mode=package <dir>`` invocation
  * a top-level module   -> one ``nuitka --mode=module  <file>`` invocation

``--src`` should point at the directory that is the PARENT of your top-level
packages/modules (i.e. the directory you would put on ``sys.path``). If
``--src`` is itself a package, it is compiled as a single package unit.

Guard injection (robust)
------------------------
A small path-anchored bootstrap is injected at the earliest legal point in
every module (after any shebang, encoding line, module docstring, and
``from __future__`` imports — located via the real Python grammar with the
``ast`` module, so it is correct for every quoting/prefix style). At runtime
the bootstrap finds `_lg` by walking upward from the module's own location,
so it works at any nesting depth, whether imported as a package or as loose
modules, and whether `_lg` is compiled or shipped as source.

Prereqs
-------
* ``pip install nuitka``
* A working C compiler:
    - Linux: gcc or clang
    - Windows: MSVC (Visual Studio Build Tools) or MinGW-w64
    - macOS: Xcode Command Line Tools

Usage
-----
    python build_time_locked_nuitka.py \\
        --cutoff 2027-01-31 \\
        --src /path/to/parent_of_your_packages \\
        [--output dist] \\
        [--jobs 4] \\
        [--exclude-all GLOB]     [--exclude-all GLOB2] ... \\
        [--exclude-compile GLOB] [--exclude-compile GLOB2] ...

--exclude-all
    Glob (relative to --src) whose matches are completely omitted from the
    build: not copied, not injected, not compiled. Repeatable.

--exclude-compile
    Glob (relative to --src) whose matches receive the guard injection but
    are shipped as plain .py (not compiled). Repeatable. NOTE: because a
    package compiles as a unit, an exclude-compile match on any file INSIDE a
    top-level package promotes that WHOLE package to shipped-as-source; a
    match on a standalone top-level module keeps just that module as source.

Glob semantics follow ``pathlib.Path.glob``: ``*`` matches within one path
component, ``**`` matches any number of components. Examples:

    --exclude-all "tests/**"                 # entire tests/ directory
    --exclude-all "**/scratch_*.py"          # any scratch_*.py anywhere
    --exclude-compile "mypkg/plugins/*.py"   # -> keeps all of mypkg as source
    --exclude-compile "helper.py"            # standalone top-level module

Behavior at runtime
-------------------
Any pandas DataFrame or parquet flow containing samples strictly after the
cutoff raises ``RuntimeError("License Expired")``. The system clock is never
read.
"""
from __future__ import annotations

import argparse
import ast
import concurrent.futures as _cf
import json
import random
import re
import shutil
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path


# =============================================================================
# 1. Guard module template — bit-identical to the PyArmor build
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
    if series is None:
        return None
    try:
        mx = series.max()
    except Exception:
        return None
    if mx is None:
        return None
    try:
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
    for col, dt in df.dtypes.items():
        if str(dt).startswith("datetime64"):
            _raise_if_over(_series_max_epoch(df[col]))

def _check_parquet_path(path):
    try:
        import pyarrow.parquet as _pq
    except Exception:
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
                _check_df(result)
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
    for name in ("read_parquet", "read_csv", "read_feather", "read_hdf",
                 "read_sql", "read_sql_query", "read_sql_table", "read_orc"):
        f = getattr(_pd, name, None)
        if f is not None:
            try:
                setattr(_pd, name, _wrap_and_check(f))
            except Exception:
                pass
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

_install_once()
__CAE_DIAGNOSTIC__
'''


# Injected into the guard only when --diagnose-chained-assignment is passed.
# Runs at guard import (i.e. in the compiled bundle only, since dev runs the raw
# source without the guard). Reports each ChainedAssignmentError with the column
# being assigned and the file/line pandas attributes it to.
_CAE_DIAGNOSTIC_CODE = '''
def _install_cae_diagnostic():
    import warnings as _w, sys as _sys
    try:
        from pandas.errors import ChainedAssignmentError as _CAE
    except Exception:
        return
    _orig_show = _w.showwarning
    def _show(message, category, filename, lineno, file=None, line=None):
        try:
            _hit = isinstance(category, type) and issubclass(category, _CAE)
        except Exception:
            _hit = False
        if _hit:
            _key = "<not found>"
            _cols = None
            _f = _sys._getframe()
            while _f is not None:
                if _f.f_code.co_name == "__setitem__" and "key" in _f.f_locals:
                    try:
                        _key = repr(_f.f_locals.get("key"))
                    except Exception:
                        _key = "<unreadable>"
                    try:
                        _cols = list(_f.f_locals.get("self").columns)
                    except Exception:
                        _cols = None
                    break
                _f = _f.f_back
            print(">>> [ChainedAssignment] assigning " + _key
                  + " at " + str(filename) + ":" + str(lineno)
                  + ((" (columns: " + repr(_cols) + ")") if _cols is not None else ""),
                  file=_sys.stderr)
        return _orig_show(message, category, filename, lineno, file, line)
    _w.showwarning = _show
    _w.filterwarnings("always", category=_CAE)

_install_cae_diagnostic()
'''


# =============================================================================
# 2. Same helpers as the PyArmor build
# =============================================================================
def _obfuscate_cutoff(cutoff_epoch: int) -> dict:
    rng  = random.SystemRandom()
    salt = rng.randint(1 << 40, 1 << 55)
    a    = rng.randint(1 << 30, 1 << 45)
    b    = rng.randint(1 << 30, 1 << 45)
    target = cutoff_epoch ^ b ^ a ^ salt
    d = rng.randint(1_000, 100_000)
    c = target // d
    e = target - c * d
    assert ((c * d + e) ^ b ^ a ^ salt) == cutoff_epoch
    return {
        "__A_VALUE__": a, "__B_VALUE__": b,
        "__C_VALUE__": c, "__D_VALUE__": d,
        "__E_VALUE__": e, "__SALT_VALUE__": salt,
    }

_ENCODING_RE = re.compile(r'^#.*coding[:=]\s*[-\w.]+')

# ---------------------------------------------------------------------------
# Guard bootstrap that is injected into every user module.
#
# Rather than a bare ``import _lg`` (which resolves as an ABSOLUTE top-level
# import and therefore breaks for modules living inside a package unless the
# dist root happens to be on sys.path), we inject a tiny path-anchored
# loader. It walks up from this file's own location to find `_lg` (compiled
# `_lg*.pyd` / `_lg*.so`, or source `_lg.py`) and loads it directly. This is
# robust to:
#   * modules at any nesting depth inside packages
#   * the code being imported as a package OR as loose modules
#   * `_lg` being compiled (native extension) OR shipped as source
#   * arbitrary sys.path configurations on the customer's machine
#
# The loader is deliberately branded with a sentinel comment so re-runs are
# idempotent and so the block can be recognised.
# ---------------------------------------------------------------------------
_GUARD_SENTINEL = "# __LG_GUARD_BOOTSTRAP__"

_GUARD_BOOTSTRAP = _GUARD_SENTINEL + """
import os as _os, sys as _sys, importlib.util as _ilu, importlib.machinery as _ilm
def _lg_load():
    name = "_lg"
    if name in _sys.modules:
        return _sys.modules[name]
    try:
        here = _os.path.dirname(_os.path.abspath(__file__))
    except NameError:
        here = _os.getcwd()
    exts = list(_ilm.EXTENSION_SUFFIXES) + [".py"]
    d = here
    while True:
        for ext in exts:
            cand = _os.path.join(d, name + ext)
            if _os.path.isfile(cand):
                spec = _ilu.spec_from_file_location(name, cand)
                if spec and spec.loader:
                    mod = _ilu.module_from_spec(spec)
                    _sys.modules[name] = mod
                    spec.loader.exec_module(mod)
                    return mod
        parent = _os.path.dirname(d)
        if parent == d:
            break
        d = parent
    try:
        return __import__(name)
    except Exception:
        return None
_lg = _lg_load()
del _lg_load
""".rstrip() + "\n"


def _future_and_docstring_end_line(source: str) -> int:
    """Return the 1-based line number AFTER which it is always legal to insert
    a statement — i.e. after the module docstring (if any) and after ALL
    ``from __future__`` imports.

    Uses the real Python grammar via ast, so it is correct for every quoting
    style, string prefix, implicit concatenation, and comment arrangement.
    Returns 0 if nothing needs to be skipped (insert at very top).
    """
    tree = ast.parse(source)
    insert_after = 0

    body = tree.body
    idx = 0

    # Module docstring: first statement is an Expr wrapping a (possibly
    # concatenated) string constant.
    if body and isinstance(body[0], ast.Expr) and isinstance(
            getattr(body[0], "value", None), ast.Constant) and isinstance(
            body[0].value.value, str):
        insert_after = max(insert_after, body[0].end_lineno or 0)
        idx = 1

    # All __future__ imports must be contiguous at the top (after docstring).
    while idx < len(body):
        node = body[idx]
        if isinstance(node, ast.ImportFrom) and node.module == "__future__":
            insert_after = max(insert_after, node.end_lineno or 0)
            idx += 1
        else:
            break

    return insert_after


def _inject_import(py_file: Path) -> None:
    """Inject the path-anchored guard bootstrap at the earliest legal point.

    Robust to shebang, encoding declarations, module docstrings (any style),
    and ``from __future__`` imports. Idempotent via the sentinel.
    """
    text = py_file.read_text(encoding="utf-8", errors="ignore")
    if _GUARD_SENTINEL in text:
        return

    try:
        insert_after = _future_and_docstring_end_line(text)
    except SyntaxError:
        # If the user's file does not parse, we cannot safely inject. Leave it
        # untouched and let Nuitka surface the real syntax error with context.
        return

    lines = text.splitlines(keepends=True)

    # Ensure the file ends with a newline so our insertion math is clean.
    if lines and not lines[-1].endswith("\n"):
        lines[-1] = lines[-1] + "\n"

    block = _GUARD_BOOTSTRAP
    if not block.endswith("\n"):
        block += "\n"

    if insert_after <= 0:
        new_text = block + "".join(lines)
    else:
        head = "".join(lines[:insert_after])
        tail = "".join(lines[insert_after:])
        # Guarantee a newline between the head and our block.
        if head and not head.endswith("\n"):
            head += "\n"
        new_text = head + block + tail

    py_file.write_text(new_text, encoding="utf-8")

def _parse_cutoff(s: str) -> int:
    dt = datetime.strptime(s, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    dt = dt.replace(hour=23, minute=59, second=59)
    return int(dt.timestamp())


# =============================================================================
# 3. Glob-pattern matching for --exclude-all / --exclude-compile
# =============================================================================
def _collect_matching(src_root: Path, patterns: list[str]) -> set[Path]:
    """Expand each glob against src_root and return matched paths, relative to src_root."""
    matched: set[Path] = set()
    for pat in patterns:
        pat = pat.replace("\\", "/")   # accept Windows-style separators too
        for p in src_root.glob(pat):
            try:
                matched.add(p.relative_to(src_root))
            except ValueError:
                pass
    return matched

def _matches_or_child(rel_path: Path, matched: set[Path]) -> bool:
    """True if rel_path itself, or any of its ancestors, is in `matched`.

    An ancestor match is what makes directory patterns (e.g. ``tests/``)
    cover every file below them.
    """
    if rel_path in matched:
        return True
    for parent in rel_path.parents:
        if parent == Path("."):
            continue
        if parent in matched:
            return True
    return False

def _read_text_any_encoding(path: Path, forced_encoding=None):
    """Read a text file robustly, returning (text, encoding_used).

    Order: an explicit forced encoding, then a byte-order-mark if present, then
    UTF-8, then the platform's locale preferred encoding (what Python's built-in
    open() uses by default, so it matches how the file reads on the build host),
    then cp1252, then latin-1 (which decodes any byte sequence and therefore
    always succeeds as a last resort).
    """
    raw = path.read_bytes()

    if forced_encoding:
        return raw.decode(forced_encoding), forced_encoding

    boms = [
        (b"\xef\xbb\xbf",     "utf-8-sig"),
        (b"\xff\xfe\x00\x00", "utf-32-le"),
        (b"\x00\x00\xfe\xff", "utf-32-be"),
        (b"\xff\xfe",         "utf-16-le"),
        (b"\xfe\xff",         "utf-16-be"),
    ]
    for bom, enc in boms:
        if raw.startswith(bom):
            return raw.decode(enc), enc

    import locale
    candidates = ["utf-8"]
    loc = locale.getpreferredencoding(False)
    if loc:
        candidates.append(loc)
    candidates += ["cp1252", "latin-1"]

    seen = set()
    last_err = None
    for enc in candidates:
        key = enc.lower()
        if key in seen:
            continue
        seen.add(key)
        try:
            return raw.decode(enc), enc
        except (UnicodeDecodeError, LookupError) as ex:
            last_err = ex
    raise last_err if last_err else UnicodeDecodeError("unknown", b"", 0, 1, "no encoding")


def _dotted_import_hint(staging: Path, py_rel: Path) -> str:
    """Best-effort dotted import path for a generated module at py_rel.

    If the module lives inside a top-level package, returns pkg.sub.module.
    Otherwise returns the bare module name (its containing directory must be on
    sys.path at runtime).
    """
    parts = py_rel.parts
    mod_stem = py_rel.stem
    cur = staging
    pkg_start = None
    for i, comp in enumerate(parts[:-1]):
        cur = cur / comp
        if (cur / "__init__.py").is_file():
            pkg_start = i
            break
    if pkg_start is None:
        return mod_stem
    return ".".join(list(parts[pkg_start:-1]) + [mod_stem])


def _rewrite_json_loads(staging: Path,
                        embedded_by_basename: dict,
                        generated_data_modules: set) -> tuple[list, list, list]:
    """Rewrite json.load/json.loads of embedded JSON files to use the compiled
    data module instead of opening a file that no longer exists.

    Returns (rewritten, broken, uncertain):
      rewritten : "file:line  pattern  ->  module" for calls we rewrote.
      broken    : "file:line  reason" for references to an embedded (now-deleted)
                  file that we could NOT rewrite. These WILL crash at runtime, so
                  the build must stop unless the user overrides.
      uncertain : "file:line  reason" for calls we could not classify (e.g. a
                  fully-dynamic path that may point to a non-embedded file that is
                  still present). These are warnings, not hard failures.

    Handled patterns (path may be a literal or an expression whose filename
    literal is used, e.g. os.path.join(HERE, "config.json")):
        json.load(open(PATH[, ...]))
        json.loads(open(PATH[, ...]).read())
        json.loads(Path(PATH).read_text([...]))
        with open(PATH[, ...]) as f: ... json.load(f) ...   (f used only by load)
        f = open(PATH[, ...]); ...; json.load(f); [f.close()]
    Non-file json.loads (e.g. json.loads(meta[b"pandas"])) and loads of
    non-embedded JSON are left exactly as-is.
    """
    rewritten: list = []
    broken: list = []
    uncertain: list = []

    if not embedded_by_basename:
        return rewritten, broken, uncertain

    for py in sorted(staging.rglob("*.py")):
        rel = py.relative_to(staging)
        if rel in generated_data_modules:
            continue

        raw = py.read_bytes()
        had_bom = raw.startswith(b"\xef\xbb\xbf")
        try:
            text = raw.decode("utf-8-sig") if had_bom else raw.decode("utf-8")
        except UnicodeDecodeError:
            if b"json" in raw and b"load" in raw:
                uncertain.append(f"{rel}  (non-UTF-8 source; not scanned — if it loads an "
                                 f"embedded JSON it will crash; edit manually)")
            continue

        try:
            tree = ast.parse(text)
        except SyntaxError:
            continue

        for parent in ast.walk(tree):
            for child in ast.iter_child_nodes(parent):
                child.parent = parent

        src_bytes = text.encode("utf-8")
        line_starts = [0]
        for ln in src_bytes.split(b"\n"):
            line_starts.append(line_starts[-1] + len(ln) + 1)

        def abs_off(lineno: int, col: int) -> int:
            return line_starts[lineno - 1] + col

        json_mods: set = set()
        load_names: dict = {}
        path_names: set = set()
        pathlib_mods: set = set()
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                for a in node.names:
                    if a.name == "json":
                        json_mods.add(a.asname or "json")
                    elif a.name == "pathlib":
                        pathlib_mods.add(a.asname or "pathlib")
            elif isinstance(node, ast.ImportFrom):
                if node.module == "json":
                    for a in node.names:
                        if a.name in ("load", "loads"):
                            load_names[a.asname or a.name] = a.name
                elif node.module == "pathlib":
                    for a in node.names:
                        if a.name == "Path":
                            path_names.add(a.asname or "Path")
        if not json_mods and not load_names:
            json_mods.add("json")

        def is_open_call(n) -> bool:
            if not isinstance(n, ast.Call):
                return False
            f = n.func
            if isinstance(f, ast.Name) and f.id == "open":
                return True
            if (isinstance(f, ast.Attribute) and f.attr == "open"
                    and isinstance(f.value, ast.Name) and f.value.id == "io"):
                return True
            return False

        def json_kind(n):
            if not isinstance(n, ast.Call):
                return None
            f = n.func
            if (isinstance(f, ast.Attribute) and f.attr in ("load", "loads")
                    and isinstance(f.value, ast.Name) and f.value.id in json_mods):
                return f.attr
            if isinstance(f, ast.Name) and f.id in load_names:
                return load_names[f.id]
            return None

        def lit_str(n):
            return n.value if isinstance(n, ast.Constant) and isinstance(n.value, str) else None

        def filename_from_pathexpr(node):
            if node is None:
                return None
            direct = lit_str(node)
            if direct is not None:
                return Path(direct.replace("\\", "/")).name
            found = [lit_str(sub) for sub in ast.walk(node)]
            found = [s for s in found if s]
            if not found:
                return None
            for s in reversed(found):
                if s.lower().endswith(".json"):
                    return Path(s.replace("\\", "/")).name
            return Path(found[-1].replace("\\", "/")).name

        def resolve(basename: str, literalish: str):
            cands = embedded_by_basename.get(basename)
            if not cands:
                return None
            if len(cands) == 1:
                c = cands[0]
                return (c["modstem"], c["dotted"])
            lit_parts = Path((literalish or basename).replace("\\", "/")).parts
            best = None
            for c in cands:
                rel_parts = Path(c["rel"]).parts
                k = 0
                for a, b in zip(reversed(lit_parts), reversed(rel_parts)):
                    if a == b:
                        k += 1
                    else:
                        break
                if best is None or k > best[0]:
                    best = (k, c)
                elif best is not None and k == best[0]:
                    best = (k, None)
            if best and best[1] is not None and best[0] >= 1:
                return (best[1]["modstem"], best[1]["dotted"])
            return "AMBIGUOUS"

        def alias_for(modstem: str) -> str:
            return "_embedded_" + re.sub(r"\W", "_", modstem)

        def enclosing_with_binding(node, name):
            p = getattr(node, "parent", None)
            while p is not None:
                if isinstance(p, ast.With) and len(p.items) == 1:
                    it = p.items[0]
                    if isinstance(it.optional_vars, ast.Name) and it.optional_vars.id == name:
                        return p
                p = getattr(p, "parent", None)
            return None

        def enclosing_scope(node):
            p = getattr(node, "parent", None)
            while p is not None:
                if isinstance(p, (ast.FunctionDef, ast.AsyncFunctionDef, ast.Module)):
                    return p
                p = getattr(p, "parent", None)
            return tree

        def stmt_block_of(node):
            cur = node
            p = getattr(cur, "parent", None)
            while p is not None:
                for _field, val in ast.iter_fields(p):
                    if isinstance(val, list) and cur in val:
                        return val, val.index(cur)
                cur = p
                p = getattr(cur, "parent", None)
            return None

        def name_loads(scope, name):
            return [n for n in ast.walk(scope)
                    if isinstance(n, ast.Name) and n.id == name and isinstance(n.ctx, ast.Load)]

        def open_is_readmode(call):
            mode = None
            if len(call.args) >= 2:
                mode = lit_str(call.args[1])
            for kw in call.keywords:
                if kw.arg == "mode":
                    mode = lit_str(kw.value)
            if mode is None:
                return True  # default mode is 'r'
            return not any(c in mode for c in ("w", "a", "x", "+"))

        edits: list = []
        used_modules: dict = {}
        handled_calls: set = set()
        consumed_opens: set = set()   # open() nodes rewritten away
        flagged_opens: set = set()    # open() nodes already reported as broken

        def neutralize_with(w):
            it = w.items[0]
            anchor = it.optional_vars or it.context_expr
            anchor_end = abs_off(anchor.end_lineno, anchor.end_col_offset)
            colon_idx = src_bytes.index(b":", anchor_end)
            wstart = abs_off(w.lineno, w.col_offset)
            edits.append((wstart, colon_idx + 1, b"if True:"))

        # ---- (A) file-handle loads ---------------------------------------
        for call in [n for n in ast.walk(tree) if isinstance(n, ast.Call)]:
            if id(call) in handled_calls:
                continue
            kind = json_kind(call)
            if kind is None or not call.args:
                continue
            arg = call.args[0]
            var = None
            if kind == "load" and isinstance(arg, ast.Name):
                var = arg.id
            elif (kind == "loads" and isinstance(arg, ast.Call)
                  and isinstance(arg.func, ast.Attribute) and arg.func.attr == "read"
                  and isinstance(arg.func.value, ast.Name)):
                var = arg.func.value.id
            if var is None:
                continue

            mech = None
            open_call = None
            asg = None
            w = enclosing_with_binding(call, var)
            if w is not None and is_open_call(w.items[0].context_expr):
                mech, open_call = "with", w.items[0].context_expr
            else:
                loc = stmt_block_of(call)
                if loc:
                    block, idx = loc
                    for j in range(idx - 1, -1, -1):
                        st = block[j]
                        if (isinstance(st, ast.Assign) and len(st.targets) == 1
                                and isinstance(st.targets[0], ast.Name)
                                and st.targets[0].id == var and is_open_call(st.value)):
                            mech, open_call, asg = "assign", st.value, (block, j, st)
                            break
                        if (isinstance(st, ast.Assign) and any(
                                isinstance(t, ast.Name) and t.id == var for t in st.targets)):
                            break
            if open_call is None:
                continue

            fname = filename_from_pathexpr(open_call.args[0]) if open_call.args else None
            if fname is None:
                uncertain.append(f"{rel}:{call.lineno}  json.{kind}(<file handle>) opens a "
                                 f"fully-dynamic path — if that resolves to an embedded JSON it "
                                 f"will crash; otherwise ignore")
                continue
            base = fname
            if base not in embedded_by_basename:
                continue
            res = resolve(base, fname)
            if res in (None, "AMBIGUOUS"):
                if res == "AMBIGUOUS":
                    broken.append(f"{rel}:{call.lineno}  '{base}' is embedded but the name is "
                                  f"ambiguous among embedded files — cannot rewrite")
                    flagged_opens.add(id(open_call))
                continue
            modstem, dotted = res

            if mech == "with":
                loads = name_loads(w, var)
                if len(loads) != 1:
                    broken.append(f"{rel}:{call.lineno}  '{base}' is embedded but file handle "
                                  f"'{var}' is used more than once in the with-block — cannot "
                                  f"safely rewrite")
                    flagged_opens.add(id(open_call))
                    continue
                neutralize_with(w)
            else:
                scope = enclosing_scope(call)
                loads = name_loads(scope, var)
                close_calls = [n for n in ast.walk(scope)
                               if isinstance(n, ast.Call) and isinstance(n.func, ast.Attribute)
                               and n.func.attr == "close" and isinstance(n.func.value, ast.Name)
                               and n.func.value.id == var]
                if len(loads) != 1 + len(close_calls):
                    broken.append(f"{rel}:{call.lineno}  '{base}' is embedded but file handle "
                                  f"'{var}' has other uses — cannot safely rewrite")
                    flagged_opens.add(id(open_call))
                    continue
                os_ = abs_off(open_call.lineno, open_call.col_offset)
                oe_ = abs_off(open_call.end_lineno, open_call.end_col_offset)
                edits.append((os_, oe_, b"None"))
                for cc in close_calls:
                    cs = abs_off(cc.lineno, cc.col_offset)
                    ce = abs_off(cc.end_lineno, cc.end_col_offset)
                    edits.append((cs, ce, b"None"))

            alias = alias_for(modstem)
            used_modules[modstem] = dotted
            cs = abs_off(call.lineno, call.col_offset)
            ce = abs_off(call.end_lineno, call.end_col_offset)
            edits.append((cs, ce, f"{alias}.load()".encode("utf-8")))
            handled_calls.add(id(call))
            consumed_opens.add(id(open_call))
            rewritten.append(f"{rel}:{call.lineno}  json.{kind}(<file handle>) -> {dotted}")

        # ---- (B) inline expressions --------------------------------------
        for node in ast.walk(tree):
            if not isinstance(node, ast.Call) or id(node) in handled_calls:
                continue
            kind = json_kind(node)
            if kind is None or not node.args:
                continue
            arg = node.args[0]
            fname = None
            open_node = None
            if kind == "load" and is_open_call(arg) and arg.args:
                fname = filename_from_pathexpr(arg.args[0]); open_node = arg
            elif kind == "loads" and isinstance(arg, ast.Call) and isinstance(arg.func, ast.Attribute):
                recv = arg.func.value
                if arg.func.attr == "read" and is_open_call(recv) and recv.args:
                    fname = filename_from_pathexpr(recv.args[0]); open_node = recv
                elif arg.func.attr == "read_text":
                    fname = filename_from_pathexpr(recv)

            if fname is None:
                if kind == "load" and is_open_call(arg) and arg.args:
                    uncertain.append(f"{rel}:{node.lineno}  json.load(open(<fully-dynamic>)) — "
                                     f"if that resolves to an embedded JSON it will crash")
                continue

            base = fname
            if base not in embedded_by_basename:
                continue
            res = resolve(base, fname)
            if res in (None, "AMBIGUOUS"):
                if res == "AMBIGUOUS":
                    broken.append(f"{rel}:{node.lineno}  '{base}' is embedded but the name is "
                                  f"ambiguous among embedded files — cannot rewrite")
                    if open_node is not None:
                        flagged_opens.add(id(open_node))
                continue
            modstem, dotted = res
            used_modules[modstem] = dotted
            alias = alias_for(modstem)
            start = abs_off(node.lineno, node.col_offset)
            end = abs_off(node.end_lineno, node.end_col_offset)
            edits.append((start, end, f"{alias}.load()".encode("utf-8")))
            if open_node is not None:
                consumed_opens.add(id(open_node))
            rewritten.append(f"{rel}:{node.lineno}  {kind}(...) -> {dotted}")

        # ---- (C) backstop: any remaining reference to an embedded file ----
        for call in [n for n in ast.walk(tree) if isinstance(n, ast.Call)]:
            # bare open() of an embedded file in read mode, not handled above
            if is_open_call(call) and id(call) not in consumed_opens and id(call) not in flagged_opens:
                fn = filename_from_pathexpr(call.args[0]) if call.args else None
                if fn and fn in embedded_by_basename and open_is_readmode(call):
                    broken.append(f"{rel}:{call.lineno}  open('{fn}') reads a JSON that was "
                                  f"embedded and removed, but this open() was not rewritten "
                                  f"(unrecognised load pattern) — build would crash at runtime")
            # pandas.read_json / <x>.read_json of an embedded file
            f = call.func
            if isinstance(f, ast.Attribute) and f.attr == "read_json" and call.args:
                fn = filename_from_pathexpr(call.args[0])
                if fn and fn in embedded_by_basename:
                    broken.append(f"{rel}:{call.lineno}  read_json('{fn}') reads a JSON that was "
                                  f"embedded and removed — read_json is not auto-rewritten; "
                                  f"exclude this file from --embed-json or load it differently")

        if not edits:
            continue

        edits.sort(key=lambda e: e[0], reverse=True)
        out = src_bytes
        last_start = None
        safe_edits = []
        for start, end, repl in edits:
            if last_start is not None and end > last_start:
                broken.append(f"{rel}  (overlapping rewrite near byte {start} was skipped; "
                              f"a load of an embedded file may remain — review manually)")
                continue
            safe_edits.append((start, end, repl))
            last_start = start
        for start, end, repl in safe_edits:
            out = out[:start] + repl + out[end:]
        new_text = out.decode("utf-8")

        try:
            insert_after = _future_and_docstring_end_line(new_text)
        except SyntaxError as e:
            broken.append(f"{rel}  (auto-rewrite produced invalid code: {e}); edit manually")
            continue
        import_lines = [f"import {dotted} as {alias_for(modstem)}\n"
                        for modstem, dotted in sorted(used_modules.items())]
        nlines = new_text.splitlines(keepends=True)
        if insert_after <= 0:
            new_text = "".join(import_lines) + new_text
        else:
            head = "".join(nlines[:insert_after])
            tail = "".join(nlines[insert_after:])
            if head and not head.endswith("\n"):
                head += "\n"
            new_text = head + "".join(import_lines) + tail

        try:
            ast.parse(new_text)
        except SyntaxError as e:
            broken.append(f"{rel}  (auto-rewrite produced invalid syntax: {e}); edit manually")
            continue

        data_out = new_text.encode("utf-8")
        if had_bom:
            data_out = b"\xef\xbb\xbf" + data_out
        py.write_bytes(data_out)

    return rewritten, broken, uncertain

# Python bytecode caches are never part of a Nuitka build and must never reach
# dist — a stray .pyc is a decompilable copy of source. We drop these
# unconditionally at both the staging-copy and final-copy stages.
_BYTECODE_DIR_NAMES = {"__pycache__"}
_BYTECODE_SUFFIXES  = {".pyc", ".pyo"}

def _is_bytecode_artifact(name_or_rel) -> bool:
    p = Path(name_or_rel)
    if p.suffix in _BYTECODE_SUFFIXES:
        return True
    # Any component named __pycache__ (covers nested dirs)
    if any(part in _BYTECODE_DIR_NAMES for part in p.parts):
        return True
    return False

def _make_ignore_callback(src_root: Path, exclude_all_rel: set[Path]):
    """Build a shutil.copytree(ignore=...) callback.

    Always skips Python bytecode caches (__pycache__, *.pyc, *.pyo). Also skips
    anything matched by --exclude-all.
    """
    def _ignore(current_dir: str, entries: list[str]) -> list[str]:
        cd = Path(current_dir)
        try:
            rel_dir = cd.relative_to(src_root)
        except ValueError:
            rel_dir = Path(".")
        skip = []
        for entry in entries:
            # Unconditional: never stage bytecode caches.
            if entry in _BYTECODE_DIR_NAMES or Path(entry).suffix in _BYTECODE_SUFFIXES:
                skip.append(entry)
                continue
            if exclude_all_rel:
                rel_entry = (rel_dir / entry) if str(rel_dir) != "." else Path(entry)
                if _matches_or_child(rel_entry, exclude_all_rel):
                    skip.append(entry)
        return skip
    return _ignore


# =============================================================================
# 4. Nuitka compilation — operates at PACKAGE granularity, not per-file.
#
# Nuitka rejects `nuitka --module some_pkg/__init__.py`; a directory holding
# an __init__.py is a package and must be compiled as a unit via
# `nuitka --module some_pkg/`. Compiling package submodules individually as
# top-level modules also breaks their relative imports. So we identify
# "compile units":
#   * top-level package  -> a directory (at the compile root) with __init__.py
#   * top-level module   -> a .py file not contained in any package
# and invoke Nuitka once per unit.
# =============================================================================
def _is_package_dir(d: Path) -> bool:
    return (d / "__init__.py").is_file()

def _compile_unit(target: Path, out_dir: Path, is_package: bool) -> tuple[Path, int, str]:
    """Compile one unit (a package directory or a single module file)."""
    mode = "--mode=package" if is_package else "--mode=module"
    cmd = [
        sys.executable, "-m", "nuitka",
        mode,
        "--remove-output",
        "--no-pyi-file",
        "--quiet",
        f"--output-dir={out_dir}",
        str(target),
    ]
    r = subprocess.run(cmd, capture_output=True, text=True)
    return target, r.returncode, (r.stderr or r.stdout)


def _discover_compile_units(root: Path) -> tuple[list[Path], list[Path]]:
    """Return (package_dirs, module_files) that are the top-level compile units
    directly under `root`. Nested packages/modules are compiled as part of
    their enclosing top-level package and are NOT returned separately.

    If `root` itself is a package (has __init__.py), the whole root is a single
    package unit.
    """
    if _is_package_dir(root):
        return [root], []

    package_dirs: list[Path] = []
    module_files: list[Path] = []
    for entry in sorted(root.iterdir()):
        if entry.is_dir():
            if _is_package_dir(entry):
                package_dirs.append(entry)
            # A non-package directory may itself contain top-level packages or
            # modules (namespace-style layout). Recurse into it.
            else:
                sub_pkgs, sub_mods = _discover_compile_units(entry)
                package_dirs.extend(sub_pkgs)
                module_files.extend(sub_mods)
        elif entry.is_file() and entry.suffix == ".py":
            module_files.append(entry)
    return package_dirs, module_files


# =============================================================================
# 5. Main build
# =============================================================================
def build(cutoff: str, src: Path, output: Path, jobs: int,
          exclude_all: list[str], exclude_compile: list[str],
          embed_json: list[str], json_encoding=None,
          rewrite_json_loads: bool = True,
          allow_unrewritten_embedded_json: bool = False,
          diagnose_chained_assignment: bool = False) -> None:
    if not src.is_dir():
        sys.exit(f"--src is not a directory: {src}")
    try:
        subprocess.run([sys.executable, "-m", "nuitka", "--version"],
                       capture_output=True, check=True)
    except (FileNotFoundError, subprocess.CalledProcessError):
        sys.exit("nuitka not found. Install with:  pip install nuitka")

    cutoff_epoch = _parse_cutoff(cutoff)
    parts        = _obfuscate_cutoff(cutoff_epoch)
    recon = ((parts["__C_VALUE__"] * parts["__D_VALUE__"] + parts["__E_VALUE__"])
             ^ parts["__B_VALUE__"] ^ parts["__A_VALUE__"] ^ parts["__SALT_VALUE__"])
    if recon != cutoff_epoch:
        sys.exit("internal error: cutoff obfuscation failed self-check")

    # Resolve exclusion patterns against the source tree
    exclude_all_rel     = _collect_matching(src, exclude_all)
    exclude_compile_rel = _collect_matching(src, exclude_compile)
    if exclude_all:
        if exclude_all_rel:
            print(f"--exclude-all matched {len(exclude_all_rel)} path(s):")
            for p in sorted(exclude_all_rel):
                print(f"    {p}")
        else:
            print("--exclude-all matched nothing")
    if exclude_compile:
        if exclude_compile_rel:
            print(f"--exclude-compile matched {len(exclude_compile_rel)} path(s):")
            for p in sorted(exclude_compile_rel):
                print(f"    {p}")
        else:
            print("--exclude-compile matched nothing")

    # Fresh staging (skip exclude-all matches during copy).
    #
    # If --src is itself a package (has __init__.py), we must preserve its real
    # name so the compiled artifact is named correctly (e.g. mypkg.so, not
    # dist.staging.so). In that case we stage INTO staging/<pkgname>/ and treat
    # <pkgname> as a normal top-level package unit. Otherwise --src is a plain
    # container and we stage its contents directly into staging/.
    staging = output.parent / (output.name + ".staging")
    if staging.exists():
        shutil.rmtree(staging)

    src_is_package = (src / "__init__.py").is_file()
    ignore_cb = _make_ignore_callback(src, exclude_all_rel)

    if src_is_package:
        staging.mkdir(parents=True)
        staged_root = staging / src.name
        shutil.copytree(src, staged_root, ignore=ignore_cb)
    else:
        shutil.copytree(src, staging, ignore=ignore_cb)

    # Rebase --exclude-compile matches into the STAGING frame so all downstream
    # logic uses one consistent set of relative paths. (--exclude-all matches
    # were physically dropped during the copy above, so they need no staging
    # frame.) When --src is a package, staging paths are prefixed with the
    # package name.
    prefix = Path(src.name) if src_is_package else None
    def _to_staging(rel: Path) -> Path:
        return (prefix / rel) if prefix else rel
    exclude_compile_rel = {_to_staging(r) for r in exclude_compile_rel}

    # ------------------------------------------------------------------
    # Embed selected JSON files as compiled data modules so no .json lands in
    # dist. Each matched foo/bar.json becomes foo/bar_json.py holding the data
    # as native Python constants (compiled to .pyd along with the rest), and
    # the original .json is removed from staging. This changes the access
    # pattern from a file read to an import (see build output for the exact
    # import line).
    # ------------------------------------------------------------------
    embed_json_rel = {_to_staging(r) for r in _collect_matching(src, embed_json)}
    # Only actual .json files are embeddable.
    embed_json_rel = {r for r in embed_json_rel if r.suffix.lower() == ".json"}
    generated_data_modules: set[Path] = set()  # staging-relative .py we generated
    json_module_map: list[tuple[Path, Path, str, str]] = []  # (json_rel, py_rel, hint, enc_note)
    embedded_by_basename: dict[str, list[dict]] = {}  # for the json.load rewriter
    if embed_json_rel:
        for jrel in sorted(embed_json_rel):
            jpath = staging / jrel
            if not jpath.is_file():
                continue
            try:
                text, used_enc = _read_text_any_encoding(jpath, json_encoding)
            except Exception as e:
                sys.exit(f"--embed-json: could not read {jrel}: {e}")
            try:
                data = json.loads(text)
            except Exception as e:
                sys.exit(f"--embed-json: {jrel} is not valid JSON "
                         f"(decoded as {used_enc}): {e}")
            enc_note = "" if used_enc.lower() in ("utf-8", "utf8") else f"  [decoded as {used_enc}]"

            # Deterministic module name: <stem>_json.py in the same directory.
            mod_name = jpath.stem + "_json"
            py_rel = jrel.with_name(mod_name + ".py")
            py_path = staging / py_rel
            if py_path.exists():
                sys.exit(f"--embed-json: cannot generate {py_rel} (a file with that "
                         f"name already exists). Rename it or exclude this JSON.")

            # repr() of json-decoded data (dict/list/str/int/float/bool/None)
            # is valid, round-trippable Python source that becomes native
            # constants when Nuitka compiles it. Characters decoded from the
            # source (e.g. degree signs) are preserved as proper Unicode.
            literal = repr(data)
            module_src = (
                f'"""Auto-generated from {jpath.name} by build_time_locked_nuitka.py.\n'
                f'Do not edit. Access the data via DATA (shared) or load() (fresh copy).\n"""\n'
                f"import copy as _copy\n"
                f"DATA = {literal}\n\n"
                f"def load():\n"
                f"    \"\"\"Return a fresh deep copy of the embedded data — a drop-in for\n"
                f"    json.load()/json.loads() of the original file (each call returns a\n"
                f"    new object, matching json's behaviour).\"\"\"\n"
                f"    return _copy.deepcopy(DATA)\n"
            )
            # Validate what we generated actually parses before writing.
            try:
                ast.parse(module_src)
            except SyntaxError as e:  # extremely defensive; repr() should be safe
                sys.exit(f"--embed-json: failed to generate a valid module for "
                         f"{jrel}: {e}")

            # Always write the generated module as UTF-8 (with an explicit
            # coding declaration for good measure), regardless of the source
            # file's original encoding.
            py_path.write_text("# -*- coding: utf-8 -*-\n" + module_src, encoding="utf-8")
            jpath.unlink()  # remove the .json from staging so it never reaches dist
            generated_data_modules.add(py_rel)

            # Build an import hint for the user (dotted path from a top-level
            # package if applicable, else a bare module name).
            import_hint = _dotted_import_hint(staging, py_rel)
            json_module_map.append((jrel, py_rel, import_hint, enc_note))
            embedded_by_basename.setdefault(jpath.name, []).append({
                "rel": str(jrel), "modstem": mod_name, "dotted": import_hint,
            })

        print(f"--embed-json: converted {len(json_module_map)} JSON file(s) to "
              f"compiled data module(s):")
        for jrel, py_rel, hint, enc_note in json_module_map:
            print(f"    {jrel}  ->  {py_rel.with_suffix('.pyd')}   "
                  f"(import as: {hint}){enc_note}")

    # ------------------------------------------------------------------
    # Auto-rewrite json.load/json.loads of embedded files to import the compiled
    # data module (so callers don't have to be edited by hand). Runs after the
    # data modules exist and the .json files are gone. Skippable.
    # ------------------------------------------------------------------
    if embedded_by_basename and rewrite_json_loads:
        rw, broken, uncertain = _rewrite_json_loads(staging, embedded_by_basename,
                                                    generated_data_modules)
        if rw:
            print(f"Auto-rewrote {len(rw)} json.load/loads call(s) to use embedded "
                  f"data modules:")
            for line in rw:
                print(f"    {line}")
        if uncertain:
            print(f"⚠ {len(uncertain)} json.load/loads call(s) could not be classified "
                  f"(dynamic path or non-UTF-8 source) — review if they load an embedded file:")
            for line in uncertain:
                print(f"    {line}")
        if not rw and not broken and not uncertain:
            print("No json.load/loads calls referencing embedded files were found "
                  "to rewrite.")
        if broken:
            # These reference JSON that was embedded (and deleted from dist) but
            # could not be rewritten. The compiled program WOULD crash at runtime
            # with FileNotFoundError. Stop the build unless explicitly overridden.
            header = (f"{len(broken)} reference(s) to embedded JSON could not be "
                      f"rewritten and would crash at runtime:")
            for line in broken:
                print(f"    {line}")
            if allow_unrewritten_embedded_json:
                print(f"⚠ {header}")
                print("  Proceeding anyway because --allow-unrewritten-embedded-json was "
                      "given. The resulting binary will fail when it hits these calls "
                      "unless you fix them or the code paths are never executed.")
            else:
                # Clean up scratch dirs before aborting.
                for scratch in (staging, output.parent / (output.name + ".guard")):
                    try:
                        shutil.rmtree(scratch)
                    except Exception:
                        pass
                sys.exit(
                    f"\nBuild stopped: {header}\n"
                    + "\n".join(f"    {line}" for line in broken)
                    + "\n\nFix options:\n"
                    "  - edit those call sites to load the compiled data module "
                    "(e.g. `from <pkg>.<stem>_json import load`), or\n"
                    "  - drop those files from --embed-json so they ship as .json, or\n"
                    "  - re-run with --allow-unrewritten-embedded-json to build anyway "
                    "(NOT recommended; the binary will crash at those calls).\n"
                )

    # Write the guard into a SEPARATE staging-sibling directory so it is never
    # accidentally swept into a package's compilation (which happens if the
    # source root — or a subdir — is itself a package). The guard is compiled
    # on its own and placed at the OUTPUT ROOT, where the path-anchored
    # bootstrap injected into every module can find it by walking upward.
    guard_source = GUARD_TEMPLATE
    for k, v in parts.items():
        guard_source = guard_source.replace(k, str(v))
    # Inject the chained-assignment diagnostic into the guard (bundle-only) when
    # requested; otherwise strip the placeholder.
    guard_source = guard_source.replace(
        "__CAE_DIAGNOSTIC__",
        _CAE_DIAGNOSTIC_CODE if diagnose_chained_assignment else "")
    guard_staging = output.parent / (output.name + ".guard")
    if guard_staging.exists():
        shutil.rmtree(guard_staging)
    guard_staging.mkdir(parents=True)
    guard_file = guard_staging / "_lg.py"
    guard_file.write_text(guard_source, encoding="utf-8")

    # Inject the guard bootstrap into every user .py in staging (covers
    # exclude-compile files too — they carry the guard even though they are
    # shipped as source and not compiled). Generated JSON data modules are
    # pure data and are skipped — they do not process DataFrames.
    for py in staging.rglob("*.py"):
        rel = py.relative_to(staging)
        if rel in generated_data_modules:
            continue
        _inject_import(py)

    if output.exists():
        shutil.rmtree(output)
    output.mkdir(parents=True)

    # ------------------------------------------------------------------
    # Determine keep-as-py units.
    #
    # --exclude-compile can name individual files, but Nuitka compiles a
    # package as an indivisible unit. So any exclude-compile match that lands
    # inside a top-level package promotes that ENTIRE top-level package to
    # keep-as-source. A match on a standalone top-level module keeps just that
    # module as source. We compute, per staging-relative path, whether it must
    # be kept as source.
    # ------------------------------------------------------------------
    def _top_level_unit_of(rel: Path) -> Path:
        """Map a staging-relative file to the relative path of its top-level
        compile unit (the outermost package directory containing it, or the
        file itself if it is a standalone top-level module)."""
        parts = rel.parts
        # Walk from the root, find the first component that is a package dir.
        cur = staging
        for i, comp in enumerate(parts[:-1]):  # exclude filename
            cur = cur / comp
            if _is_package_dir(cur):
                # top-level package is everything up to and including this comp
                return Path(*parts[: i + 1])
        # No package ancestor -> standalone module (the file itself)
        return rel

    # Which top-level units are forced to source by --exclude-compile?
    keep_units: set[Path] = set()
    for py in staging.rglob("*.py"):
        rel = py.relative_to(staging)
        if _matches_or_child(rel, exclude_compile_rel):
            keep_units.add(_top_level_unit_of(rel))

    # Report promotions (a file-level exclude that pulled in a whole package)
    promoted = [u for u in keep_units if (staging / u).is_dir()]
    if promoted:
        print("Note: --exclude-compile matched files inside package(s); "
              "keeping the whole package as source (packages compile as a unit):")
        for u in sorted(promoted):
            print(f"    {u}/")

    # ------------------------------------------------------------------
    # Discover compile units across the staging tree, then split into
    # (compile) vs (ship-as-source) based on keep_units.
    # ------------------------------------------------------------------
    pkg_units, mod_units = _discover_compile_units(staging)

    compile_units: list[tuple[Path, Path, bool]] = []  # (target, out_dir, is_pkg)
    keep_as_source_units: list[Path] = []              # staging-relative

    # The guard is ALWAYS compiled and placed at the output root, regardless of
    # the user's layout. It lives in its own staging-sibling dir so it is never
    # part of a user package's compilation.
    compile_units.append((guard_file, output.resolve(), False))

    for pkg in pkg_units:
        rel = pkg.relative_to(staging)
        if rel in keep_units:
            keep_as_source_units.append(rel)
        else:
            out_dir = (output / rel.parent).resolve()
            out_dir.mkdir(parents=True, exist_ok=True)
            compile_units.append((pkg, out_dir, True))

    for mod in mod_units:
        rel = mod.relative_to(staging)
        if rel in keep_units or _matches_or_child(rel, exclude_compile_rel):
            keep_as_source_units.append(rel)
        else:
            out_dir = (output / rel.parent).resolve()
            out_dir.mkdir(parents=True, exist_ok=True)
            compile_units.append((mod, out_dir, False))

    # ------------------------------------------------------------------
    # Ship keep-as-source units verbatim (already injected in staging). Filter
    # out any bytecode caches so promoted-to-source packages stay clean.
    # ------------------------------------------------------------------
    def _bytecode_ignore(_dir: str, entries: list[str]) -> list[str]:
        return [e for e in entries
                if e in _BYTECODE_DIR_NAMES or Path(e).suffix in _BYTECODE_SUFFIXES]

    for rel in keep_as_source_units:
        src_path = staging / rel
        dest = output / rel
        if src_path.is_dir():
            shutil.copytree(src_path, dest, dirs_exist_ok=True, ignore=_bytecode_ignore)
        else:
            dest.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(src_path, dest)

    n_pkg = sum(1 for _, _, isp in compile_units if isp)
    n_mod = sum(1 for _, _, isp in compile_units if not isp)
    print(f"Compiling {len(compile_units)} unit(s) with Nuitka "
          f"({n_pkg} package(s), {n_mod} module(s), jobs={jobs})"
          + (f"; keeping {len(keep_as_source_units)} unit(s) as source"
             if keep_as_source_units else "")
          + "...")

    failed: list[tuple[Path, str]] = []
    with _cf.ThreadPoolExecutor(max_workers=jobs) as ex:
        futs = {
            ex.submit(_compile_unit, tgt, od, isp): tgt
            for (tgt, od, isp) in compile_units
        }
        for i, fut in enumerate(_cf.as_completed(futs), 1):
            tgt, rc, msg = fut.result()
            status = "ok" if rc == 0 else "FAIL"
            try:
                label = str(tgt.relative_to(staging))
            except ValueError:
                label = tgt.name  # e.g. the guard file (_lg.py), outside staging
            print(f"  [{i}/{len(compile_units)}] {status}  {label}")
            if rc != 0:
                failed.append((tgt, msg))

    if failed:
        print("\nOne or more units failed to compile:")
        for tgt, msg in failed:
            print(f"\n{tgt}:\n{msg}")
        sys.exit(1)

    # ------------------------------------------------------------------
    # Copy non-.py files that are NOT inside a compiled package (compiled
    # packages already bundle their own data via Nuitka, and copying loose
    # .py from a compiled package would shadow the compiled version). We copy:
    #   * non-.py files at top level or inside non-package directories
    #   * everything inside keep-as-source units (handled above via copytree)
    # Data files inside a compiled package are copied so runtime file access
    # (e.g. importlib.resources) still finds them.
    # ------------------------------------------------------------------
    compiled_pkg_rel = {tgt.relative_to(staging) for (tgt, _, isp) in compile_units if isp}

    def _inside_compiled_pkg(rel: Path) -> bool:
        for pkg_rel in compiled_pkg_rel:
            if rel == pkg_rel or pkg_rel in rel.parents:
                return True
        return False

    def _inside_keep_source_unit(rel: Path) -> bool:
        for u in keep_as_source_units:
            if rel == u or u in rel.parents:
                return True
        return False

    for other in staging.rglob("*"):
        if not other.is_file():
            continue
        rel = other.relative_to(staging)
        # Never ship Python bytecode caches (defense in depth; staging copy
        # already excludes them, but a build step could have created some).
        if _is_bytecode_artifact(rel):
            continue
        if _inside_keep_source_unit(rel):
            continue  # already copied wholesale
        if other.suffix == ".py":
            # Loose .py belonging to a compiled package must NOT be shipped as
            # source (the compiled package supersedes it). Loose .py that is a
            # standalone compiled module also must not be shipped as source.
            continue
        # Non-.py data file: copy it (including data inside compiled packages).
        dest = output / rel
        dest.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(other, dest)

    # Final safety sweep: remove any bytecode caches that slipped into output
    # (e.g. created by a Nuitka subprocess importing something in a data dir).
    for pyc in list(output.rglob("*.pyc")) + list(output.rglob("*.pyo")):
        try:
            pyc.unlink()
        except Exception:
            pass
    for pycache in output.rglob("__pycache__"):
        try:
            shutil.rmtree(pycache)
        except Exception:
            pass

    # Clean up staging scratch directories.
    for scratch in (staging, guard_staging):
        try:
            shutil.rmtree(scratch)
        except Exception:
            pass

    # Warn if any .json remains in dist (i.e. not covered by --embed-json). This
    # surfaces leftover JSON the user may have wanted embedded.
    remaining_json = sorted(str(p.relative_to(output)) for p in output.rglob("*.json"))

    print(f"\n✓ Built {output.resolve()}")
    print(f"  Cutoff embedded (UTC): {datetime.fromtimestamp(cutoff_epoch, tz=timezone.utc)}")
    print(f"  Compiled units: {len(compile_units)} ({n_pkg} package(s), {n_mod} module(s))")
    if json_module_map:
        print(f"  JSON embedded as compiled data modules: {len(json_module_map)}")
    if keep_as_source_units:
        print(f"  Kept as source (injected, not compiled): {len(keep_as_source_units)} unit(s)")
    if exclude_all_rel:
        print(f"  Fully excluded: {len(exclude_all_rel)} path(s)")
    if remaining_json:
        print(f"\n  ⚠ {len(remaining_json)} .json file(s) remain in dist (not embedded):")
        for j in remaining_json:
            print(f"      {j}")
        print("    To embed these as compiled data modules, add e.g. "
              "--embed-json \"**/*.json\"")
    if diagnose_chained_assignment:
        print("  NOTE: chained-assignment diagnostic is COMPILED INTO this build — it "
              "prints to stderr on every ChainedAssignmentError. Rebuild without "
              "--diagnose-chained-assignment for a clean release.")
    print(f"\n  Ship the '{output.name}/' directory as-is.")

def main() -> None:
    p = argparse.ArgumentParser(
        description="Build a data-driven time-locked distribution with Nuitka Free.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    p.add_argument("--cutoff", required=True, help="YYYY-MM-DD (interpreted as end-of-day UTC)")
    p.add_argument("--src",    required=True, type=Path, help="Directory containing your source .py files")
    p.add_argument("--output", default=Path("dist"), type=Path)
    p.add_argument("--jobs",   default=4, type=int, help="Parallel compile jobs (default 4)")
    p.add_argument("--exclude-all", action="append", default=[], metavar="GLOB",
                   help="Glob (relative to --src) whose matches are completely omitted "
                        "from the build. Repeatable.")
    p.add_argument("--exclude-compile", action="append", default=[], metavar="GLOB",
                   help="Glob (relative to --src) whose matches receive the `import _lg` "
                        "injection and are shipped as-is (.py) rather than compiled. "
                        "Repeatable.")
    p.add_argument("--embed-json", action="append", default=[], metavar="GLOB",
                   help="Glob (relative to --src) selecting .json files to embed as "
                        "compiled data modules (<stem>_json.pyd) instead of copying "
                        "them as files. The original .json is removed from dist. "
                        "Use \"**/*.json\" to embed all JSON. Repeatable. NOTE: code "
                        "that read the file via json.load(open(...)) must switch to "
                        "`from <stem>_json import DATA` (or `.load()`).")
    p.add_argument("--json-encoding", default=None, metavar="ENC",
                   help="Force a specific text encoding when reading --embed-json "
                        "files (e.g. cp1252, latin-1, utf-8-sig). Default: auto-detect "
                        "(BOM, then utf-8, then the OS locale encoding, then cp1252, "
                        "then latin-1).")
    p.add_argument("--no-rewrite-json-loads", action="store_true",
                   help="Disable automatic rewriting of json.load()/json.loads() calls "
                        "that read an embedded JSON file. By default such calls are "
                        "rewritten to import the compiled data module instead.")
    p.add_argument("--allow-unrewritten-embedded-json", action="store_true",
                   help="Build even if some json.load/open of an embedded (removed) JSON "
                        "could not be rewritten. NOT recommended: the resulting binary "
                        "will raise FileNotFoundError at those call sites. By default the "
                        "build stops and lists them.")
    p.add_argument("--diagnose-chained-assignment", action="store_true",
                   help="Inject a runtime diagnostic into the bundle that prints, to "
                        "stderr, the column name and pandas-attributed file:line for every "
                        "ChainedAssignmentError. Bundle-only (dev runs the raw source, "
                        "unaffected). Off by default. Use it to locate the assignments to "
                        "convert to .assign(); pair it with a dev-side "
                        "filterwarnings('error', ChainedAssignmentError) gate to catch "
                        "genuine chained assignments accurately.")
    args = p.parse_args()
    build(args.cutoff, args.src.resolve(), args.output.resolve(), args.jobs,
          args.exclude_all, args.exclude_compile, args.embed_json, args.json_encoding,
          rewrite_json_loads=not args.no_rewrite_json_loads,
          allow_unrewritten_embedded_json=args.allow_unrewritten_embedded_json,
          diagnose_chained_assignment=args.diagnose_chained_assignment)

if __name__ == "__main__":
    main()
