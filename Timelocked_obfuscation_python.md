# Time-Locked Obfuscated Python Build — two flavors

Bake a cutoff date into your compiled Python code so it raises
`RuntimeError("License Expired")` when the pandas / parquet data it
processes contains samples strictly after the cutoff. The system clock
is never read.

## Two scripts, same behavior

| Script | Obfuscator | Prereqs | Notes |
|---|---|---|---|
| `build_time_locked.py` | PyArmor Free | `pip install pyarmor` | Bytecode encryption. Fast build. 32 KB per-code-object cap. |
| `build_time_locked_nuitka.py` | Nuitka Free | `pip install nuitka` + a C compiler | Real native extension modules. Slower build. No size cap. |

Both produce a `dist/` directory shaped like your input directory, with
`_lg` next to your own modules. Import from `dist/` as you would from
your original source; the guard is auto-installed on the first import.

## Usage

PyArmor variant:
```
python build_time_locked.py \
    --cutoff 2027-01-31 \
    --src /path/to/your/mycode \
    [--output dist]
```

Nuitka variant:
```
python build_time_locked_nuitka.py \
    --cutoff 2027-01-31 \
    --src /path/to/your/mycode \
    [--output dist] \
    [--jobs 4]
```

## What both scripts do

1. Convert the cutoff date to POSIX seconds (UTC, end of day).
2. Split it into six large integers `(A, B, C, D, E, SALT)` such that
   `((C*D + E) ^ B ^ A ^ SALT) == cutoff`. No plaintext form of the
   date or the epoch integer appears in the generated `_lg.py`.
3. Generate `_lg.py` with the guard logic.
4. Inject `import _lg` at the top of every `.py` in the source
   (respecting shebang / encoding line / module docstring / `from
   __future__` imports).
5. Run the obfuscator (PyArmor or Nuitka).
6. Ship the resulting `dist/`.

## What the guard hooks into

`pd.read_parquet`, `pd.read_csv`, `pd.read_feather`, `pd.read_hdf`,
`pd.read_sql*`, `pd.read_orc`, `pd.DataFrame.set_index`, and
`pyarrow.parquet.read_table`. It inspects the `DatetimeIndex` on the
index (and each level of a `MultiIndex`) and every column with
`datetime64*` dtype.

If some of your code paths build DataFrames from raw numpy without
touching these entry points, add an explicit call inside the critical
function:

```python
import _lg
_lg.check(df)             # DataFrame, Series, or DatetimeIndex
_lg.check("path.parquet") # reads only parquet stats — no full load
```

## Picking between them

- **PyArmor Free** if the build machine has no C compiler, if build
  speed matters, and if your individual functions all fit under
  ~32 KB of bytecode.
- **Nuitka Free** if you have a C toolchain, if you want real native
  code with no runtime library, and if any of your functions might
  exceed the PyArmor Free per-code-object cap.

Both give roughly equivalent protection against a casual attacker.
Neither is unbreakable — a determined reverse engineer with a debugger
can extract the cutoff from a live process's memory. If that threat
matters, use PyArmor Pro (BCC + RFT) or Nuitka Commercial (data-hiding
plugin), both of which are drop-in extensions to their respective free
versions.

## A third free option: Cython

If you already Cython-ize hot paths, Cython can compile `_lg.py` and
your own modules to `.so` / `.pyd` too. The protection profile is close
to Nuitka Free (native code, no string encryption), but the build
requires you to structure your project as installable packages with
`setup.py` / `pyproject.toml`, which is more work. Nuitka Free is the
easier drop-in.

## Excluding files from compilation (Nuitka variant)

Use `--exclude` (repeatable) or `--exclude-from FILE` to keep specified files
as-is in the output — no `import _lg` injection, no Nuitka compilation.

```bash
# Single file
python build_time_locked_nuitka.py --cutoff 2027-01-31 --src ./mycode \
    --exclude "legacy_helpers.py"

# Whole directory
python build_time_locked_nuitka.py --cutoff 2027-01-31 --src ./mycode \
    --exclude "vendor"

# Recursive glob + multiple flags
python build_time_locked_nuitka.py --cutoff 2027-01-31 --src ./mycode \
    --exclude "vendor/**/*.py" --exclude "**/legacy_*.py"

# From a file (one pattern per line, # for comments)
python build_time_locked_nuitka.py --cutoff 2027-01-31 --src ./mycode \
    --exclude-from excludes.txt
```

Pattern rules: `*` matches within one path segment, `**` matches any number of
segments, and a bare directory name expands to every `.py` under it. `_lg.py`
is never excluded regardless of patterns.

**Caveat:** the guard installs on the first import of *any compiled* module.
If you exclude your program's entry-point script, add `import _lg` manually
at its top — otherwise the check never fires.


## New in the Nuitka build: `--exclude-all` and `--exclude-compile`

Both flags are repeatable and take a glob pattern relative to `--src`.
Glob semantics follow `pathlib.Path.glob` — `*` matches within one path
component; `**` matches any depth.

`--exclude-all GLOB` — matched paths are **completely omitted** from the
build. Not copied to staging, no guard injection, no compilation. Use for
tests, scratch files, private docs, anything the customer should never
see.

`--exclude-compile GLOB` — matched `.py` files **get the `import _lg`
injection** and are **shipped as plain `.py`** (not passed to Nuitka).
The runtime guard still applies (they still trigger the "License Expired"
check when they touch expired data), but they remain human-readable.
Useful for modules Nuitka can't reliably compile (heavy metaclass magic,
complex dynamic imports) or that you want kept readable for support.

The guard module `_lg.py` is always compiled — it can never be
`--exclude-compile`d, since protecting the cutoff is the whole point.

### Example

```bash
python build_time_locked_nuitka.py \
    --cutoff 2027-01-31 \
    --src ./mypkg \
    --exclude-all "tests/**" \
    --exclude-all "**/scratch_*.py" \
    --exclude-compile "plugins/*.py" \
    --exclude-compile "utils.py"
```

produces a `dist/` where:
- Everything under `tests/` and any `scratch_*.py` anywhere is gone.
- `plugins/*.py` and `utils.py` are present as `.py`, with `import _lg`
  injected.
- Every other `.py` becomes a `.so` / `.pyd` native extension.
- Non-`.py` files (CSV, JSON, docs, models) are copied verbatim.

### Directory-pattern note

`Path.glob("tests/**")` returns only the directories under `tests/` (not
their contained files) — that's Python's glob behavior. The script's
ancestor-check logic then correctly excludes every file inside those
directories, so `tests/**` works the way you'd expect a `.gitignore`
entry to work. If you want to be explicit, `tests/**/*.py` matches every
`.py` file directly.


## Nuitka build: package-aware compilation and robust injection (updated)

Two classes of error are now fixed in `build_time_locked_nuitka.py`:

**`from __future__ imports must occur at the beginning of the file`.**
The guard is no longer injected by string/line heuristics. The script now
parses each module with Python's own `ast` grammar to find the exact end of
the module docstring and the last `from __future__` import, and inserts the
guard bootstrap immediately after — always a legal position, for every
quoting style, string prefix (`r"""`, etc.), implicit concatenation, and
comment arrangement. Verified against 16 header layouts.

**`to compile a package, specify its directory, not the '__init__.py'`.**
The script no longer compiles files one-by-one. It detects *compile units*:
a top-level package (a directory with `__init__.py`) is compiled as a single
unit via `nuitka --mode=package <dir>`, and a top-level module via
`nuitka --mode=module <file>`. `__init__.py` is never handed to Nuitka
directly, and package submodules are never compiled in isolation (so their
relative imports keep working).

### `--src` should point at the parent of your packages

Point `--src` at the directory you would put on `sys.path` — the parent of
your top-level packages/modules. If `--src` is itself a package (has
`__init__.py`), the script detects that and compiles it as one package unit
under its real name (so you get `mypkg.so`, not `dist.so`).

### How `--exclude-compile` interacts with packages

Because a package compiles as an indivisible unit, an `--exclude-compile`
match on any file *inside* a top-level package promotes that **whole
package** to shipped-as-source (the script prints a note when this happens).
An `--exclude-compile` match on a standalone top-level module keeps just that
module as source. `--exclude-all` is unchanged: matches are dropped from the
build entirely.

### How the guard is found at runtime

The injected bootstrap is path-anchored: at import time it walks upward from
the module's own location to find `_lg` (compiled `_lg.pyd`/`_lg.so`, or
`_lg.py` source) and loads it directly. This works at any nesting depth,
whether your code is imported as a package or as loose modules, and whether
`_lg` is compiled or shipped as source — no `sys.path` assumptions. `_lg` is
always compiled and placed at the output root.


## Nuitka build: no `.pyc` and no `.json` in dist (updated)

**Bytecode caches are always stripped.** `__pycache__` directories and any
`.pyc` / `.pyo` files are now excluded at every stage — during staging,
during the data-file copy, and via a final sweep of `dist`. You never need to
clean them by hand, and a stray `.pyc` (which is a decompilable copy of
source) can no longer leak into the deliverable.

**`--embed-json GLOB` turns JSON into compiled data modules.** Each matched
`foo/bar.json` is converted to `foo/bar_json.py` holding the parsed data as
native Python constants, compiled to `bar_json.pyd` along with the rest, and
the original `.json` is removed from `dist`. Repeatable.

```bash
python build_time_locked_nuitka.py --cutoff 2027-01-31 --src .\yourcode \
    --embed-json "**/*.json"        # embed ALL json -> zero .json in dist
```

To embed only some (e.g. keep customer-editable config as a file), pass
specific globs: `--embed-json "config/secret_*.json"`. Any `.json` left in
`dist` triggers a build-time warning listing the files.

### Required code change when you embed a JSON file

Embedding changes the access from a file read to an import — this is
unavoidable with Nuitka Free (Nuitka compiles code, not data files). The
build prints the exact import line for each embedded file, e.g.:

```
pkg/config.json  ->  pkg/config_json.pyd   (import as: pkg.config_json)
```

Update your code accordingly:

```python
# before
import json
cfg = json.load(open("pkg/config.json"))

# after
from pkg.config_json import DATA as cfg      # or: from pkg.config_json import load; cfg = load()
```

Notes:
- The generated module is named `<stem>_json` (so `config.json` →
  `config_json`); if a file of that name already exists the build stops with a
  clear message rather than overwriting.
- Data is embedded as Python constants (integers, dict-building code), which
  is more protected than an embedded JSON string. With **Nuitka Free** these
  constants are not encrypted — hard to reach, but not cryptographically
  hidden. Nuitka Commercial's data-hiding plugin encrypts constants if the
  data is truly secret.
- If an embedded JSON lives inside a package that gets **promoted to source**
  (because of `--exclude-compile` on any file in that package), its generated
  data module ships as readable source too. Keep secret JSON out of
  promoted-to-source packages.
- Generated data modules are pure data, so the license guard is intentionally
  not injected into them.


## `--embed-json` now handles non-UTF-8 files

If an embedded JSON file is saved in a non-UTF-8 encoding (common on Windows,
where editors default to cp1252 / Windows-1252), the reader no longer fails
with a `'utf-8' codec can't decode byte 0x..` error. It auto-detects the
encoding in this order: an explicit `--json-encoding` if given, then a
byte-order mark (UTF-8-BOM, UTF-16/32), then UTF-8, then the OS locale
encoding (what Python's `open()` uses by default), then cp1252, then latin-1.
The chosen encoding is printed next to each converted file (e.g.
`[decoded as cp1252]`). Characters like `°`, `µ`, `±` are preserved and the
generated module is always written as UTF-8.

Force a specific encoding when needed:

```bash
python build_time_locked_nuitka.py --cutoff 2027-01-31 --src .\yourcode \
    --embed-json "**/*.json" --json-encoding cp1252
```


## `--embed-json` now auto-rewrites `json.load()` callers

When you embed JSON with `--embed-json`, the build now also **rewrites the code
that read those files** so you don't have to edit each call by hand. This is on
by default whenever `--embed-json` is used; disable with
`--no-rewrite-json-loads`.

Patterns detected and rewritten (only when the path is a string literal whose
basename matches an embedded file):

```python
data = json.load(open("config.json"))              # -> data = _embedded_config_json.load()
data = json.loads(open("config.json").read())      # -> data = _embedded_config_json.load()
data = json.loads(Path("config.json").read_text()) # -> data = _embedded_config_json.load()
with open("config.json") as f:                     # -> data = _embedded_config_json.load()
    data = json.load(f)
```

It also recognises `import json as J` / `from json import load` aliases and
`pathlib.Path`. A matching `import <pkg>.<stem>_json as _embedded_<stem>_json`
is added after the module's `__future__`/docstring header. The generated
`load()` returns a **fresh deep copy** each call, matching `json.load`'s
semantics (so code that mutates the result is unaffected).

What it deliberately does NOT touch, and reports for a manual look instead:
- `json.load(open(var))` where the path is a variable/expression (can't match it
  to a file statically),
- `with` blocks whose body is more than the single load,
- an ambiguous basename (same filename embedded from two directories, loaded by
  bare basename — a path that disambiguates, like `a/config.json`, is rewritten),
- non-UTF-8 source files.

Loads of JSON files you did **not** embed are left exactly as they were. The
build prints a summary of every call it rewrote and every one you should review.

> The rewrite happens only on the staged copy that gets compiled — your original
> source files are never modified.


## `--embed-json` auto-rewrite now covers file handles and dynamic paths

The auto-rewriter was extended to handle the two shapes that dominate real code
and were previously skipped:

**File-handle loads** — the classic `with` block, including multi-statement
bodies:

```python
with open("config.json") as f:      # -> if True:
    ...                             #        ...
    data = json.load(f)            #        data = _embedded_config_json.load()
```

It traces the handle (`f`, `fh`, …) back to its `open()` and neutralises the
now-dead open (the `with` header becomes `if True:`, preserving the body). It
also handles the simple assignment form `f = open(...); data = json.load(f);
f.close()`.

**Dynamic paths** — the filename literal inside a path expression is used to
match the embedded file:

```python
json.load(open(os.path.join(HERE, "config.json")))
json.load(open(Path(__file__).parent / "config.json"))
with open(os.path.join(BASE, "config.json")) as f: json.load(f)
```

Safety rules (unchanged philosophy — never break code silently):
- A file handle is only rewritten if it is used *solely* by the load (plus an
  optional `.close()`). If it's used again (`f.seek(0)`, `f.name`, passed
  elsewhere), the call is left as-is and flagged for a manual look.
- Non-file `json.loads(...)` is never touched — e.g.
  `json.loads(meta[b"pandas"])` (parquet metadata) or `json.loads(resp.read())`
  stay exactly as they were.
- Fully-dynamic paths (filename only known at runtime) are left alone; if they
  live in `--exclude-all` folders they aren't scanned at all.
- Loads of JSON files you did not embed are left untouched.

Reused handles across independent `with` blocks in the same function are each
rewritten correctly. The build still prints a summary of every call rewritten
and every one you should review.
