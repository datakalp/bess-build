# bess-build
Setup and Instructions to Build BESS Platform and Vis code

# Installing Nuitka on Windows 11 laptop
winget install Python.Python.3.12
winget install Microsoft.VisualStudio.2022.BuildTools --override "--wait --add Microsoft.VisualStudio.Workload.VCTools --includeRecommended"
pip install nuitka

cd .\Dell-laptop-bkup-13-May-2025\Datakalp\Datakriti\ANIL\CogniBESS\time_locked_build\

# Command to build while keeping JSONs open
python build_time_locked_nuitka.py `
    --cutoff 2026-08-19 `
    --src "C:/temp/BESS_framework" `
    --exclude-all "Test_suites/**" `
    --exclude-all "outputs/**" `
    --exclude-all "*.md" `
    --exclude-all "**/*.md" `
    --exclude-compile "run_*.py" `
    --exclude-compile "soc_energy_balance_check.py" `
    --exclude-compile "pcs_energy_soc_balance.py" `
    --exclude-compile "dkvis_exploration_tool.py"

# Command to build including JSONs (does not work yet)
python build_time_locked_nuitka.py `
    --cutoff 2026-08-19 `
    --src "C:/temp/bess-platform-0.9.4/BESS_framework" `
    --exclude-all "Test_suites/**" `
    --exclude-all "outputs/**" `
    --exclude-all "*.md" `
    --exclude-all "**/*.md" `
    --exclude-compile "./*.py" `
    --exclude-compile "exploratory dashboard/dkvis_exploration_tool.py" `
    --embed-json "**/*.json"
    

# install uv without admin (or: pip install uv / pipx install uv)
powershell -c "irm https://astral.sh/uv/install.ps1 | iex"

uv python install 3.12        # fetches a standalone CPython 3.12 into %LOCALAPPDATA%\uv
uv venv --python 3.12 myenv   # venv pinned to that version
myenv\Scripts\activate

# Why JSONs cannot be bundled yet
    core_libraries\bess_6d_analysis.py:795  json.load(<handle>) opens a fully-dynamic path — handle manually
    core_libraries\bess_chunker.py:181  json.load(<handle>) opens a fully-dynamic path — handle manually
    core_libraries\chunked_6d_builder.py:73  json.load(<handle>) opens a fully-dynamic path — handle manually
    core_libraries\plant_context.py:68  json.load(<handle>) opens a fully-dynamic path — handle manually

