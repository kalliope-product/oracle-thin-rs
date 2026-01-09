# Environment Directives

## Python Environment
- **Always activate venv first**: `source /home/ec2-user/python_env/dev/bin/activate`
- **Use `uv` for all Python operations** - not raw pip
  - Install: `uv pip install <package>`
  - Run tests: `uv run pytest`
  - Sync deps: `uv pip sync requirements.txt`
  - Run scripts: `uv run path/to/script.py`
- **Python already created, do not try to recreate**
  - If venv is missing, ask: "Can you recreate it with `uv venv /home/ec2-user/python_env/dev -p=3.12`?"

## External Code
- **Never fetch from GitHub URLs** - ask user to clone locally instead
- Reference repos belong in project subdirectories (e.g., `python-ref/`)
- If a repo isn't cloned yet, ask: "Can you clone X to `<suggested-path>`?"

## Rust Environment
- Use stable toolchain unless feature requires nightly
- `cargo fmt` before commits