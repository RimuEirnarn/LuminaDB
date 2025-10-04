@echo off
call .venv\Scripts\activate.bat

pylint --rcfile ./dev-config/pylint.toml luminadb
pytest --config-file ./dev-config/pytest.ini