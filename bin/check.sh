#!/bin/sh

if [ ! -f .venv/bin/activate ]; then
    echo "Cannot found .venv: please do python3 -m venv .venv"
    echo "Make sure your python version is python3.11 or later"
    exit 1
fi

. .venv/bin/activate

if ! pylint --rcfile ./dev-config/pylint.toml luminadb; then
    echo "Pylint error"
    exit 1
fi
pytest --config-file ./dev-config/pytest.ini
