# LuminaDB

<div align="center">

![GitHub forks](https://img.shields.io/github/forks/RimuEirnarn/luminadb?style=social)
![GitHub Repo stars](https://img.shields.io/github/stars/RimuEirnarn/luminadb?style=social)

![GitHub closed issues](https://img.shields.io/github/issues-closed-raw/RimuEirnarn/luminadb)
![GitHub closed pull requests](https://img.shields.io/github/issues-pr-closed/RimuEirnarn/luminadb)
![GitHub code size in bytes](https://img.shields.io/github/languages/code-size/RimuEirnarn/luminadb)
![GitHub all releases](https://img.shields.io/github/downloads/RimuEirnarn/luminadb/total)
![GitHub Workflow(pylint) Status](https://img.shields.io/github/actions/workflow/status/RimuEirnarn/luminadb/pylint.yml?label=lint)
![GitHub Workflow(pytest) Status](https://img.shields.io/github/actions/workflow/status/RimuEirnarn/luminadb/pytest.yml?label=tests)
![GitHub Workflow(pypi) Status](https://img.shields.io/github/actions/workflow/status/RimuEirnarn/luminadb/python-publish.yml)
![GitHub release (latest by date)](https://img.shields.io/github/v/release/RimuEirnarn/luminadb)
[![Documentation Status](https://readthedocs.org/projects/luminadb/badge/?version=latest)](https://luminadb.readthedocs.io/en/latest/?badge=latest)
![GitHub](https://img.shields.io/github/license/RimuEirnarn/luminadb)
![CodeFactor Grade](https://img.shields.io/codefactor/grade/github/RimuEirnarn/luminadb)

![PyPI - Format](https://img.shields.io/pypi/format/luminadb)
![PyPI - Python Version](https://img.shields.io/pypi/pyversions/luminadb?label=min%20python)
![PyPI - Implementation](https://img.shields.io/pypi/implementation/luminadb)
![PyPI - Downloads](https://img.shields.io/pypi/dm/luminadb?label=%28PyPI%29%20downloads)
![PyPI - Downloads Daily](https://img.shields.io/pypi/dd/luminadb?label=(PyPI)%20downloads%20daily)

</div>

**LuminaDB** (previously known as SQLite Database) is a lightweight, developer-friendly wrapper for SQLite designed to feel as intuitive as Laravel's Eloquent ORM, but in Python.

> [!WARNING]
> ⚠️ This library is still pre-1.0, which means it's not optimized for high performance or low memory usage (yet). Use with care. If you run into serious issues, feel free to open an issue, we’re listening.

---

## 🚀 Usage & Demo

Curious how it works in action?  
Check out the live example here: [luminadb demo](https://github.com/RimuEirnarn/LuminaDB_demo)

---

## 📦 Installation

The library is available via PyPI:

```sh
pip install luminadb
```

Prefer to install directly from GitHub? You can still do this the old-school way:

```sh
pip install https://github.com/RimuEirnarn/LuminaDB/archive/refs/tags/<latest-version>.zip
```

---

## ✨ Features

A quick feature overview is available in [Features.md](https://github.com/RimuEirnarn/LuminaDB/blob/main/docs/SimpleGuide.md)

Or check out the full short docs at:
[luminadb.rtfd.io](https://luminadb.rtfd.io/)

---

## 📖 Origin Story & Acknowledgements

Wondering why this exists?  
Read the [History.md](History.md) to learn what led to the birth of this project.

> Pre-contributor: just ChatGPT, so blame the AI if anything’s weird.

---

## 🤝 Contributing

Found a bug? Got an idea? Want to improve something?

- Open an issue for anything noteworthy.
- PRs are welcome as long as they align with the project's vision and design goals.

---

## 🛠️ Development Setup

Thanks for considering contributing to LuminaDB! Here's what you'll need:

- **Testing**: `pytest`
- **Linting**: `pylint`
- **Docs**: `sphinx`

Dependencies are split between:
- `dev-requirements.txt` (core development)
- `docs-requirements.txt` (documentation)

To get started:

```sh
git clone https://github.com/RimuEirnarn/luminadb
cd luminadb

python -m venv .venv
source .venv/bin/activate  # or `.venv\Scripts\activate` on Windows

pip install -r ./dev-requirements.txt
./bin/check.sh
```

The `check.sh` script will run:

```sh
pylint --rcfile ./dev-config/pylint.toml luminadb
pytest --config-file ./dev-config/pytest.ini
```

Simple and clean.

---

## 📄 License

This project is licensed under the **BSD 3-Clause "New" or "Revised" License**.

Read the full license here:  
[LICENSE](https://github.com/RimuEirnarn/LuminaDB/blob/main/LICENSE)

## Short note

Either you can call this project: Library, ORM, ORM-lite, or driver is up to you. This is a high-level abstraction built on top of standard sqlite3.

---
