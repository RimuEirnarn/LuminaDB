# Quick Start

**LuminaDB** is a lightweight wrapper around Python's built-in `sqlite3`. It gives you two ways to work
with a SQLite database without writing SQL by hand, and you can mix them freely in the same program.

> The library is still pre-1.0. It favours being easy to use over being fast, so it suits scripts, small
> apps, bots, CLIs and prototypes. For hot paths, drop down to `db.sql`, the raw `sqlite3` connection.

## Install

```bash
pip install luminadb
```

LuminaDB needs Python 3.11 or newer and has no dependencies besides the standard library. The version
these docs describe is 0.8.1:

```python
import luminadb

luminadb.__version__  # -> '0.8.1'
```

## Two ways to work

| | Table API | Model API |
| --- | --- | --- |
| **Style** | Dictionaries and column builders | Python classes (dataclasses) |
| **You write** | `users.insert({"name": "Ann"})` | `User.create(name="Ann")` |
| **Rows are** | `Row` dicts with attribute access | Instances of your class |
| **Extras** | SQL functions, subqueries, CSV, pagination | Hooks, validators, relationships, scopes |
| **Read** | [Table API Guide](SimpleGuide.md) | [Model API Guide](ModelAPI.md) |

A model's table is a normal Table API object, so `User.get_table().select(...)` works whenever you need
the lower-level tools.

## Table API in one minute

```python
from luminadb import Database, integer, text, op

db = Database(":memory:")        # use "app.db" for a file

users = db.create_table("users", [
    integer("id").primary().auto_increment(),
    text("name"),
    integer("age").default(18),
])

users.insert({"name": "Ann", "age": 30})  # -> 1
users.insert_many([{"name": "Bo", "age": 22}, {"name": "Cy", "age": 41}])

users.select({"age": op > 25}, what="name")  # -> ['Ann', 'Cy']
users.update({"name": "Bo"}, {"age": 23})  # -> 1
users.delete({"name": "Cy"})  # -> 1
users.select(what="name")  # -> ['Ann', 'Bo']
```

## Model API in one minute

```python
from uuid import uuid4
from luminadb import model, BaseModel, Primary, Unique

@model(db)
class Account(BaseModel):
    __schema__ = (Primary("id"), Unique("email"))
    __auto_id__ = lambda: str(uuid4())

    id: str
    email: str
    tier: str = "free"


me = Account.create(email="me@example.com")
me.tier  # -> 'free'
Account.first(email="me@example.com").id == me.id  # -> True
me.update(tier="pro").tier  # -> 'pro'
Account.where(tier="pro").count()  # -> 1
me.delete()
Account.count()  # -> 0
```

## Good habits

- **Create one `Database` and share it.** Put it in a small `db.py` and import it where you need it.
- **Pick simple names.** Table, column and field names may use letters, digits and `_`, and may not be
  SQL keywords such as `order`, `group` or `key`.
- **Turn on foreign keys** with `db.foreign_pragma("ON")` if you use relationships. SQLite leaves them
  off by default.
- **Check [Known Issues](Troubleshooting.md)** for the rough edges in this release.

## Where next

1. [Table API Guide](SimpleGuide.md): columns, conditions, sorting, functions, subqueries, CSV.
2. [Model API Guide](ModelAPI.md): models, relationships, hooks, validators, bulk operations.
3. [Database Options and Transactions](DatabaseModes.md): `strict`, `forgive`, threads, transactions.
4. [Known Issues and Troubleshooting](Troubleshooting.md): bugs in 0.8.1 with workarounds, and a table of common errors.
5. [API Reference](api_reference.rst): generated from the docstrings.
