# Database Options, Threading and Transactions

This page covers how a `Database` behaves: the options you can pass when opening one, how it
handles threads and shutdown, and how transactions work.

> **About the examples.** The examples on this page were run against LuminaDB 0.8.1. A value after
> `# ->` is the real result, copied from that run.

## Contents

- [Opening a database](#opening-a-database)
- [`strict` and `forgive`](#strict-and-forgive)
- [Lifecycle](#lifecycle)
- [Threads](#threads)
- [Transactions](#transactions)
- [Pragmas and maintenance](#pragmas-and-maintenance)
- [Talking to SQLite directly](#talking-to-sqlite-directly)
- [Debugging SQL](#debugging-sql)

---

## Opening a database

```python
from luminadb import Database, integer, text, op

db = Database(":memory:")      # lives in RAM, vanishes at exit
# db = Database("app.db")      # lives in a file, survives restarts
db.path  # -> ':memory:'
```

`Database(path, **kwargs)` accepts two LuminaDB options (`strict`, `forgive`, below). Everything else
is passed to `sqlite3.connect`, so options such as `timeout=10` work as they do in the standard library.

LuminaDB has a single `Database` class. Earlier versions of these docs described a `DatabaseWorker`
class in a `luminadb.workers` module. That module does not exist in 0.8.1; see [Threads](#threads)
for how to use a database from several threads.

---

## `strict` and `forgive`

Two flags change how forgiving the table-management methods are. Both default to `True`.

| Flag | When `True` (default) | When `False` |
| --- | --- | --- |
| `strict` | `db.table("missing")` raises `DatabaseMissingError`. | `db.table("missing")` returns a `Table` object for a table that does not exist yet, and errors appear when you use it. |
| `forgive` | `db.create_table(...)` on an existing name returns the existing table. | The same call raises `DatabaseExistsError`. |

```python
from luminadb.errors import DatabaseExistsError, DatabaseMissingError

try:
    db.table("ghost")
except DatabaseMissingError as exc:
    print(exc)    # table ghost does not exists.

users = db.create_table("users", [integer("id"), text("name")])
db.create_table("users", [integer("id"), text("name")]).name  # -> 'users'

picky = Database(":memory:", forgive=False)
picky.create_table("a", [integer("id")])
try:
    picky.create_table("a", [integer("id")])
except DatabaseExistsError as exc:
    print(exc)    # table a already exists.
```

The defaults are the right choice for scripts that run repeatedly against the same file. Turn
`forgive` off in migrations or tests where an existing table is a mistake you want to hear about.

---

## Lifecycle

- `db.close()` closes the connection. Calling it twice is harmless.
- `db.closed` tells you whether it has been closed. Using a closed database raises
  `sqlite3.ProgrammingError`.
- Every `Database` registers itself to be closed when the interpreter exits, so a forgotten `close()`
  will not leave a file locked.
- In-memory databases are tied to their connection. Once closed, the data is gone.

```python
tmp = Database(":memory:")
tmp.close()
tmp.closed  # -> True
```

---

## Threads

When `Database` opens a connection it asks SQLite how it was compiled. If SQLite is built in its
default "serialized" threading mode (the usual case on current Python builds), LuminaDB turns off
`sqlite3`'s same-thread check, and one `Database` can be used from several threads:

```python
import threading

shared = Database(":memory:")
shared_table = shared.create_table("t", [integer("id")])
result = {}


def worker():
    shared_table.insert({"id": 1})
    result["rows"] = shared_table.select(what="id")


thread = threading.Thread(target=worker)
thread.start()
thread.join()
result  # -> {'rows': [1]}
```

On a SQLite build that does not offer serialized mode, the same-thread check stays on, and using the
connection from another thread raises `sqlite3.ProgrammingError`. In that situation, open one
`Database` per thread.

Even with serialized SQLite, a single connection shares one transaction state. If several threads write
at once and some of them use `with table:` blocks, their transactions can interfere. For heavy
multi-threaded writing, give each thread its own `Database` on the same file and keep transactions short.

---

## Transactions

By default, every `insert`, `update` and `delete` is committed straight away.

### Grouping writes with `with`

Use a table as a context manager to run several writes as one unit. The block commits when it ends
normally and rolls back if an exception leaves it:

```python
try:
    with users:
        users.insert({"id": 1, "name": "first"})
        users.insert({"id": 2, "name": "second"})
        raise RuntimeError("something failed")
except RuntimeError:
    pass
users.select(what="name")  # -> []

with users:
    users.insert({"id": 1, "name": "first"})
    users.insert({"id": 2, "name": "second"})
users.select(what="name")  # -> ['first', 'second']
```

The transaction belongs to the connection, so it covers every table of the same `Database`, not only the
one you wrote `with` on. In a model, use [`Model.atomic()`](ModelAPI.md#transactions).

### Nesting

A block inside another block becomes a SQLite savepoint. One level of nesting works. If the inner block
fails and you catch the exception, only the inner block's work is undone:

```python
with users:
    users.insert({"id": 3, "name": "outer"})
    try:
        with users:
            users.insert({"id": 4, "name": "inner"})
            raise ValueError("inner failed")
    except ValueError:
        pass
users.select({"id": op.in_([3, 4])}, what="name")  # -> ['outer']
```

```py
# Three levels deep does NOT work in 0.8.1:
with users:
    with users:
        with users:        # OperationalError: no such savepoint
            ...
```

In 0.8.1, **nesting three or more levels deep raises `sqlite3.OperationalError: no such savepoint`**,
and afterwards the internal depth counter is out of step, so later transactions in the same process fail
too. Keep to one outer block and at most one inner block. See [Known Issues](Troubleshooting.md).

### Manual control

`table.commit()` and `table.rollback()` (and the same on `db`) are there if you prefer to manage things
yourself. Setting `table.auto_commit = False` stops the table from committing after every write.

Be careful with this mode. In 0.8.1, calling `select()` on the table commits any pending writes, so a
`rollback()` after a read has nothing left to undo, and `force_dirty = True` does not change that. If
you need to undo writes, the `with table:` form is the reliable way.

---

## Pragmas and maintenance

| Method | What it does |
| --- | --- |
| `db.foreign_pragma("ON")` | Turn on foreign-key enforcement. SQLite starts with it off, for every new connection. |
| `db.foreign_pragma()` | Read the current setting, for example `{'foreign_keys': 1}`. |
| `db.optimize()` | Run `PRAGMA optimize`. |
| `db.shrink_memory()` | Run `PRAGMA shrink_memory`. |
| `db.vacuum()` | Rebuild the file to reclaim free space. |

```python
db.foreign_pragma()  # -> {'foreign_keys': 0}
db.foreign_pragma("ON")
db.foreign_pragma()  # -> {'foreign_keys': 1}
```

`foreign_pragma` accepts only `"ON"`, `"OFF"` or no argument.

---

## Talking to SQLite directly

`db.sql` is the underlying `sqlite3.Connection`, and its rows come back as `Row` dicts. Use it for
anything LuminaDB does not cover, such as `OR` conditions, joins or `PRAGMA table_info`:

```python
db.sql.execute("select count(*) as total from users").fetchone()  # -> {'total': 3}
```

Remember to pass values as parameters (`?` or `:name`) rather than building SQL strings.

---

## Debugging SQL

To see every statement LuminaDB builds along with its parameters, switch on the debug flag. This lives
in a private module, so treat it as a development aid that may change:

```python
from luminadb import _debug

_debug.STATE["DEBUG"] = True
users.select({"id": 1})
_debug.STATE["DEBUG"] = False
```

The output looks like `select * from users where id=:id_check__...`, followed by the parameter dict.
