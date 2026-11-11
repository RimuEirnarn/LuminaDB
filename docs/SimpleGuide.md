# Table API Guide

The **Table API** is LuminaDB's dictionary-driven interface to SQLite. You describe tables with
small column builders, and you read and write rows with plain Python dicts. No SQL strings needed
(though `db.sql` gives you the raw `sqlite3` connection whenever you want it).

If you prefer declaring tables as Python classes, see the [Model API](ModelAPI.md). Both APIs share
the same engine, and a model can hand you its `Table` with `Model.get_table()`.

> **About the examples.** The examples in this guide were run against LuminaDB 0.8.1. A value after
> `# ->` is the real result, copied from that run. Blocks build on each other, so read them in order.

## Contents

- [Connecting](#connecting)
- [Defining columns](#defining-columns)
- [Creating and managing tables](#creating-and-managing-tables)
- [Inserting](#inserting)
- [Selecting](#selecting)
- [Conditions (the `where` argument)](#conditions-the-where-argument)
- [Sorting, limiting and pagination](#sorting-limiting-and-pagination)
- [SQL functions](#sql-functions)
- [Updating and deleting](#updating-and-deleting)
- [Foreign keys and subqueries](#foreign-keys-and-subqueries)
- [Indexes](#indexes)
- [Transactions](#transactions)
- [CSV import and export](#csv-import-and-export)
- [Safety rules](#safety-rules)
- [Exceptions](#exceptions)

---

## Connecting

```python
from luminadb import Database, integer, text, real, blob, op, SKIP

db = Database(":memory:")          # temporary, gone when the program ends
db.path  # -> ':memory:'
db.closed  # -> False
```

Pass a filename instead to keep your data: `Database("app.db")`.

`Database(path, **kwargs)` forwards every keyword it doesn't recognise (for example `timeout=5`) to
`sqlite3.connect`. The two it does recognise are `strict` and `forgive`, both described in
[Database Options](DatabaseModes.md). Python 3.11 or newer is required.

`db.sql` is the underlying `sqlite3.Connection`. Rows it returns are `Row` objects (see below).
`db.close()` closes the connection, and it is also registered to run automatically at interpreter exit.

---

## Defining columns

Columns are built with `text()`, `integer()`, `real()` and `blob()`, then refined by chaining:

| Method | Effect |
| --- | --- |
| `.primary()` | Primary key (also implies unique). Use it on two columns for a composite key. |
| `.auto_increment()` | `AUTOINCREMENT`. Integer primary keys only. |
| `.unique()` | `UNIQUE` constraint. |
| `.default(value)` | Default value (see the warning below). |
| `.allow_null()` | Allow `NULL`. |
| `.foreign("table/column")` | Foreign key reference. |
| `.on_delete(action)` / `.on_update(action)` | `"cascade"` (default), `"null"`, `"restrict"`, `"no act"` or `"default"`. |

```python
users = db.create_table("users", [
    integer("id").primary().auto_increment(),
    text("name"),
    text("email").unique().allow_null(),
    integer("age").default(18),
])
users.name  # -> 'users'
```

**Columns are `NOT NULL` unless you say otherwise.** Either call `.allow_null()` or give the column a
default. Inserting a row without a value for a required column raises `sqlite3.IntegrityError`.

> **Watch out: falsy defaults.** `.default(0)`, `.default("")` and `.default(0.0)` do *not* produce a
> `DEFAULT` clause. The column becomes nullable and gets `NULL` instead. Truthy defaults
> (`.default(18)`, `.default(True)`) work as expected. If you need a default of zero, set it in your own
> insert code for now.

There is also a `boolean()` builder in `luminadb.column`. SQLite stores booleans as `0`/`1`, so you
read back integers.

Prefer plain objects? `Column` takes the same options as keyword arguments:

```python
from luminadb import Column

pair = db.create_table("pair", [
    Column("a", "integer", primary=True),
    Column("b", "integer", primary=True),   # two primaries -> composite key
    Column("note", "text"),
])
pair.insert({"a": 1, "b": 2, "note": "first"})  # -> 1
```

---

## Creating and managing tables

```python
db.check_table("users")  # -> True
db.check_table("ghost")  # -> False
db.table("users").name  # -> 'users'
```

- `db.create_table(name, columns)` creates the table and returns a `Table`.
- `db.table(name)` fetches an existing table. With the default `strict=True` it raises
  `DatabaseMissingError` for a table that does not exist.
- `db.tables()` returns every user table as a tuple of `Table` objects. Tables using `AUTOINCREMENT`
  make SQLite add an internal `sqlite_sequence` table, which shows up here too.
- `db.rename_table(old, new)` renames a table and returns it.
- `db.reset_table(name, columns)` is meant to drop and recreate a table (see [Known Issues](Troubleshooting.md)).
- `db.delete_table(name)` drops a table (see [Known Issues](Troubleshooting.md)).

By default, calling `create_table` for a name that already exists just returns the existing table
(`forgive=True`). Pass `forgive=False` to get a `DatabaseExistsError` instead.

### Changing an existing table

```python
users.add_column(text("nickname").allow_null())
users.rename_column("nickname", "handle")
```

`add_column` follows SQLite's rules and refuses columns that are primary or unique, columns that are
`NOT NULL` without a default, and foreign-key columns that have a non-null default.

`table.columns()` returns the `Column` objects, but only for a `Table` that came from
`create_table` in the same session. A table fetched with `db.table()` raises `AttributeError`.

---

## Inserting

```python
users.insert({"name": "Alice", "email": "alice@example.com"})  # -> 1
users.insert({"name": "Bob", "age": 30})  # -> 2
users.insert_many([{"name": "Cy", "age": 22}, {"name": "Di", "age": 41}])
```

- `insert(row)` returns the new row's `rowid`.
- `insert_many(rows)` (alias `insert_multiple`) inserts a list in one statement and returns `None`.
  Every dict must have the same keys as the first one.

Values are always sent as bound parameters, so quotes and semicolons in your data are safe.

---

## Selecting

```python
users.select(what="name")  # -> ['Alice', 'Bob', 'Cy', 'Di']
users.select({"name": "Alice"})  # -> [{'id': 1, 'name': 'Alice', 'email': 'alice@example.com', 'age': 18, 'handle': None}]
users.count()  # -> 4
```

`select(where=None, what="*", limit=0, offset=0, order=None, flatten=False)`

What you get back depends on `what`:

| `what` | Returns |
| --- | --- |
| `"*"` (default) | a list of `Row` dicts |
| `"name"` or `("name",)` | a flat list of that column's values |
| `("id", "name")` | a list of `Row` dicts with only those columns |
| `count("*")`, `Function("MAX")("age")` | a single value |

```python
users.select(what=("id", "name"), limit=2)  # -> [{'id': 1, 'name': 'Alice'}, {'id': 2, 'name': 'Bob'}]
users.select(what=("id", "name"), flatten=True)  # -> {'id': [1, 2, 3, 4], 'name': ['Alice', 'Bob', 'Cy', 'Di']}
```

`flatten=True` returns a single `Row` whose values are lists, one per column.

`select_one()` returns the first match. For a miss it returns an **empty `Row`** (which is falsy),
not `None`:

```python
users.select_one({"name": "Bob"}, what="age")  # -> 30
users.select_one({"name": "nobody"})  # -> {}
bool(users.select_one({"name": "nobody"}))  # -> False
```

### Rows

A `Row` is a `dict` that also allows attribute access:

```python
row = users.select_one({"name": "Alice"})
row.name  # -> 'Alice'
row["name"]  # -> 'Alice'
```

---

## Conditions (the `where` argument)

`where` is a dict of `column: condition`. A bare value means "equals", and several entries are joined
with `AND`. There is no `OR`; for that, run two queries or use `db.sql` directly.

```python
users.select({"name": "Bob"}, what="id")  # -> [2]
users.select({"age": op > 20, "name": op.like("%y")}, what="name")  # -> ['Cy']
```

`op` (also exported as `this`) builds conditions with ordinary Python operators:

```python
users.select({"age": op > 25}, what="name")  # -> ['Bob', 'Di']
users.select({"age": op != 30}, what="name")  # -> ['Alice', 'Cy', 'Di']
users.select({"age": op.between(20, 35)}, what="name")  # -> ['Bob', 'Cy']
users.select({"name": op.like("A%")}, what="name")  # -> ['Alice']
users.select({"id": op.in_([1, 3])}, what="name")  # -> ['Alice', 'Cy']
users.select({"id": op.in_([1, 3]).negate()}, what="name")  # -> ['Bob', 'Di']
```

Supported: `==`, `!=`, `<`, `<=`, `>`, `>=`, `.like()`, `.in_()`, `.between()`, and
`.negate()` on `in_` and `between`. For "not equal" use `op != value`.

Rules worth knowing:

- `.between(low, high)` only accepts numbers.
- `.like()` accepts letters, digits, spaces, `%` and `_`. Other punctuation raises `SecurityError`.
- `.negate()` is only reliable after `.in_()` or `.between()`. Call it *after* them, as in
  `op.in_([1]).negate()`, because `op.negate().in_(...)` fails.

### The `luminadb.operators` helpers

`eq`, `ne`, `lt`, `le`, `gt`, `ge`, `like` and `between` take the column name as their first argument
and return a `(name, condition)` tuple. Pass them in a **list**, not a dict:

```python
from luminadb.operators import eq, gt, like

users.select([eq("name", "Bob"), gt("age", 20)], what="id")  # -> [2]
users.select([like("name", "C%")], what="name")  # -> ['Cy']
```

In list form the column name is used as a dict key internally, so you cannot put two conditions on the
same column. Use `between` for ranges.

---

## Sorting, limiting and pagination

`order` is a `(column, "asc" | "desc")` tuple.

```python
users.select(order=("age", "desc"), what="name")  # -> ['Di', 'Bob', 'Cy', 'Alice']
users.select(order=("age", "asc"), limit=2, offset=1, what="name")  # -> ['Cy', 'Bob']
```

`offset` is only emitted when it is non-zero, and SQLite requires a `LIMIT` before an `OFFSET`, so
pair them as above.

> **Sorting by several columns** is declared in the code (a tuple of `(column, direction)` tuples) but
> raises `OperationalError: incomplete input` in 0.8.1. See [Known Issues](Troubleshooting.md).

`paginate_select()` is a generator that yields one page at a time:

```python
for page in users.paginate_select(length=3, what="name"):
    print(page)
# ['Alice', 'Bob', 'Cy']
# ['Di']
```

Start from a later page with `page=`. `length` defaults to 10. A negative `page` is treated as page 0
with the sort direction flipped, which gives you the last rows first.

---

## SQL functions

```python
from luminadb.functions import count, Function

users.select(what=count("*"))  # -> 4
users.select({"age": op > 25}, what=count("*"))  # -> 2
users.select(what=Function("MAX")("age"))  # -> 41
users.select(what=Function("SUM")("age"))  # -> 111
```

`Function("NAME")` builds a callable for any SQL aggregate or scalar function. Names are checked for
safety, and values go in as parameters. `table.count()` is shorthand for `select(what=count("*"))`
over the whole table.

---

## Updating and deleting

```python
users.update({"name": "Alice"}, {"age": 19})  # -> 1
users.update_one({"age": op > 20}, {"email": "bob@example.com"})  # -> 1
```

`update(where, data, limit=0, order=None)` takes the **condition first, then the new values**, and
returns the number of rows changed. `update_one` is `update` with `limit=1`.

Leaving `where` as `None` updates every row, so double check before you do that.

### `SKIP`

Use the `SKIP` sentinel for values you might not have. The query builder drops those keys:

```python
users.update({"name": "Bob"}, {"age": SKIP, "email": "bob2@example.com"})  # -> 1
users.select_one({"name": "Bob"}, what="age")  # -> 30
```

If *everything* in the dict is `SKIP`, you get a `ValueError`, since there would be nothing to write.

### Deleting

```python
users.delete({"name": "Di"})  # -> 1
users.delete_one({"name": "Cy"})  # -> 1
users.select(what="name")  # -> ['Alice', 'Bob']
```

`delete(where=None, limit=0, order=None)` returns the number of rows removed. A missing `where` deletes
everything.

---

## Foreign keys and subqueries

SQLite only enforces foreign keys when the pragma is on, so switch it on right after connecting:

```python
db.foreign_pragma("ON")
db.foreign_pragma()  # -> {'foreign_keys': 1}

groups = db.create_table("groups", [integer("id").primary(), text("name")])
members = db.create_table("members", [
    integer("id").primary(),
    text("name"),
    integer("gid").foreign("groups/id").on_delete("cascade"),
])
groups.insert({"id": 1, "name": "admins"})  # -> 1
groups.insert({"id": 2, "name": "guests"})  # -> 2
members.insert({"id": 1, "name": "Alice", "gid": 1})  # -> 1
members.insert({"id": 2, "name": "Eve", "gid": 2})  # -> 2
```

A **subquery** lets one table's `where` use another table's result. Build it with
`other_table.subquery(where, column)` and pass it as the condition value; LuminaDB turns it into
`column IN (SELECT ...)`:

```python
admin_ids = groups.subquery({"name": "admins"}, "id")
members.select({"gid": admin_ids}, what="name")  # -> ['Alice']
```

Deleting the parent row cascades:

```python
groups.delete({"id": 1})  # -> 1
members.select(what="name")  # -> ['Eve']
```

Subqueries nest up to 10 levels deep, after which `RecursionError` is raised.

---

## Indexes

```python
from luminadb import Index

idx = Index().name("idx_users_name").target("users").columns("name").unique()
idx.build_sql()  # -> 'create unique index idx_users_name on users (name)'
db.create_index(idx)
db.delete_index("idx_users_name")
```

`delete_index(name, exists_ok=True)` skips the error when the index is missing.

---

## Transactions

By default every write commits immediately. To group writes, use the table as a context manager. It
commits on success and rolls back if an exception escapes:

```python
try:
    with users:
        users.insert({"name": "T1"})
        raise RuntimeError("something went wrong")
except RuntimeError:
    pass
users.select({"name": "T1"})  # -> []

with users:
    users.insert({"name": "T2"})
users.select(what="name")  # -> ['Alice', 'Bob', 'T2']
```

For nesting limits, the `auto_commit` property and how reads interact with pending writes, read
[Database Options and Transactions](DatabaseModes.md#transactions).

---

## CSV import and export

```python
from luminadb.csv import to_csv_string, to_csv_file

colors = db.create_table("colors", [integer("id").primary(), text("name")])
colors.insert_many([{"id": 1, "name": "red"}, {"id": 2, "name": "dark, blue"}])
to_csv_string(colors)  # -> 'id,name\r\n1,red\r\n2,"dark, blue"\r\n'
to_csv_file(colors, "colors.csv")  # -> True
```

- `to_csv_string(table)` returns CSV text (an empty table gives `""`). Passing a `Database` returns a
  list of `(table_name, csv_text)` pairs.
- `to_csv_file(table, path)` writes one file. `to_csv_file(db, folder)` creates the folder and writes one
  `<table>.csv` per table.

`from_csv_string` and `from_csv_file` exist, but in 0.8.1 they do not work on text input (see
[Known Issues](Troubleshooting.md)). Until that is fixed, import with the standard library:

```python
from csv import DictReader

copy = db.create_table("colors_copy", [integer("id").primary(), text("name")])
with open("colors.csv", newline="", encoding="utf-8") as handle:
    copy.insert_many(list(DictReader(handle)))
copy.select(what="id")  # -> [1, 2]
copy.select(what="name")  # -> ['red', 'dark, blue']
```

---

## Safety rules

LuminaDB sends values as bound parameters, and it validates everything that becomes part of the SQL
text (table names, column names, function names, `like` patterns). That has a few consequences:

- Identifiers may not contain punctuation other than `_`. Stick to letters, digits and underscores.
- Reserved SQL words such as `order`, `group`, `key` or `values` cannot be used as table or column
  names.
- A rejected name raises `SecurityError` before anything touches the database.

```python
from luminadb.errors import SecurityError

try:
    users.select({"name; drop table users": 1})
except SecurityError as exc:
    print(type(exc).__name__)   # SecurityError
```

---

## Exceptions

| Exception | When |
| --- | --- |
| `SecurityError` | An identifier, function name or `like` pattern failed validation. |
| `DatabaseMissingError` | `db.table(name)` for a table that does not exist (strict mode). |
| `DatabaseExistsError` | `create_table` for an existing name with `forgive=False`. |
| `TableRemovedError` | A `Table` object is used after its table was dropped. |
| `sqlite3.IntegrityError` | A constraint failed (unique, not null, foreign key). The message carries extra notes with the SQL and arguments. |
| `sqlite3.OperationalError` | SQLite rejected the statement, for example an unknown column. |
| `sqlite3.ProgrammingError` | You used a `Database` after calling `close()`. |

All of LuminaDB's own exceptions live in `luminadb.errors`.
