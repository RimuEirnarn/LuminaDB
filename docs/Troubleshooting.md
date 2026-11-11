# Known Issues and Troubleshooting

This page lists problems found in **LuminaDB 0.8.1**, each with a workaround, followed by the error
messages you are most likely to meet and what they mean. When an issue is fixed in a later release,
delete its entry here.

> Every entry below was reproduced against 0.8.1. The workaround blocks are runnable.

## Contents

- [Known issues](#known-issues)
- [Common errors](#common-errors)

---

## Known issues

### 1. `delete_table()` raises after dropping the table, and `reset_table()` fails

**Symptom.** `db.delete_table("name")` drops the table and then raises `TableRemovedError`.
`db.reset_table(name, columns)` calls it first, so it raises too and never recreates the table, which
leaves you with the table gone.

**Cause.** After the drop, `Table._delete_hook` checks the table with a `select()` and catches
`OperationalError`, but `select()` has already translated that error into `TableRemovedError`.

**Workaround.** Catch the error yourself:

```python
from luminadb import Database, integer, text
from luminadb.errors import TableRemovedError

db = Database(":memory:")
db.create_table("scratch", [integer("id")]).insert({"id": 1})

try:
    db.delete_table("scratch")
except TableRemovedError:
    pass                      # the table has been dropped at this point
db.check_table("scratch")  # -> False

# "reset" a table = drop it, then create it again
db.create_table("scratch", [integer("id"), text("label")]).select()  # -> []
```

### 2. Sorting by several columns fails

**Symptom.** `table.select(order=(("age", "desc"), ("name", "asc")))` raises
`OperationalError: incomplete input`.

**Cause.** `parse_orders` builds the clause from a generator filtered through `check_iter(...)`, which
returns `None`, so every sort key is filtered out and the query ends in `order by `.

**Workaround.** Sort by one column in SQL and the rest in Python, or use `db.sql`:

```python
people = db.create_table("people", [text("name"), integer("age")])
people.insert_many([{"name": "Bo", "age": 30}, {"name": "Al", "age": 30}, {"name": "Cy", "age": 20}])
rows = db.sql.execute("select name from people order by age desc, name asc").fetchall()
[row["name"] for row in rows]  # -> ['Al', 'Bo', 'Cy']
```

### 3. `from_csv_string()` and `from_csv_file()` cannot import

**Symptom.** Importing fails with `OperationalError: table ... has no column named i`.

**Cause.** `from_csv_string` hands the CSV *text* to `csv.DictReader`, which expects lines and so
iterates the string one character at a time.

**Workaround.** Parse the file yourself, as shown in the [Table API guide](SimpleGuide.md#csv-import-and-export):
`table.insert_many(list(DictReader(file_handle)))`. Export (`to_csv_string`, `to_csv_file`) works.

### 4. Nested transactions deeper than two levels fail

**Symptom.** Three nested `with table:` blocks raise `OperationalError: no such savepoint`. Afterwards,
later transactions in the same process also fail.

**Workaround.** Keep to one outer block and at most one inner block. Details in
[Transactions](DatabaseModes.md#transactions).

### 5. `negate()` only works after `in_()` or `between()`

**Symptom.** `op.like("A%").negate()` raises `OperationalError: row value misused`. `(op == 1).negate()`
and `op.negate().in_(...)` raise `TypeError`.

**Workaround.** Use `op != value` for "not equal", and call `.negate()` last on `in_` and `between`.
For "not like", use `db.sql`.

### 6. Falsy defaults become `NULL`

**Symptom.** A column or model field with a default of `0`, `0.0`, `False` or `""` stores `NULL` when
you leave it out, and reads back as `None`. Truthy defaults work.

**Cause.** The table builder only writes a `DEFAULT` clause when the default is truthy, and
`BuilderColumn.default()` marks the column nullable instead.

**Workaround.** Pass the value explicitly:

```python
stats = db.create_table("stats", [integer("id").primary(), integer("hits").default(0)])
stats.insert({"id": 1})  # -> 1
stats.select_one({"id": 1}, what="hits")  # -> None
stats.insert({"id": 2, "hits": 0})  # -> 2
stats.select_one({"id": 2}, what="hits")  # -> 0
```

### 7. Hooks and validators are shared between all models

**Symptom.** A `@hook` or `@validate` defined on one model also runs for other models. A validator for
`Order.id` can reject rows of an unrelated `Customer`, and a hook that reads `item.name` can crash with
`AttributeError` on models without that field.

**Cause.** `BaseModel.__hooks__` and `BaseModel.__validators__` are single dictionaries defined on the
base class. Registering a hook mutates that shared dictionary unless the model has its own.

**Workaround.** Give every model that uses hooks, validators or `type_checking=True` its own
dictionaries:

```python
from luminadb import model, BaseModel, Primary, Unique, Foreign, hook, validate
from luminadb.models.errors import ValidationError

@model(db)
class Checked(BaseModel):
    __schema__ = (Primary("id"),)
    __hooks__ = {}
    __validators__ = {}

    id: str

    @validate("id", "id must be 'ok'")
    def id_ok(item):
        return item.id == "ok"


@model(db)
class Plain(BaseModel):
    __schema__ = (Primary("id"),)
    __hooks__ = {}
    __validators__ = {}

    id: str


Plain.create(id="anything").id  # -> 'anything'
try:
    Checked.create(id="nope")
except ValidationError as exc:
    print(exc)    # id must be 'ok'
```

### 8. Validators and `before_update` see the *old* values on `update()`

**Symptom.** `instance.update(name="")` passes a "name must not be blank" validator, because the
validator inspects the instance before the new value is assigned. The invalid value is saved, and the
next update fails because the stored value is now invalid.

**Workaround.** Validate new values yourself before calling `update()`. Validators run correctly on
`create()`.

### 9. `has_one()` returns `None`

**Symptom.** `parent.has_one(Child)` returns `None` even when a matching child exists.

**Cause.** It searches for the child using the *name* of the parent's primary-key column (for example
`"id"`) instead of its value.

**Workaround.** Query directly:

```python
@model(db)
class Owner(BaseModel):
    __schema__ = (Primary("id"),)
    id: str


@model(db)
class Profile(BaseModel):
    __schema__ = (Primary("id"), Unique("owner_id"), Foreign("owner_id", Owner))
    id: str
    owner_id: str


owner = Owner.create(id="o1")
Profile.create(id="p1", owner_id="o1")
Profile.first(owner_id=owner.id).id  # -> 'p1'
```

### 10. `has_many()` ignores custom table names

**Symptom.** `parent.has_many(Child)` raises `ValueError: ... does not have a Foreign key pointing to
...` when the parent sets `__table_name__`.

**Workaround.** Use `Child.where(foreign_key=parent.id).fetch()`.

### 11. A stray `print` in `select_one()` with SQL functions

**Symptom.** `table.select_one(what=count("*"))` prints the raw row (for example `{'COUNT(*)': 5}`)
before returning the value.

**Workaround.** Use `table.count()` or `table.select(what=count("*"))`, which do not print.

---

## Common errors

| Message | Meaning and fix |
| --- | --- |
| `SecurityError: "key" is a reserved SQL keyword and cannot be used.` | A table, column or model field is named after an SQL keyword (`order`, `group`, `key`, `on`, `when`, ...). Rename it. |
| `SecurityError: Cannot parse unsafe data.` | An identifier has punctuation other than `_`. Use letters, digits and underscores. |
| `SecurityError: Cannot understand other character.` | A `like()` pattern has characters other than letters, digits, spaces, `%` and `_`. |
| `sqlite3.IntegrityError: NOT NULL constraint failed: users.name` | A column without a default or `.allow_null()` got no value. |
| `sqlite3.IntegrityError: UNIQUE constraint failed: users.email` | Duplicate value in a `unique()` or primary-key column. |
| `sqlite3.IntegrityError: FOREIGN KEY constraint failed` | The referenced row does not exist, or you are deleting a parent that `RESTRICT` protects. Make sure `db.foreign_pragma("ON")` is what you intend. |
| `DatabaseMissingError: table x does not exists.` | `db.table("x")` before the table was created. Call `create_table` first. |
| `TableRemovedError` | The table was dropped while you still held its `Table` object. |
| `sqlite3.OperationalError: no such column` | A `where` or `what` names a column that is not in the table. |
| `TypeError: ... got an unexpected keyword argument 'x'` on a model | The model has fewer fields than the table, or `create()` got an unknown field. LuminaDB has no migrations, so add the field to the class. |
| `TypeError: non-default argument 'x' follows default argument` | Model fields with defaults must come after those without. |
| `sqlite3.ProgrammingError: Error binding parameter` | A value has a Python type SQLite cannot store (`list`, `dict`, ...). Convert it to text or bytes. |
| `sqlite3.ProgrammingError: Cannot operate on a closed database.` | The `Database` was closed. Open a new one. |
