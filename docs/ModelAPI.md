# Model API Guide

The **Model API** lets you describe a table as a Python class and work with rows as objects. If you
have used Laravel's Eloquent, the method names will feel familiar (`create`, `first`, `where`, `has_many`).

It sits on top of the [Table API](SimpleGuide.md). Every model owns a `Table`, available through
`Model.get_table()`, so you can drop down a level whenever the model layer is not enough.

> **About the examples.** The examples in this guide were run against LuminaDB 0.8.1. A value after
> `# ->` is the real result, copied from that run. Blocks build on each other, so read them in order.

## Contents

- [Your first model](#your-first-model)
- [Fields and types](#fields-and-types)
- [Schema constraints](#schema-constraints)
- [Creating rows](#creating-rows)
- [Reading rows](#reading-rows)
- [The query builder](#the-query-builder)
- [Updating and deleting](#updating-and-deleting)
- [Auto-increment keys](#auto-increment-keys)
- [Relationships](#relationships)
- [Hooks, validators and type checking](#hooks-validators-and-type-checking)
- [Hiding fields](#hiding-fields)
- [Transactions](#transactions)
- [Bulk operations and chunking](#bulk-operations-and-chunking)
- [Scopes](#scopes)
- [Escape hatches](#escape-hatches)
- [Things to know](#things-to-know)

---

## Your first model

```python
from uuid import uuid4
from luminadb import Database, model, BaseModel, Primary, Unique, Foreign, hook, validate, op
from luminadb.models import CASCADE, SETNULL, RESTRICT
from luminadb.models.errors import ValidationError, NoDataReturnedError
from luminadb.models.mixin import ScopeMixin, ChunkableMixin

db = Database(":memory:")      # or Database("app.db")
db.foreign_pragma("ON")        # make SQLite enforce foreign keys


@model(db)
class User(BaseModel, ScopeMixin, ChunkableMixin):
    __schema__ = (Primary("id"), Unique("username"))
    __auto_id__ = lambda: str(uuid4())
    __hidden__ = ("password",)

    id: str
    username: str
    password: str
    is_active: bool = True
    age: int = 18
```

`@model(db)` does three things: it turns the class into a dataclass, creates the table if it does not
exist yet (or attaches to it if it does), and wires up hooks and validators. The class attributes with
double underscores are the model's settings:

| Attribute | Purpose |
| --- | --- |
| `__schema__` | A tuple of constraints: `Primary`, `Unique`, `Foreign`. |
| `__auto_id__` | Callable that produces a primary key when `create()` is given none. |
| `__table_name__` | Table name. Defaults to the class name in lowercase (`User` becomes `user`). |
| `__hidden__` | Field names left out of `to_dict()` and blanked by `to_safe_instance()`. |
| `__hooks__`, `__validators__` | Filled in for you by `@hook` and `@validate`. See [Hooks](#hooks-validators-and-type-checking). |

---

## Fields and types

Fields are ordinary annotated dataclass attributes. The annotation picks the SQLite column type:

| Annotation | Column type |
| --- | --- |
| `str` | `TEXT` |
| `int` | `INTEGER` |
| `float` | `REAL` |
| `bytes` | `BLOB` |
| `bool` | `BOOLEAN` (stored as `0`/`1`) |
| anything else | `TEXT` |

A few consequences of this design:

- **Dataclass rules apply.** Fields without a default must come before fields that have one, or Python
  raises `TypeError` at class definition.
- **Reserved SQL words cannot be field names.** A field called `order`, `key`, `on` or `when` raises
  `SecurityError` when the class is defined.
- **Fields are `NOT NULL` unless they have a default.** A field with a default is stored with that default.
- **Booleans come back as integers** when you read a row from the database (`1` and `0`, not `True`
  and `False`). The instance returned by `create()` keeps the Python value you gave it.
- **Other Python types are not converted.** A `list` or `dict` cannot be bound by `sqlite3`. Store
  JSON text yourself, and prefer ISO-8601 strings over `datetime` objects.

> **Watch out: falsy defaults.** A default of `0`, `0.0`, `False` or `""` is *not* written to the
> table. If you create a row without that field, the instance you get back shows the default, but the
> stored value is `NULL`, and reading the row later gives `None`. Truthy defaults such as `18` or `True`
> work normally. For now, pass falsy values explicitly: `Item.create(count=0)`.

---

## Schema constraints

`__schema__` takes constraint objects that name a field:

```py
__schema__ = (
    Primary("id"),                                   # primary key
    Unique("username"),                              # unique column
    Foreign("user_id", User),                        # foreign key to User's primary key
    Foreign("team_id", "teams/id"),                  # or give "table/column" explicitly
)
```

- **`Primary(field, auto=False)`** — one primary key per model (a second one raises `ConstraintError`).
  Use `auto=True` for an auto-incrementing integer key.
- **`Unique(field)`** — adds a `UNIQUE` constraint.
- **`Foreign(field, target)`** — `target` is a model class (its primary key is used) or a
  `"table/column"` string. Refine it with `.on_delete(...)` and `.on_update(...)` using `CASCADE`,
  `SETNULL`, `RESTRICT`, `NOACT` or `DEFAULT`. These constants live in `luminadb.models`, not in the
  top-level `luminadb` package.

A constraint naming a field the class does not have raises `KeyError`.

---

## Creating rows

`create()` takes keyword arguments, inserts the row, and **returns the model instance**:

```python
ann = User.create(username="ann", password="pw1", age=30)
bob = User.create(username="bob", password="pw2", age=20, is_active=False)
cy = User.create(username="cy", password="pw3", age=41)

type(ann).__name__  # -> 'User'
ann.username  # -> 'ann'
len(ann.id)  # -> 36
```

Because `__auto_id__` is set, the primary key was generated for us. If you pass an `id` yourself, it
is used as given. Unknown keyword arguments raise `TypeError`, and a unique-constraint clash raises
`sqlite3.IntegrityError`.

---

## Reading rows

```python
User.count()  # -> 3
User.count(is_active=True)  # -> 2
User.exists(username="ann")  # -> True
User.first(username="zed")  # -> None
User.first(username="ann").age  # -> 30
[u.username for u in User.all()]  # -> ['ann', 'bob', 'cy']
```

| Method | Returns |
| --- | --- |
| `all()` | List of every row. |
| `first(**filters)` | First match, or `None`. |
| `first_or_fail(**filters)` | First match, or raises `NoDataReturnedError`. |
| `one(**filters)` | The only match, `None` if there are none, `ValueError` if there are several. |
| `find(n)` / `find_or_fail(n)` | The first `n` rows (the `_or_fail` form raises when the table is empty). |
| `count(**filters)` | Number of matching rows. |
| `exists(**filters)` | `True` or `False`. |

```python
try:
    User.first_or_fail(username="zed")
except NoDataReturnedError as exc:
    print(exc)   # Model User has no data for current scope

[u.username for u in User.find(2)]  # -> ['ann', 'bob']
```

---

## The query builder

`where()` and `query()` return a chainable `QueryBuilder`. Nothing runs until you finish the chain with
`fetch()`, `fetch_one()`, `count()`, `patch()` or `delete()`.

```python
[u.username for u in User.where(age=op > 25).fetch()]  # -> ['ann', 'cy']
[u.username for u in User.where(age=op.between(18, 35)).fetch()]  # -> ['ann', 'bob']
[u.username for u in User.query().order_by("age", descending=True).limit(2).fetch()]  # -> ['cy', 'ann']
User.where(is_active=True).count()  # -> 2
```

Filters are the same as in the Table API: a plain value means "equals", and `op` gives you comparison
operators, `like`, `between` and `in_`. Several keyword arguments are combined with `AND`.

| Method | Effect |
| --- | --- |
| `.where(**filters)` | Add filters (can be chained). |
| `.limit(n)` / `.offset(n)` | Paging. As in SQLite, `offset` needs a `limit` to go with it. |
| `.order_by(column, descending=False)` | Sort by one column. |
| `.throw()` | Make an empty result raise `NoDataReturnedError`. |
| `.fetch()` | List of instances. |
| `.fetch_one()` | One instance or `None`. |
| `.count()` | Number of matching rows. |
| `.patch(**values)` / `.update(**values)` | Update every matching row and return how many changed. |
| `.delete()` | Delete every matching row and return how many were removed. |

---

## Updating and deleting

On the query builder, which works on many rows at once and skips hooks and validators:

```python
User.where(username="bob").patch(age=21)  # -> 1
User.where(username="bob").update(age=22)  # -> 1
```

On an instance, which returns the instance itself:

```python
cy.update(age=42) is cy  # -> True
User.first(username="cy").age  # -> 42
```

Deleting an instance removes its row:

```python
temp = User.create(username="temp", password="x")
temp.delete()
User.exists(username="temp")  # -> False
```

`delete()` and `update()` find the row through the model's primary key, so a model without a primary
key can only use the query-builder forms.

### Upsert

`upsert(key, **values)` updates the row whose `key` column matches, or creates a new one:

```python
User.upsert("username", username="dee", password="x", age=5).age  # -> 5
User.upsert("username", username="dee", password="x", age=6).age  # -> 6
User.count(username="dee")  # -> 1
```

---

## Auto-increment keys

Use `Primary(field, auto=True)` on an `int` field to let SQLite number the rows. Leave the key out when
you create:

```python
@model(db)
class Ticket(BaseModel):
    __schema__ = (Primary("id", auto=True),)

    id: int
    title: str


Ticket.create(title="first").id  # -> 1
Ticket.create(title="second").id  # -> 2
```

---

## Relationships

Describe the link with a `Foreign` constraint on the child model:

```python
@model(db)
class Post(BaseModel):
    __schema__ = (
        Primary("id", auto=True),
        Foreign("user_id", User).on_delete(CASCADE),
    )

    id: int
    title: str
    user_id: str


Post.create(title="Hello", user_id=ann.id)
Post.create(title="World", user_id=ann.id)

[p.title for p in ann.has_many(Post)]  # -> ['Hello', 'World']
Post.first(title="Hello").belongs_to(User).username  # -> 'ann'
```

- `parent.has_many(Child)` returns the child rows that point at this instance.
- `child.belongs_to(Parent)` returns the parent row.

With `on_delete(CASCADE)`, removing the parent removes its children. This relies on the
`db.foreign_pragma("ON")` call from the start of this guide:

```python
gone = User.create(username="gone", password="x")
Post.create(title="Doomed", user_id=gone.id)
gone.delete()
Post.count(user_id=gone.id)  # -> 0
```

Inserting a child that points at a missing parent fails:

```python
import sqlite3

try:
    Post.create(title="Orphan", user_id="no-such-user")
except sqlite3.IntegrityError as exc:
    print(exc)   # FOREIGN KEY constraint failed
```

The other `on_delete` actions behave as in SQLite: `SETNULL` sets the child's column to `NULL` (give
the field a default of `None` so it can be loaded back), and `RESTRICT` refuses to delete a parent
that still has children.

> `has_many()` finds the link by comparing the child's foreign-key target with the parent's class name in
> lowercase. If you set a custom `__table_name__` on the parent, `has_many()` will not find it. Use
> `Child.where(user_id=parent.id).fetch()` instead. `has_one()` has a separate problem, listed in
> [Known Issues](Troubleshooting.md).

---

## Hooks, validators and type checking

**Hooks** run code around `create`, `update` and `delete`. The available events are `before_create`,
`after_create`, `before_update`, `after_update`, `before_delete` and `after_delete`. Name a function
after the event, or pass the event name to `@hook`:

**Validators** are functions that return `True` when a row is acceptable. Attach one to a field with
`@validate("field", "message")`, or name the function `validate_<field>`.

```python
log = []


@model(db)
class Item(BaseModel):
    __schema__ = (Primary("id"),)
    __auto_id__ = lambda: str(uuid4())
    __hooks__ = {}          # see the note below: always include these two lines
    __validators__ = {}

    id: str
    name: str
    qty: int = 1

    @hook
    def before_create(item):
        log.append(("before_create", item.name))

    @hook("after_update")
    def audit(item):
        log.append(("after_update", item.name))

    @validate("name", "name cannot be blank")
    def name_ok(item):
        return bool(item.name.strip())


widget = Item.create(name="widget")
widget.update(qty=3).qty  # -> 3
log  # -> [('before_create', 'widget'), ('after_update', 'widget')]
```

```python
try:
    Item.create(name="   ")
except ValidationError as exc:
    print(exc)   # name cannot be blank
```

Hook and validator functions are plain functions that receive the instance (no `self`). Each event
holds **one** hook, so a second `@hook` for the same event replaces the first. The same goes for
validators on one field.

> **Always declare `__hooks__ = {}` and `__validators__ = {}` on any model that uses `@hook`,
> `@validate` or `type_checking=True`.** Without them, the hooks and validators are stored in a
> dictionary shared by every model, and a validator written for one model will run when you create a
> row in a completely different one. See [Known Issues](Troubleshooting.md).

> **Validators on `update()` check the old values.** `instance.update(name="   ")` runs your validator
> against the instance *before* the new value is applied, so the blank name is accepted, and the next
> update then fails because of it. Until that is fixed, check new values yourself before calling
> `update()`. Validators on `create()` work as expected.

### Automatic type checking

`@model(db, type_checking=True)` adds a validator for every field that checks `isinstance` against the
annotation:

```python
@model(db, type_checking=True)
class Typed(BaseModel):
    __schema__ = (Primary("id"),)
    __hooks__ = {}
    __validators__ = {}

    id: str
    qty: int


try:
    Typed.create(id="1", qty="many")
except ValidationError as exc:
    print(exc)   # qty type is not int
```

A validator you write for a field replaces the automatic type check on that field.

---

## Hiding fields

Fields listed in `__hidden__` are left out of `to_dict()`, which is handy for API responses.
`to_safe_instance()` returns a copy of the instance with those fields set to `None`.

```python
sorted(ann.to_dict())  # -> ['age', 'id', 'is_active', 'username']
ann.to_safe_instance().password  # -> None
```

`__hidden__` only affects those two methods. The field is still stored and still loaded.

---

## Transactions

`Model.atomic()` wraps a block in a transaction. It commits when the block ends and rolls back if an
exception escapes:

```python
try:
    with User.atomic():
        User.create(username="tx1", password="x")
        raise RuntimeError("abort")
except RuntimeError:
    pass
User.exists(username="tx1")  # -> False

with User.atomic():
    User.create(username="tx2", password="x")
User.exists(username="tx2")  # -> True
```

Keep nesting shallow. See [Transactions](DatabaseModes.md#transactions) for the limits.

---

## Bulk operations and chunking

```python
@model(db)
class Note(BaseModel, ChunkableMixin):
    __schema__ = (Primary("id"),)

    id: str
    title: str


created = Note.bulk_create([{"id": str(i), "title": "draft"} for i in range(7)])
len(created)  # -> 7
Note.count()  # -> 7
```

- `bulk_create(records)` inserts a list of dicts in one statement and returns instances. It goes
  straight to the table, so it does **not** generate ids with `__auto_id__`, and it skips hooks and
  validators. Include every field, including the key.
- `bulk_update(records, key="id")` updates one row per dict, matched on `key`. You must always pass
  `key=`, even when the model has a primary key.
- `bulk_delete(values, key)` deletes every row whose `key` column is in `values`.

```python
Note.bulk_update([{"id": "0", "title": "final"}], key="id")
Note.first(id="0").title  # -> 'final'
Note.bulk_delete(["1", "2"], "id")
Note.count()  # -> 5
```

### Chunking

`ChunkableMixin` adds `chunk()` and `chunk_callback()` for walking a large table in batches, so you
never hold every row in memory:

```python
[len(batch) for batch in Note.chunk(2)]  # -> [2, 2, 1]
```

```python
sizes = []
Note.chunk_callback(3, lambda batch: sizes.append(len(batch)))
sizes  # -> [3, 2]
```

Both accept filters as keyword arguments, for example `Note.chunk(100, title="draft")`.

---

## Scopes

`ScopeMixin` adds `active()`, which returns the rows where `is_active` is true. Your model needs an
`is_active` field for it to work:

```python
[u.username for u in User.active()]  # -> ['ann', 'cy', 'dee', 'tx2']
```

For your own scopes, write a classmethod that returns a `where()` chain:

```py
@classmethod
def adults(cls):
    return cls.where(age=op >= 18).fetch()
```

---

## Escape hatches

```python
User.get_table().name  # -> 'user'
ann.raw("select count(*) as total from user", ()).fetchone()  # -> {'total': 5}
```

- `Model.get_table()` gives you the underlying [`Table`](SimpleGuide.md), with `select(...)`, `insert(...)`
  and everything else from the Table API.
- `instance.raw(sql, params)` runs a SQL statement on the model's connection and returns the cursor.
  It is an instance method, so you need a row in hand (any instance of the model will do). Always pass
  values through `params` instead of formatting them into the string.

---

## Things to know

- **There are no migrations.** The class defines the table the first time it is created. If you change
  the class later, the table does not change. When a model has fewer fields than the table, loading
  rows fails with a `TypeError` that notes the mismatch. Alter the table with `Table.add_column()` or
  `db.sql`.
- **Re-declaring an existing table attaches to it.** Running `@model(db)` for a table that already
  exists does not recreate it, which is what makes file-based databases work across runs.
- **Create the `Database` once** and import it everywhere. Each `Database(...)` opens its own connection.
- **Check [Known Issues](Troubleshooting.md)** before building on `has_one()`, hook/validator sharing,
  or validators during `update()`.
