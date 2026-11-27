# TODO

**NOTE**: This TODO need to be written in markdown. use `[ ]` or `[x]` to mark todos

**High velocity** is required but make sure to do it with low overhead.

## API Overview

**Table API** is dictionary-driven API for low-level operators such as Sub Query, functions, generic CRUD, and any other else.

**Model API** is a Model-driven API for higher-level operators with simplified usage, mimicking Laravel Eloquent ORM. Accessing Table API from Model API is as simple as calling `Model.get_table()`

## SQLite API

What API should we bring? This todo is for simplification; you can do anything with `Database.sql` property

- [x] Table API function support
- [x] Pragmas (Database, only select few)
- [x] Table API subquery
- [x] Table API `as` keyword (`as_`)
- [x] Table API join (nested rows, `what` required), Model API join + `group()`
- [x] sqlite select data squash[^1] (squash=True, Table API)
- [x] sqlite select `what` data. Instead of using `select *`, we should also have `what`. Bring up few data than select everything.[^2]
- [x] Provide caching functions; not to cache sql returns but sql query, etc.[^3]
- [ ] Model API automatic migration helpers (Design schematic is ass, hard to implement)[^4]

## Other functionality

The functionality here is outside of sqlite features such as export and import.

- [x] CSV Export
- [x] CSV Import

---

[^1]: The term data crunching is when `select*` is used, typically the functions returns list of `Row`, by using `crunch=True`, the return value should be `Row[str, tuple[Any]]`

[^2]: If `what` parameter is specified to 1, we should return `tuple[Any, ...]` instead of `list[Row[str, Any]]` or `Row[str, Any]`

[^3]: Caching should **NOT** cache query data! Provide using `combine_keyvals` to create named params

[^4]: I need a way to implement migrations to advance to version `v0.8`, I need to know how migration works, how a Python code can translate into SQL, though I can just ask Table API query builder to parse columns. However, Table API's query builder sucks when we parse SQL query into Python objects, it can't translate inline column definitions for now. Can it? Maybe, if it can, some metadata is guaranteed lost if constraints are inlined. Table API has `rename_column` and `add_column` for convenience. Modify? Remove? not now.

Migration schematics:

The Schema table is described using Table API Builder column:

```python
db.create_table("_schema_meta", [
    text("table_name").primary(),
    integer("version").default(1),
    text("schema_json"),
    real("created_at"),
    real("updated_at")
])
```

The columns must be then normalized back to `Column` and manually compare which changes and which didn't.

Any fields that was significantly changed will force new table creation, subsequently followed up and refilled. If new entries are unique, the ORM is simply fucked unless provided with custom unique field generator (i.e, uuid4())
