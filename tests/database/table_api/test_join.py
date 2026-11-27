"""Tests for the Table API join.

Proposed surface
----------------
    table.join(other, on=("left_col", "right_col"), type="inner", alias=None) -> JoinQuery
    table.alias("u")                      -> TableRef usable as base or join target
    as_("col", "alias")                   -> column alias (luminadb.operators), also
                                             works in plain select()
    JoinQuery.join(...)                   -> chain more joins
    JoinQuery.select(*, what, where=None, limit=0, offset=0, order=None)
    JoinQuery.select_one(*, what, where=None, order=None)

`what` is REQUIRED for joins: {table_or_alias: (columns...)}. "*" is rejected
because expanding it would cost an extra metadata query. A join is exactly one
SQL statement.

Result rows are nested Rows keyed by table name (or alias):
    {"users": {"id": 1, ...}, "groups": {"id": 1, ...}}
An unmatched side of a LEFT join is None (the whole slice), not a dict of Nones.
"""

# pylint: disable=redefined-outer-name,invalid-name,missing-function-docstring
from sqlite3 import sqlite_version_info

from pytest import fixture, mark, raises

from luminadb import Database, integer, text, op
from luminadb.errors import SecurityError

from luminadb.operators import as_

USERS = ("id", "username", "gid")
GROUPS = ("id", "name")
ALL = {"users": USERS, "groups": GROUPS}


@fixture
def db():
    database = Database(":memory:")
    groups = database.create_table("groups", [integer("id").primary(), text("name")])
    users = database.create_table(
        "users",
        [
            integer("id").primary(),
            text("username"),
            integer("gid").allow_null().foreign("groups/id"),
        ],
    )
    groups.insert_many([{"id": 1, "name": "root"}, {"id": 2, "name": "user"},
                        {"id": 3, "name": "empty"}])  # group 3 has no users
    users.insert_many(
        [
            {"id": 1, "username": "alice", "gid": 1},
            {"id": 2, "username": "bob", "gid": 2},
            {"id": 3, "username": "carol", "gid": None},  # orphan: no group
        ]
    )
    database.commit()
    return database

def test_inner_join_nested_rows(db):
    users, groups = db.table("users"), db.table("groups")
    rows = users.join(groups, on=("gid", "id")).select(
        what=ALL, order=("users.id", "asc")
    )
    assert rows == [
        {"users": {"id": 1, "username": "alice", "gid": 1},
         "groups": {"id": 1, "name": "root"}},
        {"users": {"id": 2, "username": "bob", "gid": 2},
         "groups": {"id": 2, "name": "user"}},
    ]


def test_same_column_names_do_not_collide(db):
    """Regression for the dict_factory problem: both tables have `id`."""
    users, groups = db.table("users"), db.table("groups")
    row = users.join(groups, on=("gid", "id")).select_one(
        what={"users": ("id",), "groups": ("id",)}, where={"users.id": 2}
    )
    assert row["users"]["id"] == 2
    assert row["groups"]["id"] == 2
    assert row["users"] is not row["groups"]


def test_left_join_unmatched_side_is_none(db):
    users, groups = db.table("users"), db.table("groups")
    rows = users.join(groups, on=("gid", "id"), type="left").select(
        what=ALL, where={"users.id": 3}
    )
    assert rows == [{"users": {"id": 3, "username": "carol", "gid": None},
                     "groups": None}]


def test_attribute_access_on_nested_rows(db):
    """Row already supports attribute access; nested slices should too."""
    users, groups = db.table("users"), db.table("groups")
    row = users.join(groups, on=("gid", "id")).select_one(
        what=ALL, where={"users.id": 1}
    )
    assert row.users.username == "alice"
    assert row.groups.name == "root"


def test_join_is_a_single_statement_without_metadata_queries(db):
    """The reason `what` is required: no pragma_table_info / sqlite_master lookups."""
    users, groups = db.table("users"), db.table("groups")
    seen: list[str] = []
    db.sql.set_trace_callback(seen.append)
    try:
        users.join(groups, on=("gid", "id")).select(what=ALL)
    finally:
        db.sql.set_trace_callback(None)
    assert len(seen) == 1
    assert seen[0].lstrip().lower().startswith("select")
    assert "pragma" not in seen[0].lower() and "sqlite_master" not in seen[0].lower()


def test_what_is_required_for_joins(db):
    users, groups = db.table("users"), db.table("groups")
    with raises(TypeError):
        users.join(groups, on=("gid", "id")).select()
    with raises(TypeError):
        users.join(groups, on=("gid", "id")).select_one()


def test_star_is_rejected_in_join_what(db):
    users, groups = db.table("users"), db.table("groups")
    q = users.join(groups, on=("gid", "id"))
    with raises(ValueError):
        q.select(what={"users": "*", "groups": GROUPS})
    with raises(ValueError):
        q.select(what="*")


def test_what_keys_must_be_known_tables_or_aliases(db):
    users, groups = db.table("users"), db.table("groups")
    with raises(ValueError):
        users.join(groups, on=("gid", "id")).select(what={"users": USERS, "nope": ("id",)})


def test_what_may_omit_a_joined_table_but_where_can_still_use_it(db):
    """Filtering/ordering on a table you don't select is plain SQL; keep it working."""
    users, groups = db.table("users"), db.table("groups")
    rows = users.join(groups, on=("gid", "id")).select(
        what={"users": ("username",)}, where={"groups.name": "root"}
    )
    assert rows == [{"users": {"username": "alice"}}]


def test_table_alias_and_self_join(db):
    users = db.table("users")
    rows = (
        users.alias("a")
        .join(users.alias("b"), on=("a.gid", "b.gid"))
        .select(what={"a": USERS, "b": USERS}, where={"a.id": 1, "b.id": 1})
    )
    assert rows == [{"a": {"id": 1, "username": "alice", "gid": 1},
                     "b": {"id": 1, "username": "alice", "gid": 1}}]


def test_column_alias_in_join(db):
    users, groups = db.table("users"), db.table("groups")
    row = users.join(groups, on=("gid", "id")).select_one(
        where={"users.id": 1},
        what={"users": ("id", as_("username", "name")),
              "groups": (as_("name", "group_name"),)},
    )
    assert row == {"users": {"id": 1, "name": "alice"},
                   "groups": {"group_name": "root"}}


def test_column_alias_in_plain_select(db):
    """Closes the open `Table API 'as' keyword` item in TODO.md."""
    users = db.table("users")
    rows = users.select(what=("id", as_("username", "name")), order=("id", "asc"))
    assert rows[0] == {"id": 1, "name": "alice"}


def test_alias_must_be_safe_identifier(db):
    users, groups = db.table("users"), db.table("groups")
    with raises(SecurityError):
        users.alias("a; drop table users")
    with raises(SecurityError):
        users.join(groups, on=("gid", "id"), alias="select")  # reserved word
    with raises(SecurityError):
        users.select(what=(as_("id", "x y"),))


def test_where_with_operators_on_qualified_columns(db):
    users, groups = db.table("users"), db.table("groups")
    rows = users.join(groups, on=("gid", "id")).select(
        what=ALL, where={"users.id": op >= 2, "groups.name": "user"}
    )
    assert [r["users"]["username"] for r in rows] == ["bob"]


def test_order_limit_offset(db):
    users, groups = db.table("users"), db.table("groups")
    q = users.join(groups, on=("gid", "id"), type="left")
    rows = q.select(what=ALL, order=("users.id", "desc"), limit=1, offset=1)
    assert [r["users"]["id"] for r in rows] == [2]


def test_where_key_must_reference_a_known_table(db):
    users, groups = db.table("users"), db.table("groups")
    with raises(ValueError):
        users.join(groups, on=("gid", "id")).select(what=ALL, where={"nope.id": 1})


def test_chained_joins_with_explicit_qualified_on(db):
    users, groups = db.table("users"), db.table("groups")
    rows = (
        users.alias("u")
        .join(groups.alias("g"), on=("u.gid", "g.id"))
        .join(groups.alias("g2"), on=("g.id", "g2.id"))
        .select(what={"u": ("id",), "g2": ("name",)}, where={"u.id": 1})
    )
    assert rows[0]["g2"]["name"] == "root"


def test_multiple_on_conditions_are_anded(db):
    users, groups = db.table("users"), db.table("groups")
    rows = users.join(groups, on=[("gid", "id"), ("id", "id")]).select(
        what={"users": ("id",)}
    )
    assert [r["users"]["id"] for r in rows] == [1, 2]  # gid == id for both


def test_missing_on_requires_explicit_cross(db):
    users, groups = db.table("users"), db.table("groups")
    with raises(ValueError):
        users.join(groups)  # silent cartesian product is almost never intended
    rows = users.join(groups, type="cross").select(what=ALL)
    assert len(rows) == 9  # 3 * 3


def test_unknown_join_type_is_rejected(db):
    users, groups = db.table("users"), db.table("groups")
    with raises(SecurityError):
        users.join(groups, on=("gid", "id"), type="left; drop table users")


@mark.skipif(sqlite_version_info < (3, 39, 0), reason="RIGHT/FULL need SQLite 3.39+")
def test_right_and_full_join(db):
    users, groups = db.table("users"), db.table("groups")
    right = users.join(groups, on=("gid", "id"), type="right").select(what=ALL)
    assert any(r["users"] is None and r["groups"]["name"] == "empty" for r in right)
    full = users.join(groups, on=("gid", "id"), type="full").select(what=ALL)
    assert len(full) == 4  # alice, bob, carol(orphan), empty group


@mark.skipif(sqlite_version_info >= (3, 39, 0), reason="only for old SQLite")
def test_right_join_on_old_sqlite_raises(db):
    users, groups = db.table("users"), db.table("groups")
    with raises(NotImplementedError):
        users.join(groups, on=("gid", "id"), type="right")


def test_join_query_is_read_only(db):
    users, groups = db.table("users"), db.table("groups")
    q = users.join(groups, on=("gid", "id"))
    for name in ("insert", "update", "delete"):
        assert not hasattr(q, name)


def test_values_are_parameterized_not_interpolated(db):
    users, groups = db.table("users"), db.table("groups")
    evil = "x' or '1'='1"
    rows = users.join(groups, on=("gid", "id")).select(
        what=ALL, where={"groups.name": evil}
    )
    assert rows == []
    assert len(db.table("users").select()) == 3


def test_where_keys_must_be_qualified(db):
    users, groups = db.table("users"), db.table("groups")
    with raises(ValueError):
        users.join(groups, on=("gid", "id")).select(what=ALL, where={"id": 1})


def test_offset_without_limit_works(db):
    """SQLite rejects a bare OFFSET; the builder must add `limit -1`."""
    users, groups = db.table("users"), db.table("groups")
    rows = users.join(groups, on=("gid", "id"), type="left").select(
        what={"users": ("id",)}, order=("users.id", "asc"), offset=2
    )
    assert rows == [{"users": {"id": 3}}]


def test_select_one_without_match_is_an_empty_row(db):
    users, groups = db.table("users"), db.table("groups")
    row = users.join(groups, on=("gid", "id")).select_one(what=ALL, where={"users.id": 99})
    assert row == {}


def test_in_like_between_on_qualified_columns(db):
    users, groups = db.table("users"), db.table("groups")
    q = users.join(groups, on=("gid", "id"))
    what = {"users": ("id",)}
    assert [r["users"]["id"] for r in q.select(what=what, where={"users.id": op.in_([1, 2])})] == [1, 2]
    assert [r["users"]["id"] for r in q.select(what=what, where={"users.username": op.like("a%")})] == [1]
    assert [r["users"]["id"] for r in q.select(what=what, where={"users.id": op.between(2, 3)})] == [2]


def test_nulls_propagate_through_chained_outer_joins(db):
    users, groups = db.table("users"), db.table("groups")
    rows = (
        users.alias("a")
        .join(groups.alias("g"), on=("gid", "id"), type="left")
        .join(users.alias("b"), on=("g.id", "b.gid"), type="left")
        .select(what={"a": ("id",), "g": ("name",), "b": ("username",)},
                order=("a.id", "asc"))
    )
    assert rows[2] == {"a": {"id": 3}, "g": None, "b": None}


def test_duplicate_alias_and_foreign_database_are_rejected(db):
    users = db.table("users")
    with raises(ValueError):
        users.join(users, on=("id", "id"))  # needs .alias() for the second use
    other = Database(":memory:").create_table("x", [integer("id")])
    with raises(ValueError):
        users.join(other, on=("id", "id"))


def test_injection_attempts_are_refused(db):
    users, groups = db.table("users"), db.table("groups")
    q = users.join(groups, on=("gid", "id"))
    with raises(SecurityError):
        users.join(groups, on=("gid", "id; drop table users"))
    with raises(SecurityError):
        q.select(what=ALL, order=("users.id", "asc; drop table users"))
    with raises(SecurityError):
        q.select(what={"users": ("id) from users; --",)})
    with raises(SecurityError):
        q.select(what=ALL, where={"users.id or 1=1 --": 1})
    assert len(users.select()) == 3


def test_plain_select_single_column_tuple_and_alias(db):
    """Fixes the old `what=("col",)` KeyError and supports aliases there."""
    users = db.table("users")
    assert users.select(what=("username",), order=("id", "asc")) == ["alice", "bob", "carol"]
    assert users.select_one(what=("username",), where={"id": 1}) == "alice"
    assert users.select(what=as_("username", "n"), order=("id", "asc"))[0] == "alice"
    assert users.select_one(what=as_("username", "n"), where={"id": 2}) == "bob"
