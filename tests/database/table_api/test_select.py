"""Test Table API select"""

from luminadb import Database, text, integer
from luminadb.operators import op, eq

from ..setup import (
    GROUP_BASE,
    USER_BASE,
    save_report,
    setup_database,
    setup_orderable,
    USER_NAMEBASE,
    GROUP_BASE_CRUNCHED,
    setup_database_builder,
    setup_database_fns,
    temp_dir
)

database = Database(temp_dir / "test-selection.db")  # type: ignore
setup_database_builder(database)
setup_database_fns(database)
users = database.table("users")
groups = database.table("groups")


def test_select():
    """test 0000 select"""
    assert groups.select_one({"id": op == 0}) == GROUP_BASE[0]
    assert groups.select() == GROUP_BASE
    assert users.select_one({"id": op == 0}) == USER_BASE[0]
    assert users.select() == USER_BASE
    assert save_report("00_test", database, groups, users)


def test_00_01_select():
    """Test 0001 select"""
    assert groups.select_one([eq("id", 0)]) == GROUP_BASE[0]
    assert groups.select() == GROUP_BASE
    assert users.select_one([eq("id", 0)]) == USER_BASE[0]
    assert users.select() == USER_BASE
    assert save_report("00_test", database, groups, users)


def test_00_02_select():
    """Test 0002 select"""
    assert groups.select_one({"id": 0}) == GROUP_BASE[0]
    assert groups.select() == GROUP_BASE
    assert users.select_one({"id": 0}) == USER_BASE[0]
    assert users.select() == USER_BASE
    assert save_report("00_test", database, groups, users)

    assert users.select_one({"id": 0}, ("username", "role")) == USER_NAMEBASE[0]
    assert save_report("00_test", database, groups, users)


def test_select_crunch():
    """Test 0004 select with crunch"""
    db = Database(":memory:")
    setup_database(db)
    gr = db.table("groups")
    assert gr.select(flatten=True) == GROUP_BASE_CRUNCHED


def test_select_one_only_one_item():
    """Test 0005"""
    db = Database(":memory:")
    setup_database(db)
    gr = db.table("groups")
    assert gr.select_one({"id": 0}, "name") == "root"


def test_select_order_by():
    """Test 0006 Select order by"""
    db = Database(":memory:")
    setup_orderable(db)
    items = db.table("items")
    first_high = items.select(what="quantity", limit=3, order=("quantity", "asc"))
    first_low = items.select(what="quantity", limit=3, order=("quantity", "desc"))
    assert first_high == [0, 1, 2]
    assert first_low == [99, 98, 97]

def test_select_subquery():
    """Test 0007 Select with subqueries"""
    db = Database(":memory:")
    notes = db.create_table("notes", [text("id").primary(), text("content")])
    notes.insert({"id": "0", "content": "abc"})
    data = notes.select_one({"id": notes.subquery({"id": "0"}, "id", limit=1)})
    assert data is not None

def test_paginate_select():
    """Test 0500 Pagination select"""
    data = []
    for a, b in zip(range(0, 100), range(1000, 1100)):
        data.append({"x": a, "y": b})

    db = Database(":memory:")
    nums = db.create_table("nums", [integer("x"), integer("y")])

    nums.insert_many(data)

    for i in nums.paginate_select():
        assert i
