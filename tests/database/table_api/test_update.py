"""Table API update test"""

from pytest import raises
from luminadb import Database
from luminadb.operators import op
from luminadb.errors import TableRemovedError

from ..setup import (
    database,
    groups,
    users,
    setup_database_builder,
    save_report,
    setup_database_fns,
    GROUP_UNEW,
    USER_UNEW,
)


def test_update():
    """test 0201 update"""
    db = Database(":memory:")
    setup_database_builder(db)
    usrs = db.table("users")
    grps = db.table("groups")
    assert grps.update({"id": op == 1}, GROUP_UNEW[0]) == 1
    assert usrs.update({"id": op == 1}, USER_UNEW[0]) == 1
    assert save_report("02_update", db, grps, usrs)


def test_update_one():
    """test 0202 update one"""
    db = Database(":memory:")
    setup_database_builder(db)
    usrs = db.table("users")
    grps = db.table("groups")
    assert grps.update_one({"id": op == 1}, GROUP_UNEW[0]) == 1
    assert usrs.update_one({"id": op == 1}, USER_UNEW[0]) == 1
    assert save_report("02_update", db, grps, usrs)


def test_update_limited_order():
    """Test 0203 update limited with order"""
    db = Database(":memory:")
    setup_database_fns(db)
    checkout = db.table("checkout")
    assert (
        checkout.update({"quantity": 50}, {"quantity": 70}, limit=2) == 2
    )  # type: ignore
    assert len(checkout.select({"quantity": 70}, limit=2)) == 2

def test_finish():
    """test 0300 finish"""
    with raises(TableRemovedError):
        assert save_report("04_finish", database, groups, users)
        assert database.delete_table("groups") is None
        assert database.delete_table("users") is None
        assert save_report("05_finish", database, groups, users)
