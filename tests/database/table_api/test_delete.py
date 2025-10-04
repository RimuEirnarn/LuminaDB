"""Table API delete tests"""

from luminadb import Database
from ..setup import setup_database_builder, setup_database_fns, USER_BASE

def test_delete():
    """Test 0600 Deletion test"""
    db = Database(":memory:")
    setup_database_builder(db)
    users = db.table("users")
    assert users.delete() == len(USER_BASE)


def test_delete_condition():
    """Test 0601 Delete based on ID"""
    db = Database(":memory:")
    setup_database_builder(db)
    users = db.table("users")
    assert users.delete({"id": 1}) == 1


def test_delete_limit():
    """Test 0602 Delete with limit"""
    db = Database(":memory:")
    setup_database_builder(db)
    users = db.table("users")
    assert users.delete(limit=1) == 1


def test_delete_limit_condition():
    """Test 0603 Delete limit with certain condition"""
    db = Database(":memory:")
    setup_database_fns(db)
    checkout = db.table("checkout")
    assert checkout.delete({"quantity": 50}, limit=2) == 2


def test_delete_order():
    """Test 0603 Delete limit with certain condition"""
    db = Database(":memory:")
    setup_database_fns(db)
    checkout = db.table("checkout")
    assert checkout.delete({"quantity": 50}, order="asc", limit=2) == 2  # type: ignore
