"""Test other features"""

from sqlite3 import OperationalError
from random import randint

from pytest import raises
from luminadb import Database, integer, text
from luminadb.workers import DatabaseWorker
from luminadb.index import Index

from ..setup import setup_database_fns, setup_database, count


def test_function_count():
    """Test 0800 Count(*) usage"""
    database = Database(":memory:")
    setup_database_fns(database)
    counted = count("*")
    data = database.table("checkout").select(what=counted)
    print(data)
    assert data == 4


def test_select_error():
    """Test errors"""
    db = Database(":memory:")
    setup_database(db)
    groups = db.table("groups")
    with raises(OperationalError):
        groups.select({"nothing": None})


def test_pragma_foreign_key():
    """Test 1001 PRAGMA tests"""
    db = Database(":memory:")
    setup_database(db)
    db.foreign_pragma("ON")
    db.foreign_pragma("OFF")
    assert db.foreign_pragma() == {"foreign_keys": 0}


def test_pragma_optimize():
    """Test 1002 PRAGMA optimize"""
    db = Database(":memory:")
    setup_database(db)
    db.optimize()
    assert 1 == 1


def test_pragma_shrink():
    """Test 1003 PRAGMA shrink"""
    db = Database(":memory:")
    t = db.create_table("t", [integer("a")])
    t.insert_many([{"a": a} for a in range(10000)])
    t.commit()
    db.shrink_memory()
    assert 1 == 1


def test_vacuum():
    """Test 1004 vacuum"""
    db = Database(":memory:")
    t = db.create_table("t", [integer("a")])
    t.insert_many([{"a": a} for a in range(10000)])
    t.commit()
    _ = [t.delete_one({"a": randint(0, 1000)})]
    t.commit()
    db.vacuum()


# @mark.skipif(sys.version_info < (3, 13), reason="Worker feature is exclusive in 3.13")
def test_worker():
    """Test worker"""

    db = DatabaseWorker(":memory:")
    t = db.create_table("t", [integer("a")])
    t.insert({"a": 1})
    t.commit()
    assert t.select_one({"a": 1}) is not None
    db.close()


def test_index():
    """Test index"""

    db = Database(":memory:")
    t = db.create_table("t", [integer("a"), text("b")])
    index = Index('testindex').target('t').columns('a', 'b')
    assert db.create_index(index) is None, "Index should be created"
    t.select_one()
    assert db.delete_index(index) is None, "Index should be destroyed"
