"""Tests for auto increment feature"""

from pytest import raises

from luminadb import Database, integer, text
from luminadb.column import Column


def test_column_constructor_autoinc_non_integer_raises():
    """Column(...) with auto_increment on non-integer must raise"""
    with raises(ValueError):
        Column("a", "text", auto_increment=True)


def test_builder_autoinc_on_defined_non_integer_raises():
    """BuilderColumn.auto_increment must raise if type already non-integer"""
    with raises(ValueError):
        # text(...) already defines type as text
        text("a").auto_increment()


def test_create_table_and_insert_autoincrement_integer():
    """Creating a table with INTEGER PRIMARY AUTOINCREMENT should auto increment ids"""
    db = Database(":memory:")
    t = db.create_table("t_autoinc", [integer("id").primary().auto_increment(), text("name")])

    # Insert without id should let sqlite assign autoincrement values
    first = t.insert({"name": "alice"})
    second = t.insert({"name": "bob"})

    assert first == 1
    assert second == 2

    ids = t.select(what="id")
    assert ids == [1, 2]
