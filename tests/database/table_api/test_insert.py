"""Table API insertion tests"""

from pytest import raises
from luminadb import Database, text, SKIP
from luminadb.errors import CuteDemonLordException

from ..setup import groups, users, database, save_report, GROUP_NEW, USER_NEW

def test_insert():
    """test 0100 insert"""
    assert groups.insert_many(GROUP_NEW) is None
    assert users.insert_many(USER_NEW) is None
    users._sql.commit()  # pylint: disable=protected-access
    assert not not users.select()  # pylint: disable=unnecessary-negation
    assert save_report("01_insert", database, groups, users)


def test_insert_with():
    """Test 0101 insert in with-statement"""
    db = Database(":memory:")
    table = db.create_table("a", [text("name")])
    try:
        with table:
            table.insert({"name": "Admin"})
            raise CuteDemonLordException(
                "Haha, i'm cute demon lord and I cast [Insert Dispell]"
            )
    except CuteDemonLordException:
        pass
    assert table.select() == []


def test_insert_empty():
    """Test 0101 insert in with-statement"""
    db = Database(":memory:")
    table = db.create_table("a", [text("name")])
    with raises(ValueError):
        table.insert({"name": SKIP})
