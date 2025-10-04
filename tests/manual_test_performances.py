"""Performance testing"""
# pylint: disable=all
from typing import Callable
from random import randint
from timeit import timeit
import os.path
from sys import path
path.insert(0, os.path.realpath(os.path.join(__file__, "../..")))

from luminadb import Column, Database, integer, text
from luminadb.signature import op
from luminadb.utils import crunch
from luminadb.functions import Function

count = Function("COUNT")

GROUP_BASE = [{
    "id": 0,
    "name": "root"
}, {
    "id": 1,
    "name": "user"
}]

GROUP_BASE_CRUNCHED = crunch(GROUP_BASE)

GROUP_NAMEBASE = [{
    "name": "root"
}, {
    "name": "user"
}]

USER_NAMEBASE = [{
    "username": 'root',
    'role': 'root'
}]

USER_BASE = [{
    "id": 0,
    "username": "root",
    "role": "root",
    "gid": 0
}, {
    "id": 1,
    "username": "user",
    "role": "user",
    "gid": 1
}]

GROUP_NEW = [{
    "id": 2,
    "name": "test"
}]

USER_NEW = [{
    "id": 2,
    "username": "test",
    "role": "test",
    "gid": 2
}]

GROUP_UNEW = [{
    "id": 3,
    "name": "test1"
}]

USER_UNEW = [{
    "id": 3,
    "username": "test0",
    "role": "test0",
}]

COUNT_DATA = [{
    "item_id": 1,
    "quantity": 50,
    "name": "A"
}, {
    "item_id": 2,
    "quantity": 50,
    "name": "A"

}, {
    "item_id": 3,
    "quantity": 50,
    "name": "A"

}, {
    "item_id": 4,
    "quantity": 50,
    "name": "A"

}]

performance_counter = {}


def setup_database(database: Database):
    """setup database"""
    users = database.create_table("users", [
        Column("id", "integer", unique=True, primary=True),
        Column("username", "text"),
        Column("role", "text", default="user"),
        Column("gid", "integer", foreign=True, foreign_ref="groups/id")
    ])
    groups = database.create_table("groups", [
        Column("id", "integer", primary=True),
        Column("name", "text")
    ])
    groups.insert_many(GROUP_BASE)
    users.insert_many(USER_BASE)


def setup_database_builder(database: Database):
    """Setup database with builder pattern for column"""
    users = database.create_table("users", [
        integer('id'),
        text('username'),
        text('role').default('user'),
        integer('gid').foreign('groups/id')
    ])
    groups = database.create_table("groups", [
        integer('id'),
        text('name')
    ])
    groups.insert_many(GROUP_BASE)
    users.insert_many(USER_BASE)
    database.commit()

def setup_database_fns(database: Database):
    """Setup database used for functions"""
    checkout = database.create_table("checkout", [
        integer("item_id"),
        text("name"),
        integer('quantity')
    ])
    checkout.insert_many(COUNT_DATA)
    database.commit()

def setup_database_1mdata(database: Database):
    """Setup database and fill it with 1m data"""
    items = database.create_table('items', [
        text('name'),
        integer('quantity')
    ])
    items.insert_many([{'name': f'item {randint(0, 1_000_000)}', 'quantity': randint(0, 100)} for _ in range(1_000_000)])
    database.commit()

def init_memdb(initializer: Callable[[Database], None]):
    db = Database(":memory:")
    initializer(db)
    return db

def testp_00_select():
    """Test Performance 0000 select"""
    db = init_memdb(setup_database_builder)
    users = db.table("users")
    performance_counter['generic_select:all'] = timeit(lambda: users.select())
    assert True

def testp_00_01_select_one():
    """Test Performance 0001 select one"""
    db = init_memdb(setup_database_builder)
    users = db.table("users")
    performance_counter['specific_select:one'] = timeit(lambda: users.select_one())
    assert True

def testp_00_02_select_only():
    """Test Performance 0003 select only"""
    db = init_memdb(setup_database_builder)
    users = db.table("users")
    performance_counter['specific_select:only_one'] = timeit(lambda: users.select_one({'id': 0}, ('username', 'role')))
    assert True

def testp_00_03_select_crunch():
    """Test Performance 0004 select with crunch"""
    db = init_memdb(setup_database_builder)
    groups = db.table('groups')
    performance_counter['generic_select:crunched_return'] = timeit(lambda: groups.select(squash=True))
    assert True

def testp_01_insert():
    """test 0100 insert"""
    db = init_memdb(setup_database_builder)
    groups = db.table('groups')
    performance_counter['generic_insert'] = timeit(lambda: groups.insert_many(GROUP_NEW))
    groups.delete()

def testp_02_01_update():
    """test 0201 update"""
    db = init_memdb(setup_database_builder)
    groups = db.table('groups')
    performance_counter['generic_update'] = timeit(lambda: (groups.update({"id": op == 1}, GROUP_UNEW[0]), groups.update({'id': 3}, {'id': 1})), number=500_000)

def testp_03_00_delete():
    """Test Performance 0300 single deletion test"""
    db = init_memdb(setup_database_1mdata)
    items = db.table('items')
    performance_counter['generic_delete'] = timeit(lambda: items.delete_one())

def testp_03_01_delete_all():
    """Test 0301 deletion test (all)"""
    db = init_memdb(setup_database_1mdata)
    items = db.table('items')
    performance_counter['generic_delete_all'] = timeit(lambda: items.delete(), number=1)

def testp_99_99_final():
    """Test final"""
    with open("perf-counter.txt", 'w') as f:
        sto = '\n'.join(f'{key} -> {value}' for key, value in performance_counter.items())
        f.write(f"""Performance testing:\n{sto}\n{'='*30}""")
