"""Setup"""

# pylint: disable=redefined-outer-name,invalid-name
from json import dumps
from io import StringIO
from os.path import abspath
from types import SimpleNamespace
from tempfile import mkdtemp
from pathlib import Path

from luminadb import Column, Database, integer, text
from luminadb.models import Primary, Unique, model, BaseModel, Foreign, CASCADE
from luminadb.utils import crunch
from luminadb.functions import Function

def parse(data):
    """parse"""
    return dumps(data, indent=2)


temp_dir = Path(mkdtemp())

config = SimpleNamespace()
file = abspath(f"{__file__}/../../reports.txt")
pstdout = StringIO()

count = Function("COUNT")

GROUP_BASE = [{"id": 0, "name": "root"}, {"id": 1, "name": "user"}]

GROUP_BASE_CRUNCHED = crunch(GROUP_BASE)

GROUP_NAMEBASE = [{"name": "root"}, {"name": "user"}]

USER_NAMEBASE = [{"username": "root", "role": "root"}]

USER_BASE = [
    {"id": 0, "username": "root", "role": "root", "gid": 0},
    {"id": 1, "username": "user", "role": "user", "gid": 1},
]

GROUP_NEW = [{"id": 2, "name": "test"}]

USER_NEW = [{"id": 2, "username": "test", "role": "test", "gid": 2}]

GROUP_UNEW = [{"id": 3, "name": "test1"}]

USER_UNEW = [
    {
        "id": 3,
        "username": "test0",
        "role": "test0",
    }
]

COUNT_DATA = [
    {"item_id": 1, "quantity": 50, "name": "A"},
    {"item_id": 2, "quantity": 50, "name": "A"},
    {"item_id": 3, "quantity": 50, "name": "A"},
    {"item_id": 4, "quantity": 50, "name": "A"},
]


def setup_database(database: Database):
    """setup database"""
    users = database.create_table(
        "users",
        [
            Column("id", "integer", unique=True, primary=True),
            Column("username", "text"),
            Column("role", "text", default="user"),
            Column("gid", "integer", foreign=True, foreign_ref="groups/id"),
        ],
    )
    groups = database.create_table(
        "groups", [Column("id", "integer", primary=True), Column("name", "text")]
    )
    groups.insert_many(GROUP_BASE)
    users.insert_many(USER_BASE)


def setup_database_builder(database: Database):
    """Setup database with builder pattern for column"""
    users = database.create_table(
        "users",
        [
            integer("id").primary(),
            text("username"),
            text("role").default("user"),
            integer("gid").foreign("groups/id"),
        ],
    )
    groups = database.create_table("groups", [integer("id").primary(), text("name")])
    groups.insert_many(GROUP_BASE)
    users.insert_many(USER_BASE)
    database.commit()


def setup_database_fns(database: Database):
    """Setup database used for functions"""
    checkout = database.create_table(
        "checkout", [integer("item_id").primary(), text("name"), integer("quantity")]
    )
    checkout.insert_many(COUNT_DATA)
    database.commit()


def setup_orderable(database: Database):
    """Setup database with order-able data"""
    items = database.create_table("items", [text("name"), integer("quantity")])
    items.insert_many([{"name": "a", "quantity": a} for a in range(100)])
    database.commit()


def setup_model_api(database: Database):
    # pylint: disable=missing-class-docstring
    """Setup Model API Users/Posts"""

    @model(database)
    class Users(BaseModel):
        __schema__ = (Primary("id"), Unique("username"))
        id: str
        username: str
        is_active: bool = True

    @model(database)
    class Posts(BaseModel):
        __schema__ = (Primary("id"),
                      Foreign("user_id", Users)
                              .on_delete(CASCADE)
                              .on_update(CASCADE))
        id: str
        user_id: str
        title: str
        content: str

    return Users, Posts

database = Database(temp_dir / "test.db")  # type: ignore
setup_database_builder(database)
setup_database_fns(database)
users = database.table("users")
groups = database.table("groups")

def save_report(tid, database, grouptb, usertb):
    """save report"""
    master = database.table("sqlite_master")
    print(
        f">> {tid}",
        "\n>> master ",
        parse(master.select()),
        "\n>> groups",
        parse(grouptb.select()),
        "\n>> users",
        parse(usertb.select()),
        end="\n=======\n",
        file=pstdout,
    )
    return True
