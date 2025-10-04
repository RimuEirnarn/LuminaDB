"""Core database utility"""
# pylint: disable=useless-parent-delegation

# This module must not import anything from this package except errors.

from uuid import uuid4
from re import Pattern
from re import compile as re_compile
from re import escape as re_escape
from sqlite3 import Cursor, connect
from string import punctuation
from typing import Any, Generic, Iterable, Iterator, Mapping, TypeVar, TypeAlias

from .errors import SecurityError


T = TypeVar("T")
_INVALID_STR = punctuation.replace("_", "")
_re_valid = re_compile(f"[{re_escape(_INVALID_STR)}]+")

_SQLITE_KEYWORDS = {
    "ABORT",
    "ACTION",
    "ADD",
    "AFTER",
    "ALL",
    "ALTER",
    "ANALYZE",
    "AND",
    "AS",
    "ASC",
    "ATTACH",
    "AUTOINCREMENT",
    "BEFORE",
    "BEGIN",
    "BETWEEN",
    "BY",
    "CASCADE",
    "CASE",
    "CAST",
    "CHECK",
    "COLLATE",
    "COLUMN",
    "COMMIT",
    "CONFLICT",
    "CONSTRAINT",
    "CREATE",
    "CROSS",
    "CURRENT_DATE",
    "CURRENT_TIME",
    "CURRENT_TIMESTAMP",
    "DATABASE",
    "DEFAULT",
    "DEFERRABLE",
    "DEFERRED",
    "DELETE",
    "DESC",
    "DETACH",
    "DISTINCT",
    "DROP",
    "EACH",
    "ELSE",
    "END",
    "ESCAPE",
    "EXCEPT",
    "EXCLUSIVE",
    "EXISTS",
    "EXPLAIN",
    "FAIL",
    "FOR",
    "FOREIGN",
    "FROM",
    "FULL",
    "GLOB",
    "GROUP",
    "HAVING",
    "IF",
    "IGNORE",
    "IMMEDIATE",
    "IN",
    "INDEX",
    "INDEXED",
    "INITIALLY",
    "INNER",
    "INSERT",
    "INSTEAD",
    "INTERSECT",
    "INTO",
    "IS",
    "ISNULL",
    "JOIN",
    "KEY",
    "LEFT",
    "LIKE",
    "LIMIT",
    "MATCH",
    "NATURAL",
    "NO",
    "NOT",
    "NOTNULL",
    "NULL",
    "OF",
    "OFFSET",
    "ON",
    "OR",
    "ORDER",
    "OUTER",
    "PLAN",
    "PRAGMA",
    "PRIMARY",
    "QUERY",
    "RAISE",
    "RECURSIVE",
    "REFERENCES",
    "REGEXP",
    "REINDEX",
    "RELEASE",
    "RENAME",
    "REPLACE",
    "RESTRICT",
    "RIGHT",
    "ROLLBACK",
    "ROW",
    "SAVEPOINT",
    "SELECT",
    "SET",
    "TABLE",
    "TEMP",
    "TEMPORARY",
    "THEN",
    "TO",
    "TRANSACTION",
    "TRIGGER",
    "UNION",
    "UNIQUE",
    "UPDATE",
    "USING",
    "VACUUM",
    "VALUES",
    "VIEW",
    "VIRTUAL",
    "WHEN",
    "WHERE",
    "WITH",
    "WITHOUT",
}


class NullObject:
    """Null object"""

    __obj = None

    def __new__(cls):
        if cls.__obj is None:
            cls.__obj = object.__new__(cls)
        return cls.__obj

    def __init__(self) -> None:
        pass

    def __repr__(self) -> str:
        return "null"

    def __str__(self) -> str:
        return repr(self)

    def __int__(self) -> int:
        return 0

    def __float__(self) -> float:
        return 0.0


class Sentinel:  # pylint: disable=too-few-public-methods
    """A Sentinel value, has unique behavior for Table API and Model API.

    Pre-defined value:
        - Null, use this one if you're unsure if the data you pulled exists or not.
            The query builder will remove it if it detects Null sentinel."""

    def __repr__(self) -> str:
        return "<Sentinel>"


class Row(Mapping[str, T], Generic[T], dict): # type: ignore
    """Attribute Dictionary"""

    def __setitem__(self, key: str, value: T):
        super().__setitem__(key, value)

    def __getitem__(self, key: str) -> T:
        return dict.__getitem__(self, key)

    def __iter__(self) -> Iterator[str]:
        return dict.__iter__(self)

    def __len__(self) -> int:
        return dict.__len__(self)

    def __getattr__(self, __name: str) -> Any:
        try:
            return super().__getattribute__(__name)
        except AttributeError:
            return self[__name]


def dict_factory(cursor, row):
    """dict factory"""
    fields = [column[0] for column in cursor.description]
    return Row(zip(fields, row))
    # return {key: value for key, value in zip(fields, row)}


def sqlite_multithread_check():
    """sqlite mulththread check"""
    thread_safe = {0: 0, 2: 1, 1: 3}
    conn = connect(":memory:", check_same_thread=False)
    data = conn.execute(
        "select * from pragma_compile_options where compile_options \
like 'THREADSAFE=%'"
    ).fetchone()[0]
    conn.close()

    threadsafety_value = int(data.split("=")[1])
    return thread_safe[threadsafety_value]


def matches(pattern: Pattern, value: str):
    """matches"""
    if len(pattern.findall(value)) == 0:
        return False
    return True


def check_one(data: str, bypass_list: tuple[str, ...] | None = None):
    """check one to check if a string contains illegal character OR
    if it is a reserved SQL keyword"""
    if matches(_re_valid, data) is True:
        exc = SecurityError("Cannot parse unsafe data.")
        exc.add_note(f"Target: {data}")
        raise exc
    bp = bypass_list or []
    if data.upper() in _SQLITE_KEYWORDS:
        if data in bp:
            return data
        raise SecurityError(f'"{data}" is a reserved SQL keyword and cannot be used.')
    return data


def check_iter(data: Iterable[str], bypass_list: tuple[str, ...] | None = None):
    """An iterable checks as it's check_one"""
    for val in data:
        check_one(val, bypass_list)


class WithCursor(Cursor):
    """With cursor"""

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        if exc_type and exc_value and traceback:
            return True
        self.connection.commit()
        return True

    def __repr__(self) -> str:
        return type(self).__name__


class NoopResource:
    """No-op resource control"""

    def close(self):
        """Close this resource"""

    def open(self, *_, **__):
        """open something"""


def test_installed():
    """Test if project is truly installed. It was meant for docs compatibility and thus,
    always returns true"""
    return True

def generate_ids():
    """Generate ids for statements"""
    return str(uuid4().int)


null = NullObject()
Null = Sentinel()
AttrDict = Row
Query: TypeAlias = Row[Any]
SquashedQueries: TypeAlias = Row[list[Any]]
Queries: TypeAlias = list[Query] | SquashedQueries

def crunch(query: Queries | list[dict[str, Any]]) -> Row[list[Any]]:
    """Crunch queries into Row that all of its value become a list."""
    data: Row[list[Any]] = Row()
    for value in query:
        for key, val in value.items(): # type: ignore
            if key not in data:
                data[key] = []
            data[key].append(val)
    return data



__all__ = [
    "null",
    "Null",
    "WithCursor",
    "check_iter",
    "check_one",
    "matches",
    "Row",
    "AttrDict",
    "NullObject",
    "sqlite_multithread_check",
    "NoopResource",
]
