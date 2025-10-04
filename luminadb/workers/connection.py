"""Worker Connection"""

# pylint: disable=ungrouped-imports,possibly-used-before-assignment,too-few-public-methods,no-name-in-module,no-member

from sqlite3 import Cursor
from typing import Any
from .worker import Worker, WorkerType

def is_shutdown(worker: Worker):
    """Is shutdown?"""
    if worker.is_closed:
        return lambda *a, **kw: None
    return lambda *a, **kw: worker.push(worker.conn.close, "connection", *a, **kw)

class WorkerCursor:
    """Worker cursor"""

    __slots__ = ("_worker", "_cursor")

    def __init__(self, worker: Worker, real_cursor: Cursor):
        self._worker: Worker = worker
        self._cursor: Cursor = real_cursor

    def __getattr__(self, item):
        # print(self, item)
        if item in self.__slots__:
            return super().__getattribute__(item)

        attr = getattr(self._cursor, item)
        if callable(attr):
            return lambda *a, **kw: self._worker.push(attr, "cursor", *a, **kw)
        return attr


class WorkerConnection:
    """Worker connection"""

    def __init__(self, *args, worker_type: WorkerType = "thread", **kwargs):
        self._real: Worker = Worker(*args, worker_type=worker_type, **kwargs)

    def __getattr__(self, item):
        # print(self, item)
        if item in ("_real", "cursor"):
            return super().__getattribute__(item)

        if item in ("join",):
            return getattr(self._real, item)

        conn = self._real.conn
        attr = getattr(conn, item)
        if item == "close":
            return is_shutdown(self._real)

        if callable(attr):
            return lambda *a, **kw: self._real.push(attr, "connection", *a, **kw)
        return attr

    def cursor(self, *args, **kwargs):
        """Return cursor object"""
        real = self._real.push(
            getattr(self._real.conn, "cursor"), "connection", *args, **kwargs
        )
        return WorkerCursor(self._real, real)

    def __setattr__(self, name: str, value: Any) -> None:
        if name == "_real":
            super().__setattr__(name, value)
            return
        self._real.push(name, "connection", value=value)

    def __enter__(self):
        return self.cursor()

    def __exit__(self, _, exc, __):
        pass
