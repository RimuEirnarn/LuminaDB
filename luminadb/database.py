"""SQLite Database"""

from atexit import register as finalize
from sqlite3 import OperationalError, connect
from typing import Iterable, Literal, Optional

from luminadb._debug import if_debug_print

from .utils import (
    WithCursor,
    check_iter,
    check_one,
    dict_factory,
    sqlite_multithread_check,
)
from .column import BuilderColumn, Column
from .query_builder.table_creation import extract_table_creations
from .table import Table
from .errors import DatabaseExistsError, DatabaseMissingError
from .index import Index

Columns = Iterable[Column] | Iterable[BuilderColumn]

__all__ = ["Database"]

IGNORE_TABLE_CHECKS = ("sqlite_master", "sqlite_temp_schema", "sqlite_temp_master")


class Database: # pylint: disable=too-many-instance-attributes
    """Sqlite3 database, this provide basic integration.

    Custom flags:
        strict : Certain actions are prevented when active, i.e, initializing nonexistent tables
        forgive: Certain actions are replaced when active, i.e, replacing .create_table to .table
                 when a table exists"""

    def __init__(self, path: str, **kwargs) -> None:
        kwargs["check_same_thread"] = sqlite_multithread_check() != 3
        self._path = path
        self._strict: bool = kwargs.get("strict", True)
        self._forgive: bool = kwargs.get("forgive", True)
        self._active = []
        if 'forgive' in kwargs:
            del kwargs['forgive']
        if 'strict' in kwargs:
            del kwargs['strict']
        self._config = None
        self._closed = False
        self._table_class = Table
        if not self._closed or self.__dict__.get("_initiated", False) is False:
            finalize(self._finalizer)
            self._initiated = True
        self._kwargs = kwargs
        self._create_connection()

    def _create_connection(self):
        self._database = connect(self._path, **self._kwargs)
        self._database.row_factory = dict_factory

    def _finalizer(self):
        self.close()

    def cursor(self) -> WithCursor:
        """Create cursor"""
        return self._database.cursor(WithCursor)  # type: ignore

    def create_table(self, table: str, columns: Columns):
        """Create table

        Args:
            table (str): Table name
            columns (Iterable[Column]): Columns for table

        Returns:
            Table: Newly created table
        """
        columns = (
            column.to_column() if isinstance(column, BuilderColumn) else column
            for column in columns
        )
        tbquery = extract_table_creations(columns)
        query = f"create table {table} ({tbquery})"

        if_debug_print(query)

        if self._forgive and self.check_table(table):
            return self.table(table, columns)

        try:
            cursor = self._database.cursor()
            cursor.execute(query)
            self._database.commit()
        except OperationalError as error:
            if "already exists" in str(error):
                dberror = DatabaseExistsError(f"table {table} already exists.")
                dberror.add_note(f"{type(error).__name__}: {error!s}")
                raise dberror from error
            error.add_note(f"Query: {query}")
            raise error
        table_ = self.table(table, columns)
        table_._deleted = False  # pylint: disable=protected-access
        return table_

    def delete_table(self, table: str):
        """Delete an existing table

        Args:
            table (str): table name
        """
        check_one(table)
        table_ = self.table(table)
        self._database.cursor().execute(f"drop table {table}")
        # pylint: disable-next=protected-access
        table_._delete_hook()  # pylint: disable=protected-access

    def table(self, table: str, __columns: Optional[Iterable[Column]] = None):  # type: ignore
        """fetch table"""

        if self._strict and not self.check_table(table):
            raise DatabaseMissingError(f"table {table} does not exists.")

        try:
            this_table = self._table_class(self, table, __columns)
        except OperationalError as exc:
            dberror = DatabaseMissingError(f"table {table} does not exists")
            dberror.add_note(f"{type(exc).__name__}: {exc!s}")
            raise dberror from None
        return this_table

    def reset_table(self, table: str, columns: Columns) -> Table:
        """Reset existing table with new, this rewrote entire table than altering it."""
        try:
            self.delete_table(table)
        except OperationalError:
            pass
        return self.create_table(table, columns)

    def rename_table(self, old_table: str, new_table: str) -> Table:
        """Rename existing table to a new one."""
        check_iter((old_table, new_table))
        cursor = self.sql.cursor()
        cursor.execute(f"alter table {old_table} rename to {new_table}")
        self.sql.commit()
        return self.table(new_table)

    def check_table(self, table: str):
        """Check if table is exists or not."""
        # if self._path in PLUGINS_PATH:
        #     plugin = self._path[2:]
        #     raise ValueError(f"Plugin {plugin} must redefine check_table.")
        check_one(table)
        if table in IGNORE_TABLE_CHECKS:
            return True  # Let's return true.
        cursor = self.sql.cursor()
        cursor.execute(
            "select name from sqlite_master where type='table' and name=?", (table,)
        )
        return cursor.fetchone() is not None

    def create_index(self, index: Index):
        """Create an index"""
        with self.sql as dbcursor:
            dbcursor.execute(index.build_sql())

    def delete_index(self, name: str | Index, exists_ok: bool = False):
        """Drop an index"""
        _name = name.index_name if isinstance(name, Index) else name
        check_one(_name)
        with self.sql as dbcursor:
            if_ok = "if exists" if exists_ok else ""
            dbcursor.execute(f"drop index {if_ok} {_name}")

    def __repr__(self) -> str:
        return f"<{type(self).__name__} {id(self)}>"

    def close(self):
        """Close database"""
        if self._closed:
            return
        self._database.close()
        if self.path == ":memory:":
            self._closed = True
            return
        self._closed = True

    def tables(self) -> tuple[Table, ...]:
        """Return tuple containing all table except internal tables"""
        master = self.table("sqlite_master")
        listed = []
        for table in master.select():
            if table.type == "table":
                listed.append(self.table(table.name))
        return tuple(listed)

    def commit(self):
        """Commit changes to database"""
        self._database.commit()

    def rollback(self):
        """Rollback changes"""
        self._database.rollback()

    def foreign_pragma(self, bool_state: Literal["ON", "OFF", ""] = ""):
        """Enable/disable foreign key pragma"""
        if bool_state not in ("ON", "OFF", ""):
            raise ValueError("Either ON/OFF for foreign key pragma.")
        return self._database.execute(
            f"PRAGMA foreign_keys{'='+bool_state if bool_state else ''}"
        ).fetchone()  # pylint: disable=line-too-long

    def optimize(self):
        """Optimize current database"""
        return self._database.execute("PRAGMA optimize").fetchone()

    def shrink_memory(self):
        """Shrink memories from database as much as it can."""
        return self._database.execute("PRAGMA shrink_memory").fetchone()

    def vacuum(self):
        """Vacuum this database"""
        return self._database.execute("VACUUM").fetchone()

    @property
    def closed(self):
        """Is database closed?"""
        return self._closed

    @closed.setter
    def closed(self, __o: bool):
        """Is database closed?"""
        if __o:
            self.close()
            return
        raise ValueError("Expected non-false/non-null value")

    @property
    def path(self):
        """Path to SQL Connection"""
        return self._path or ":memory:"

    @property
    def sql(self):
        """SQL Connection"""
        return self._database
