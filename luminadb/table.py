"""Table"""

# pylint: disable=too-many-arguments,too-many-public-methods,R0801

from contextvars import ContextVar
from sqlite3 import Connection, Error, OperationalError
from typing import (
    Any,
    Generator,
    Iterable,
    Literal,
    Optional,
    overload,
    TYPE_CHECKING
)

from luminadb.functions import ParsedFn, Function, count
from luminadb.subquery import SubQuery


from .utils import check_iter, check_one, Row, crunch
from ._debug import if_debug_print
from .column import BuilderColumn, Column
from .errors import TableRemovedError
from .query_builder import (
    # extract_single_column,
    # fetch_columns,
    build_select,
    build_insert,
    build_delete,
    build_update,
)
from .query_builder.typings import Condition
from .query_builder.table_creation import extract_single_column
# from .signature import op
from .typings import (
    Data,
    Orders,
    Query,
    # _MasterQuery,
    OnlyColumn,
    SquashedQueries,
    JustAColumn,
)

if TYPE_CHECKING:
    from .database import Database

# Let's add a little bit of 'black' magic here.
_null = Function("__NULL__")()
_tx_stack = ContextVar("_tx_stack", default=[])

class Table: # pylint: disable=too-many-instance-attributes
    """Table. Make sure you remember how the table goes."""


    def __init__(
        self,
        parent: "Database",  # type: ignore
        table: str,
        columns: Optional[Iterable[Column]] = None,  # type: ignore
    ) -> None:
        if parent.closed:
            raise ConnectionError("Connection to database is already closed.")
        self._parent_repr = repr(parent)
        self._db = parent
        self._sql: Connection = parent.sql
        # pylint: disable-next=protected-access
        self._sql_path = parent._path
        self._deleted = False
        self._force_dirty = False
        self._dirty = False
        self._auto = True
        self._table = check_one(table)
        self._prev_autocommit = None
        self._prev_auto = True
        self._columns: Optional[list[Column]] = list(columns) if columns else None

    def __enter__(self):
        self._prev_auto = self._auto
        self._prev_autocommit = self._sql.isolation_level

        self._auto = False
        self._sql.isolation_level = None
        self._begin_transaction()
        return self

    def __exit__(self, exc_type, _, __):
        if exc_type is None:
            self._commit_transaction()
            self._dirty = False
        else:
            self._rollback_transaction()
            self._dirty = False

        self._sql.isolation_level = self._prev_autocommit
        self._auto = self._prev_auto

    @property
    def deleted(self):
        """Is table deleted"""
        return self._deleted

    @property
    def name(self):
        """Table name"""
        return self._table

    @property
    def force_dirty(self):
        """Force dirty state, whether .selecting() on dirty/uncommitted data is allowed or not"""
        return self._force_dirty

    @force_dirty.setter
    def force_dirty(self, value: bool):
        """Force dirty state, whether .selecting() on dirty/uncommitted data is allowed or not"""
        if not isinstance(value, bool):
            return
        self._force_dirty = value

    @property
    def auto_commit(self):
        """Auto commit state of this instance"""
        return self._auto

    @auto_commit.setter
    def auto_commit(self, value: bool):
        if not isinstance(value, bool):
            return
        self._auto = value

    @property
    def in_transaction(self):
        """Returns True if the table is in an active transaction."""
        return not self._auto or self._sql.isolation_level is None

    def _finalize(self):
        pass

    def _delete_hook(self):
        try:
            self.select()
        except OperationalError:
            self._deleted = True

    def _exec(
        self,
        query: str,
        data: dict[str, Any] | list[dict[str, Any]],
        which: Literal["execute", "executemany"] = "execute",
    ):
        """Execute a sql query"""
        if_debug_print(query, '\n', data)
        cursor = self._sql.cursor()
        fn = cursor.execute if which == "execute" else cursor.executemany
        try:
            fn(query, data)
        except Error as exc:
            if str(exc).startswith("no such table:"):
                raise TableRemovedError(f"Table {self._table} doesn't exists anymore") from None
            exc.add_note(f"SQL query: {query}")
            exc.add_note(f"Arguments: {data}")
            exc.add_note(
                f"There's about {1 if isinstance(data, dict) else len(data)} value(s) inserted"
            )
            raise exc
        return cursor

    def _control(self):
        if self._deleted:
            raise TableRemovedError(f"{self._table} is already removed")

    def _query_control(self):
        if self._dirty and self._force_dirty is False:
            self._sql.commit()
            self._dirty = False

    def force_nodelete(self):
        """Force "undelete" table. Used if table was mistakenly assigned as
        deleted."""
        self._deleted = True

    def delete(
        self,
        where: Condition = None,
        limit: int = 0,
        order: Optional[Orders] = None,
    ):
        """Delete row or rows

        Args:
            where (Condition, optional): Condition to determine deletion
                See `Signature` class about conditional stuff. Defaults to None.
            limit (int, optional): Limit deletion by integer. Defaults to 0.
            order (Optional[Orders], optional): Order of deletion. Defaults to None.

        Returns:
            int: Rows affected
        """
        query, data = build_delete(self._table, where, limit, order)  # type: ignore
        self._control()
        cursor = self._exec(query, data)
        rcount = cursor.rowcount
        if not self.in_transaction:
            self._sql.commit()
        else:
            self._dirty = True
        return rcount

    def delete_one(self, where: Condition = None, order: Optional[Orders] = None):
        """Delete a row

        Args:
            where (Condition, optional): Conditional to determine deletion.
            Defaults to None.
            order (Optional[Orders], optional): Order of deletion. Defaults to None.
        """
        return self.delete(where, 1, order)

    def insert(self, data: Data):
        """Insert data to current table

        Args:
            data (Data): Data to insert. Make sure it's compatible with the table.

        Returns:
            int: Last rowid
        """
        query, _ = build_insert(self._table, data)  # type: ignore
        self._control()
        cursor = self._exec(query, data)
        rlastrowid = cursor.lastrowid
        if not self.in_transaction:
            self._sql.commit()
        else:
            self._dirty = True
        return rlastrowid

    def insert_multiple(self, datas: list[Data]):
        """Insert multiple values

        Args:
            datas (Iterable[Data]): Data to be inserted.
        """
        self._control()
        query, _ = build_insert(self._table, datas[0])  # type: ignore
        self._exec(query, datas, "executemany")
        if not self.in_transaction:
            self._sql.commit()
        else:
            self._dirty = True

    def insert_many(self, datas: list[Data]):
        """Alias to `insert_multiple`"""
        return self.insert_multiple(datas)

    def update(
        self,
        where: Condition | None = None,
        data: Data | None = None,
        limit: int = 0,
        order: Optional[Orders] = None,
    ):
        """Update rows of current table

        Args:
            data (Data): New data to update
            where (Condition, optional): Condition dictionary.
                See `Signature` about how condition works. Defaults to None.
            limit (int, optional): Limit updates. Defaults to 0.
            order (Optional[Orders], optional): Order of change. Defaults to None.

        Returns:
            int: Rows affected
        """
        if data is None:
            raise ValueError("data parameter must not be None")
        query, data = build_update(
            self._table, data, where, limit, order
        )  # type: ignore
        self._control()
        cursor = self._exec(query, data)
        rcount = cursor.rowcount
        if not self.in_transaction:
            self._sql.commit()
        else:
            self._dirty = True
        return rcount

    def update_one(
        self,
        where: Condition | None = None,
        data: Data | None = None,
        order: Orders | None = None,
    ) -> int:
        """Update 1 data only"""
        return self.update(where, data, 1, order)

    @overload
    def select(
        self,
        where: Condition = None,
        what: OnlyColumn = "*",
        limit: int = 0,
        offset: int = 0,
        order: Optional[Orders] = None,
        flatten: Literal[False] = False,
    ) -> list[Query]:
        pass

    @overload
    def select(
        self,
        where: Condition = None,
        what: OnlyColumn = "*",
        limit: int = 0,
        offset: int = 0,
        order: Optional[Orders] = None,
        flatten: Literal[True] = True,
    ) -> SquashedQueries:
        pass

    @overload
    def select(
        self,
        where: Condition = None,
        what: ParsedFn = _null,
        limit: int = 0,
        offset: int = 0,
        order: Optional[Orders] = None,
        flatten: Literal[False] = False,
    ) -> Any:
        pass

    @overload
    def select(
        self,
        where: Condition = None,
        what: JustAColumn = "_COLUMN",
        limit: int = 0,
        offset: int = 0,
        order: Optional[Orders] = None,
        flatten: Literal[False] = False,
    ) -> list[Any]:
        pass

    def select(
        self,  # pylint: disable=too-many-arguments
        where: Condition = None,
        what: OnlyColumn | ParsedFn | JustAColumn = "*",
        limit: int = 0,
        offset: int = 0,
        order: Optional[Orders] = None,
        flatten: bool = False,
    ):
        """Select data in current table. Bare .select() returns all data.

        Args:
            where (Condition, optional): Conditions to used. Defaults to None.
            what: (OnlyColumn, ParsedFn, optional): Select what you want. Default to None.
            limit (int, optional): Limit of select. Defaults to 0.
            offset (int, optional): Offset. Defaults to 0
            order (Optional[Orders], optional): Selection order. Defaults to None.
            flatten (bool): Flatten returned data into dict of lists. Defaults to False.

        Returns:
            Queries: Selected data
        """
        self._control()
        self._query_control()
        query, data = build_select(
            self._table, where, what, limit, offset, order
        )  # type: ignore
        just_a_column = (isinstance(what, tuple) and len(what) == 1) or (
            isinstance(what, str) and what != "*"
        )
        with self._sql:
            cursor = self._exec(query, data)
            data = cursor.fetchall()
            if just_a_column:
                return [d[what] for d in data]
            if flatten:
                return crunch(data)
            if isinstance(what, ParsedFn):
                return data[0][what.parse_sql()[0]]
            return data

    @overload
    def paginate_select(
        self,
        where: Condition = None,
        what: OnlyColumn = "*",
        page: int = 0,
        length: int = 10,
        order: Optional[Orders] = None,
        flatten: Literal[False] = False,
    ) -> Generator[list[Query], None, None]:
        pass

    @overload
    def paginate_select(
        self,
        where: Condition = None,
        what: JustAColumn = "_COLUMN",
        page: int = 0,
        length: int = 10,
        order: Optional[Orders] = None,
        flatten: Literal[False] = False,
    ) -> Generator[list[Any], None, None]:  # type: ignore
        pass

    @overload
    def paginate_select(
        self,
        where: Condition = None,
        what: OnlyColumn = "*",
        page: int = 0,
        length: int = 10,
        order: Optional[Orders] = None,
        flatten: Literal[True] = True,
    ) -> Generator[SquashedQueries, None, None]:
        pass

    def paginate_select(
        self,
        where: Condition = None,
        what: OnlyColumn | JustAColumn = "*",
        page: int = 0,
        length: int = 10,
        order: Optional[Orders] = None,
        flatten: bool = False,
    ):
        """Paginate select

        Args:
            where (Condition, optional): Confitions to use. Defaults to None.
            what (OnlyColumn, optional): Select what you want. Default to None.
            page (int): Which page number be returned first
            length (int, optional): Pagination length. Defaults to 10.
            order (Optional[Orders], optional): Order. Defaults to None.
            flatten (bool): Flatten returned data into dict of lists. Defaults to False.

        Yields:
            Generator[Queries, None, None]: Step-by-step paginated result.
        """

        if page < 0:
            page = 0
            order = "desc" if order in ("asc", None) else "asc"  # type: ignore
        self._control()
        self._query_control()
        start = page * length
        # ! A `only` keyword as a string or tuple of 1 element will
        # ! actually be a problem if they left alone because the end result is a list
        just_a_column = (isinstance(what, str) and what != "*") or (
            isinstance(what, tuple) and len(what) == 1
        )
        while True:
            query, data = build_select(
                self._table, where, what, length, start, order
            )  # type: ignore
            with self._sql:
                cursor = self._exec(query, data)
                fetched = cursor.fetchmany(length)
                if len(fetched) == 0:
                    return
                if flatten and not just_a_column:
                    fetched = crunch(fetched)
                if len(fetched) != length:
                    yield fetched
                    return
                yield fetched
                start += length

    @overload
    def select_one(
        self,
        where: Condition = None,
        what: ParsedFn = _null,
        order: Optional[Orders] = None,
    ) -> Any:
        pass

    @overload
    def select_one(
        self,
        where: Condition = None,
        what: OnlyColumn = "*",
        order: Optional[Orders] = None,
    ) -> Query:
        pass

    @overload
    def select_one(
        self,
        where: Condition = None,
        what: JustAColumn = "_COLUMN",
        order: Optional[Orders] = None,
    ) -> Any:
        pass

    def select_one(
        self,
        where: Condition = None,
        what: OnlyColumn | JustAColumn | ParsedFn = "*",
        order: Optional[Orders] = None,
    ):
        """Select one data

        Args:
            where (Condition, optional): Condition to use. Defaults to None.
            what: (OnlyColumn, optional): Select what you want. Default to None.
            order (Optional[Orders], optional): Order of selection. Defaults to None.

        Returns:
            Any: Selected data
        """
        self._control()
        self._query_control()
        query, data = build_select(
            self._table, where, what, 1, 0, order
        )  # type: ignore
        with self._sql:
            cursor = self._exec(query, data)
            returned = cursor.fetchone()
            if isinstance(what, ParsedFn):
                print(returned)
                return returned[what.parse_sql()[0]]
            if not returned:
                return Row()
            if isinstance(what, tuple) and len(what) == 1:
                return returned[what]
            if isinstance(what, str) and what != "*":
                return returned[what]
            return returned

    def columns(self):
        """Table columns"""
        if self._columns is None:
            raise AttributeError("columns are undefined.")

        return tuple(self._columns)

    def add_column(self, column: Column | BuilderColumn):
        """Add column to table"""
        sql = self._sql
        column = column.to_column() if isinstance(column, BuilderColumn) else column
        if column.primary or column.unique:
            raise OperationalError(
                "New column cannot have primary or unique constraint"
            )
        if column.nullable is False and column.default is None:
            raise OperationalError(
                "New column cannot be not null while default value is \
set to null"
            )
        if column.default is not None and column.foreign:
            raise OperationalError(
                "New column must accept null default value if foreign \
constraint is enabled."
            )
        query = f"alter table {self._table} add column {extract_single_column(column)}"
        if self._columns is not None:
            self._columns.append(column)
        sql.execute(query)

    def subquery(self, where: Condition, columns: OnlyColumn | str, limit: int = 0) -> SubQuery:
        """Push subquery to current .select() of other table"""
        return SubQuery(self, columns, where, limit)

    def rename_column(self, old_column: str, new_column: str):
        """Rename existing column to new column"""
        check_iter((old_column, new_column))
        query = f"alter table {self._table} rename column {old_column} to {new_column}"
        self._sql.execute(query)

    def commit(self):
        """Commit changes"""
        self._sql.commit()
        self._dirty = False

    def rollback(self):
        """Rollback"""
        self._sql.rollback()
        self._dirty = False

    def _begin_transaction(self):
        """Start a transaction or savepoint depending on depth."""
        stack = list(_tx_stack.get())  # copy since ContextVar values are immutable
        depth = len(stack)

        if depth == 0:
            self._sql.execute("BEGIN TRANSACTION")
        else:
            savepoint_name = f"sp_{depth}"
            self._sql.execute(f"SAVEPOINT {savepoint_name}")

        stack.append(True)
        _tx_stack.set(stack)

    def _commit_transaction(self):
        """Commit or release savepoint depending on depth."""
        stack = list(_tx_stack.get())
        depth = len(stack)

        if depth == 1:
            self._sql.commit()
        elif depth > 1:
            savepoint_name = f"sp_{depth-1}"
            self._sql.execute(f"RELEASE SAVEPOINT {savepoint_name}")
        stack.pop()
        _tx_stack.set(stack)

    def _rollback_transaction(self):
        """Rollback or rollback to savepoint depending on depth."""
        stack = list(_tx_stack.get())
        depth = len(stack)

        if depth == 1:
            self._sql.rollback()
        elif depth > 1:
            savepoint_name = f"sp_{depth-1}"
            self._sql.execute(f"ROLLBACK TO SAVEPOINT {savepoint_name}"
            )

        stack.pop()
        _tx_stack.set(stack)

    def count(self):
        """Count how much objects/rows stored in this table"""
        # ? Might as well uses __len__? But it's quite expensive.
        return self.select(what=count("*"))

    def __repr__(self) -> str:
        return f"<{type(self).__name__}({self._table}) -> {self._db!r}>"

__all__ = ["Table"]
