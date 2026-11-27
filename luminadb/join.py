"""Table API joins (read-only)."""

# pylint: disable=protected-access,redefined-builtin,too-many-arguments

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Optional

from .query_builder.join import (
    JoinSpec,
    build_join_select,
    nest_row,
    normalize_join_type,
    normalize_on,
)
from .query_builder.typings import Condition
from .utils import Row, check_ident

if TYPE_CHECKING:
    from .table import Table


class TableRef:
    """A table together with the alias it is known by inside one join query.

    Get one with `Table.alias("u")`; the alias is also the key of that table in
    the nested result rows."""

    def __init__(self, table: Table, alias: Optional[str] = None) -> None:
        self._table = table
        self._alias = check_ident(alias if alias is not None else table.name)

    @property
    def table(self) -> Table:
        """The underlying table"""
        return self._table

    @property
    def alias(self) -> str:
        """Alias (the table name when none was given)"""
        return self._alias

    def alias_as(self, alias: str) -> TableRef:
        """Same table under another alias"""
        return TableRef(self._table, alias)

    def join(
        self, other: Table | TableRef, on: Any = None, type: str = "inner",
        alias: Optional[str] = None
    ) -> JoinQuery:
        """Start a join query with this table on the left."""
        return JoinQuery(self).join(other, on=on, type=type, alias=alias)


class JoinQuery:
    """A read-only join of two or more tables. Immutable: `.join()` returns a new query.

    Rows come back nested, keyed by table name or alias:
    `{"users": {"id": 1}, "groups": {"id": 1}}`. The unmatched side of an outer
    join is `None`.

    Note:
        Outer-joined sides are detected through `rowid`, so tables created
        `WITHOUT ROWID` cannot be on the nullable side of an outer join.
    """

    def __init__(
        self,
        base: TableRef,
        joins: tuple[JoinSpec, ...] = (),
        refs: tuple[TableRef, ...] = (),
    ) -> None:
        self._base = base
        self._joins = joins
        self._refs = refs or (base,)

    @property
    def aliases(self) -> tuple[str, ...]:
        """Aliases of all tables in this query, in join order"""
        return tuple(ref.alias for ref in self._refs)

    def join(
        self, other: Table | TableRef, on: Any = None, type: str = "inner",
        alias: Optional[str] = None
    ) -> JoinQuery:
        """Join another table.

        Args:
            other: Table or `table.alias("x")`.
            on: `(left_col, right_col)` or a list of those, ANDed. A bare left
                column is a column of the base table of this query, a bare right
                column is a column of `other`. Use `alias.column` to be explicit.
                Required unless `type="cross"`.
            type: inner, left, right, full or cross. right/full need SQLite 3.39+.
            alias: Alias for `other`.
        """
        ref = other if isinstance(other, TableRef) else TableRef(other)
        if alias is not None:
            ref = ref.alias_as(alias)
        if ref.table._sql is not self._base.table._sql:
            raise ValueError("Cannot join tables from different databases")
        if ref.alias in self.aliases:
            raise ValueError(
                f"{ref.alias!r} is already used in this join; use table.alias(...) "
                "to give the second occurrence another name"
            )
        kind = normalize_join_type(type)
        spec = JoinSpec(
            kind,
            ref.table.name,
            ref.alias,
            normalize_on(on, kind, self._base.alias, ref.alias, self.aliases),
        )
        return JoinQuery(self._base, self._joins + (spec,), self._refs + (ref,))

    def _run(self, plan) -> list[Row]:
        for ref in self._refs:
            ref.table._control()
            ref.table._query_control()
        cursor = self._base.table._exec(plan.query, plan.data)
        cursor.row_factory = None  # positional tuples: duplicate names can't collapse
        return [nest_row(values, plan) for values in cursor.fetchall()]

    def _plan(self, what, where, limit, offset, order):
        return build_join_select(
            (self._base.table.name, self._base.alias),
            self._joins,
            what,
            where,
            limit,
            offset,
            order,
        )

    def select(
        self,
        *,
        what: Any,
        where: Condition = None,
        limit: int = 0,
        offset: int = 0,
        order: Any = None,
    ) -> list[Row]:
        """Select rows.

        Args:
            what: Required. `{alias: columns}`, columns being names and/or
                `as_("col", "alias")`. `"*"` is not supported (it would need a
                metadata query).
            where: Conditions; keys must be `alias.column`.
            limit (int, optional): Limit. Defaults to 0 (no limit).
            offset (int, optional): Offset. Defaults to 0.
            order (optional): `("alias.col", "asc")` or a tuple of those.
        """
        return self._run(self._plan(what, where, limit, offset, order))

    def select_one(self, *, what: Any, where: Condition = None, order: Any = None):
        """Select the first row; an empty Row when nothing matches."""
        rows = self._run(self._plan(what, where, 1, 0, order))
        return rows[0] if rows else Row()
