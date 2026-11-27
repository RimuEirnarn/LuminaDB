"""Join query building.

Joins are read-only and always a single SQL statement. Every selected column is
addressed *positionally*, so two tables that both have an `id` column can never
overwrite each other. `nest_row()` then rebuilds `{table_or_alias: {column: value}}`.
"""

from collections.abc import Mapping
from dataclasses import dataclass
from sqlite3 import sqlite_version_info
from typing import Any, Iterable, Optional

from ..errors import SecurityError
from ..operators import Alias
from ..utils import Row, check_ident, check_qualified
from .engine import extract_signature
from .typings import Condition

JOIN_TYPES = {
    "inner": "inner join",
    "left": "left join",
    "right": "right join",
    "full": "full join",
    "cross": "cross join",
}
# RIGHT and FULL OUTER JOIN were added in SQLite 3.39.0
NEEDS_SQLITE_3_39 = ("right", "full")
# which previously joined slices become NULL-extended when this kind runs
_NULLABLE_NEW = ("left", "full")
_NULLABLE_OLD = ("right", "full")

Pair = tuple[str, str]


@dataclass(frozen=True)
class JoinSpec:
    """One `<kind> join <table> as <alias> on ...` clause. `on` is fully qualified."""

    kind: str
    table: str
    alias: str
    on: tuple[Pair, ...]


@dataclass(frozen=True)
class JoinPlan:
    """A built join select."""

    query: str
    data: dict[str, Any]
    # one entry per output column: (slice alias, key). key is None for the hidden
    # presence marker of a slice that may be NULL-extended by an outer join.
    layout: tuple[tuple[str, Optional[str]], ...]
    slices: tuple[str, ...]


def normalize_join_type(kind: str) -> str:
    """Whitelist a join type. Anything else is refused, never interpolated."""
    if not isinstance(kind, str) or kind.lower() not in JOIN_TYPES:
        raise SecurityError(
            f"Unknown join type {kind!r}; expected one of {', '.join(JOIN_TYPES)}"
        )
    kind = kind.lower()
    if kind in NEEDS_SQLITE_3_39 and sqlite_version_info < (3, 39, 0):
        raise NotImplementedError(
            f"{kind.upper()} JOIN needs SQLite 3.39+, "
            f"found {'.'.join(map(str, sqlite_version_info))}"
        )
    return kind


def normalize_on(
    on: Any,
    kind: str,
    base_alias: str,
    new_alias: str,
    known: Iterable[str],
) -> tuple[Pair, ...]:
    """Turn the user's `on` into qualified (left, right) pairs.

    `on` is `(left_col, right_col)` or a list of those (ANDed). A bare left column
    belongs to the base table of the query, a bare right column to the table being
    joined; `alias.column` is always taken literally.
    """
    if on is None:
        if kind != "cross":
            raise ValueError(
                "`on` is required unless type='cross' (an unconditioned join is a "
                "cartesian product)"
            )
        return ()
    if kind == "cross":
        raise ValueError("a cross join takes no `on` condition")
    if isinstance(on, tuple) and len(on) == 2 and all(isinstance(i, str) for i in on):
        on = [on]
    if not isinstance(on, list) or not on:
        raise TypeError("`on` must be (left, right) or a non-empty list of those")

    scope = frozenset((*known, new_alias))
    pairs: list[Pair] = []
    for item in on:
        if not (isinstance(item, tuple) and len(item) == 2):
            raise TypeError("each `on` entry must be a (left, right) tuple")
        left, right = item
        left = (
            check_qualified(left, scope)
            if "." in left
            else f"{base_alias}.{check_ident(left)}"
        )
        right = (
            check_qualified(right, scope)
            if "." in right
            else f"{new_alias}.{check_ident(right)}"
        )
        pairs.append((left, right))
    return tuple(pairs)


def _normalize_what(what: Any, known: set[str]):
    """Validate `what` -> ordered {slice alias: ((column, out_name), ...)}."""
    if not isinstance(what, Mapping):
        raise ValueError(
            "join `what` must be a mapping of table/alias -> columns, "
            'e.g. {"users": ("id", "name")}; "*" is not supported in joins'
        )
    if not what:
        raise ValueError("join `what` is empty")
    result: dict[str, tuple[Pair, ...]] = {}
    for alias, columns in what.items():
        if alias not in known:
            raise ValueError(f"{alias!r} is not a table/alias in this join")
        if isinstance(columns, (str, Alias)):  # Alias is a tuple: test it first
            columns = (columns,)
        if not isinstance(columns, (tuple, list)) or not columns:
            raise ValueError(f"no columns selected for {alias!r}")
        picked: list[Pair] = []
        for column in columns:
            if column == "*":
                raise ValueError('"*" is not supported in joins; list the columns')
            if isinstance(column, Alias):
                picked.append((check_ident(column.column), check_ident(column.alias)))
            else:
                picked.append((check_ident(column), column))
        outs = [out for _, out in picked]
        if len(set(outs)) != len(outs):
            raise ValueError(f"duplicate output column names for {alias!r}: {outs}")
        result[alias] = tuple(picked)
    return result


def _nullable_slices(base_alias: str, joins: tuple[JoinSpec, ...]) -> set[str]:
    nullable: set[str] = set()
    seen = [base_alias]
    for join in joins:
        if join.kind in _NULLABLE_OLD:
            nullable.update(seen)
        if join.kind in _NULLABLE_NEW:
            nullable.add(join.alias)
        seen.append(join.alias)
    return nullable


def _parse_order(order: Any, known: set[str]) -> str:
    if order is None:
        return ""
    if isinstance(order, tuple) and len(order) == 2 and isinstance(order[0], str):
        order = (order,)
    parts = []
    for column, direction in order:
        check_qualified(column, known)
        direction = str(direction).lower()
        if direction not in ("asc", "desc"):
            raise SecurityError(f"Order direction must be asc/desc, got {direction!r}")
        parts.append(f"{column} {direction}")
    return " order by " + ", ".join(parts)


def build_join_select(  # pylint: disable=too-many-arguments,too-many-locals
    base: tuple[str, str],
    joins: tuple[JoinSpec, ...],
    what: Any,
    condition: Condition = None,
    limit: int = 0,
    offset: int = 0,
    order: Any = None,
) -> JoinPlan:
    """Build the SQL for `base` joined with `joins`.

    Args:
        base: (table name, alias) of the left-most table.
        joins: Join clauses in order.
        what: {alias: columns}; required, "*" is refused (see `_normalize_what`).
        condition: Where conditions; keys must be `alias.column`.
    """
    if not all(isinstance(i, int) and not isinstance(i, bool) for i in (limit, offset)):
        raise TypeError("Expected limit/offset to be integer")
    base_table, base_alias = check_ident(base[0]), check_ident(base[1])
    known = {base_alias, *(j.alias for j in joins)}
    picked = _normalize_what(what, known)
    nullable = _nullable_slices(base_alias, joins)

    columns: list[str] = []
    layout: list[tuple[str, Optional[str]]] = []
    for alias, cols in picked.items():
        for column, out in cols:
            columns.append(
                f"{alias}.{column}" if column == out else f"{alias}.{column} as {out}"
            )
            layout.append((alias, out))
        if alias in nullable:
            # rowid of a NULL-extended row is NULL; real rows always have one
            columns.append(f"{alias}.rowid")
            layout.append((alias, None))

    def table_ref(table: str, alias: str) -> str:
        table = check_ident(table)
        return table if table == alias else f"{table} as {alias}"

    query = f"select {', '.join(columns)} from {table_ref(base_table, base_alias)}"
    for join in joins:
        query += f" {JOIN_TYPES[join.kind]} {table_ref(join.table, join.alias)}"
        if join.on:
            query += " on " + " and ".join(f"{left} = {right}" for left, right in join.on)

    where, data = extract_signature(condition, known=frozenset(known))
    if where:
        query += f" {where}"
    query += _parse_order(order, known)
    if limit:
        query += f" limit {limit}"
    if offset:
        # SQLite rejects OFFSET without LIMIT; -1 means "no limit"
        query += f"{'' if limit else ' limit -1'} offset {offset}"

    return JoinPlan(query, data, tuple(layout), tuple(picked))


def nest_row(values: tuple, plan: JoinPlan) -> Row:
    """Rebuild one positional row as `{alias: Row | None}`."""
    out = {alias: Row() for alias in plan.slices}
    absent: set[str] = set()
    for value, (alias, key) in zip(values, plan.layout):
        if key is None:
            if value is None:
                absent.add(alias)
        else:
            out[alias][key] = value
    for alias in absent:
        out[alias] = None  # type: ignore
    return Row(out)
