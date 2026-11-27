"""Model API joins.

A Model is a typed dataclass, so a related instance is never stored *inside* one.
A join returns plain tuples instead, `(Base, Other)`, where each side is built from
its own slice of the nested row:

    Posts.join(Users).fetch()                       # [(Post, User), ...]
    Users.join(Posts, type="left").fetch()          # [(User, Post | None), ...]
    Users.join(Posts, type="left").group()          # [(User, [Post, ...]), ...]
"""

# pylint: disable=protected-access,duplicate-code

from __future__ import annotations

from dataclasses import fields
from typing import TYPE_CHECKING, Any, Generic, Optional, Type, TypeVar

from .helpers import Foreign
from .query_builder import add_excnote
from ..query_builder.join import normalize_join_type
from ..utils import check_qualified

if TYPE_CHECKING:
    from . import BaseModel

A = TypeVar("A", bound="BaseModel")
B = TypeVar("B", bound="BaseModel")


def _foreign_pairs(model: Type[BaseModel], target: Type[BaseModel]):
    """`(column of model, referenced column of target)` for each Foreign() of
    `model` that points at `target`'s table."""
    pairs = []
    for constraint in model.__schema__:
        if isinstance(constraint, Foreign):
            table, column = str(constraint.target).split("/", 1)
            if table.lower() == target.__table_name__.lower():
                pairs.append((constraint.column, column))
    return pairs


def infer_on(base: Type[BaseModel], other: Type[BaseModel]) -> tuple[str, str]:
    """Find the `on` of a join from Foreign() constraints, in either direction.

    Returns `(base column, other column)`. Raises ValueError when there is none or
    when it is ambiguous, in which case pass `on=` explicitly."""
    forward = _foreign_pairs(base, other)  # base.fk -> other.pk
    backward = [(ref, fk) for fk, ref in _foreign_pairs(other, base)]  # other.fk -> base.pk
    found = forward + backward
    if not found:
        raise ValueError(
            f"No Foreign() constraint links {base.__name__} and {other.__name__}; "
            "pass on=(base_column, other_column)"
        )
    if len(found) > 1:
        raise ValueError(
            f"{base.__name__} and {other.__name__} are linked by more than one "
            "Foreign() constraint; pass on=(base_column, other_column)"
        )
    return found[0]


class ModelJoinQuery(Generic[A, B]):  # pylint: disable=too-many-instance-attributes
    """A read-only join of two Models. Build it with `Model.join(Other, ...)`."""

    def __init__(
        self,
        base: Type[A],
        other: Type[B],
        on: Any = None,
        type: str = "inner",  # pylint: disable=redefined-builtin
    ) -> None:
        self._base = base
        self._other = other
        self._kind = normalize_join_type(type)
        if on is None and self._kind != "cross":
            on = infer_on(base, other)
        self._join = base._tbl.join(other._tbl, on=on, type=self._kind)
        self._base_alias = base._tbl.name
        self._other_alias = other._tbl.name
        self._filters: dict[str, Any] = {}
        self._order: list[tuple[str, str]] = []
        self._limit = 0
        self._offset = 0

    # --- building -------------------------------------------------------------

    def _qualify(self, column: str) -> str:
        """A bare column means a column of the base model."""
        if "." in column:
            return check_qualified(column)
        return f"{self._base_alias}.{check_qualified(column)}"

    def where(self, **kwargs: Any):
        """Conditions. A bare key is a column of the base model; use
        `where(**{"posts.title": "x"})` for the other one."""
        for key, value in kwargs.items():
            self._filters[self._qualify(key)] = value
        return self

    def order_by(self, column: str, descending: bool = False):
        """Order by a column (bare = base model). Calls accumulate."""
        self._order.append((self._qualify(column), "desc" if descending else "asc"))
        return self

    def limit(self, value: int):
        """Limit rows (not parents, when used with `group()`)"""
        self._limit = value
        return self

    def offset(self, value: int):
        """Offset rows"""
        self._offset = value
        return self

    # --- running --------------------------------------------------------------

    def _what(self):
        return {
            self._base_alias: tuple(f.name for f in fields(self._base)), # type: ignore
            self._other_alias: tuple(f.name for f in fields(self._other)), # type: ignore
        }

    @staticmethod
    def _build(model: Type[Any], data: Optional[dict[str, Any]]):
        if data is None:  # the unmatched side of an outer join
            return None
        try:
            return model(**data)
        except TypeError as exc:
            add_excnote(exc)
            raise

    def _pair(self, row) -> tuple[Optional[A], Optional[B]]:
        return (
            self._build(self._base, row[self._base_alias]),
            self._build(self._other, row[self._other_alias]),
        )

    def fetch(self) -> list[tuple[Optional[A], Optional[B]]]:
        """Fetch `(Base, Other)` pairs. The side that an outer join left unmatched
        is `None`."""
        rows = self._join.select(
            what=self._what(),
            where=self._filters or None,
            limit=self._limit,
            offset=self._offset,
            order=tuple(self._order) or None,
        )
        return [self._pair(row) for row in rows]

    def fetch_one(self) -> Optional[tuple[Optional[A], Optional[B]]]:
        """Fetch the first pair, or None."""
        row = self._join.select_one(
            what=self._what(),
            where=self._filters or None,
            order=tuple(self._order) or None,
        )
        return self._pair(row) if row else None

    def group(self) -> list[tuple[A, list[B]]]:
        """One-to-many view: `[(Base, [Other, ...]), ...]`.

        Groups by the primary key of the base model, in order of first appearance.
        Bases with no match (left join) get an empty list. Only available for
        inner/left joins, where the base side always exists.
        """
        if self._kind not in ("inner", "left"):
            raise ValueError("group() needs an inner or left join")
        primary = self._base._primary
        if not primary:
            raise ValueError(f"{self._base.__name__} has no primary key to group by")
        groups: dict[Any, tuple[A, list[B]]] = {}
        for parent, child in self.fetch():
            key = getattr(parent, primary)
            if key not in groups:
                groups[key] = (parent, [])  # type: ignore
            if child is not None:
                groups[key][1].append(child)  # type: ignore
        return list(groups.values())
