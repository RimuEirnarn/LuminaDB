"""Subexpression"""

from __future__ import annotations
from typing import TYPE_CHECKING


if TYPE_CHECKING:
    from .table import Table
    from .query_builder.typings import Condition, OnlyColumn
    from .typings import Orders


class SubQuery:
    """Allows subquery usage. Only select at this time"""

    def __init__(
        self,
        table: str | Table,
        cols: OnlyColumn,
        where: Condition,
        limit: int = 0,
        order: Orders | None = None,
    ) -> None:
        self._table = table if isinstance(table, str) else table.name
        self._cols = cols
        self._where = (
            tuple(where.items() if isinstance(where, dict) else where)
            if where is not None
            else ()
        )
        self._limit = limit
        self._order = order

    def __hash__(self):
        return hash((self._table, self._cols, self._where, self._limit))

    def get_params(self):
        """Get parameters/where data"""
        return tuple((value for key, value in self._where))

    @property
    def table(self):
        """Return target table name"""
        return self._table

    @property
    def cols(self):
        """Return columns associated"""
        return self._cols

    @property
    def where(self):
        """Return all conditions"""
        return self._where

    @property
    def limit(self):
        """Return limit query"""
        return self._limit

    @property
    def orders(self):
        """Return orders"""
        return self._order
