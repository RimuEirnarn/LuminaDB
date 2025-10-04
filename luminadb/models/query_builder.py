"""Model QueryBuilder"""

# pylint: disable=protected-access

from __future__ import annotations
from typing import Any, Generic, Type, TypeVar
from .errors import NoDataReturnedError
from ..column import check_one
from ..functions import Function
from .. import models  # pylint: disable=unused-import

T = TypeVar("T", bound="models.BaseModel")
count = Function("COUNT")

MGR_REQ = "Note: The database record has serious mismatch, consider doing migration."

def add_excnote(exc: Exception):
    """Add exception notes when an entry retrieved is different"""
    msg = str(exc)
    if "() missing" in msg or "() got" in msg:
        exc.add_note(MGR_REQ)
    # if "() got" in msg:
        # exc.add_note(MGR_REQ)
class QueryBuilder(Generic[T]):
    # pylint: disable=protected-access
    """Query builder for Model ORM"""

    def __init__(self, model: Type[T]) -> None:
        self._model = model
        self._filters: dict[str, Any] = {}
        self._limit = 0
        self._offset = 0
        self._order = None
        self._failing = False

    def throw(self):
        """Set when fetch() returns nothing, will raise an error"""
        self._failing = True
        return self

    def where(self, **kwargs: Any):
        """Sets conditioning"""
        self._filters.update(kwargs)
        return self

    def limit(self, value: int):
        """Sets limit"""
        self._limit = value
        return self

    def offset(self, value: int):
        """Sets offset"""
        self._offset = value
        return self

    def order_by(self, column: str, descending: bool = False):
        """Order the query by a column"""
        self._order = (check_one(column), "asc" if descending is False else "desc")
        return self

    def fetch(self) -> list[T]:
        """Fetch data from table"""
        records = self._model._tbl.select(  # pylint: disable=protected-access
            self._filters, limit=self._limit, offset=self._offset, order=self._order
        )

        if len(records) == 0 and self._failing:
            raise NoDataReturnedError(
                f"Model {self._model.__name__} has no data for current scope"
            )
        try:
            return [self._model(**record) for record in records]
        except TypeError as exc:
            add_excnote(exc)
            raise exc

    def fetch_one(self) -> T | None:
        """Fetch one data from table"""
        # pylint: disable=protected-access
        record = self._model._tbl.select_one(self._filters, order=self._order)
        if not record and self._failing:
            raise NoDataReturnedError(
                f"Model {self._model.__name__} has no data for current scope"
            )
        if not record:
            return None
        try:
            return self._model(**record)
        except TypeError as exc:
            add_excnote(exc)
            raise exc

    def patch(self, **kwargs: Any):
        """Update a data based on the filter according to passed keyword args"""
        affected_rows = self._model._tbl.update(
            self._filters, kwargs, self._limit, self._order
        )

        if affected_rows == 0 and self._failing:
            raise NoDataReturnedError(
                f"Model {self._model.__name__} updates no data for current scope"
            )
        return affected_rows

    def delete(self):
        """Delete data based on the filter"""
        affected_rows = self._model._tbl.delete(self._filters, self._limit, self._order)

        if affected_rows == 0 and self._failing:
            raise NoDataReturnedError(
                f"Model {self._model.__name__} delete exactly no rows"
            )

        return affected_rows

    def count(self) -> int:
        """Count how much data is within this operation"""
        return self._model._tbl.select(self._filters, what=count("*"))

    update = patch
