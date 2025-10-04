"""Typing Extensions"""

# pylint: disable=unnecessary-ellipsis
from typing import Any, Literal, TypedDict, TypeAlias, TypeVar
from .utils import Queries, SquashedQueries, Query


T = TypeVar("T")
Order: TypeAlias = tuple[str, Literal['asc'] | Literal['desc']]
Orders: TypeAlias = tuple[Order, ...] | Order
Data: TypeAlias = dict[str, Any]
OnlyColumn: TypeAlias = tuple[str, ...] | Literal["*"]
JustAColumn: TypeAlias = str | tuple[str] # pylint: disable=invalid-name
tuple_list: TypeAlias = list[T] | tuple[T, ...] # pylint: disable=invalid-name
null = object()


class MasterQuery(TypedDict):
    """Master Query"""

    type: str
    name: str
    tbl_name: str
    rootpage: int
    sql: str

__all__ = (
    "Order",
    "Orders",
    "Data",
    'OnlyColumn',
    "JustAColumn",
    "tuple_list",
    "Queries",
    "SquashedQueries",
    "Query",
    "null"
)
