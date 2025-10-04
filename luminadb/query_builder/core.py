"""Core builder"""

from typing import Any, Optional, Literal

from .typings import Condition
from .engine import QueryParams, _build_delete, _build_insert, _build_select, _build_update
from .utils import (
    setup_hashable,
    remove_null,
    combine_keyvals,
)

from ..typings import Orders, Data
from ..functions import ParsedFn # type: ignore

def build_select(  # pylint: disable=too-many-arguments
    table_name: str,
    condition: Condition = None,
    only: tuple[str, ...] | ParsedFn | str | Literal["*"] = "*",
    limit: int = 0,
    offset: int = 0,
    order: Optional[Orders] = None,
) -> tuple[str, dict[str, Any]]:
    """Build select query (this function (backendly) cache!)

    Args:
        table_name (str): Table name
        condition (Condition, optional): Condition to use. Defaults to None.
        only: (OnlyColumn, optional): Select what you want. Default to None.
        limit (int, optional): Limit query (this also limits DB-API 2 `.fetchall`). Defaults to 0.
        offset (int, optional): Offset. Defaults to 0.
        order (Optional[Orders], optional): Order. Defaults to None.

    Returns:
        tuple[str, dict[str, Any]]: query and query data
    """
    cond, order_, _ = setup_hashable(condition, order)
    params = QueryParams(
        table_name=table_name,
        condition=cond,
        only=only,
        limit=limit,
        offset=offset,
        order=order_,  # type: ignore
    )
    return _build_select(params)

def build_update(
    table_name: str,
    new_data: Data,
    condition: Condition = None,
    limit: int = 0,
    order: Optional[Orders] = None,
) -> tuple[str, dict[str, Any]]:
    """Build update query (once again, this function backendly cache)

    Args:
        table_name (str): Table name
        new_data (Data): New data to update
        condition (Condition, optional): Condition to limit what to update. Defaults to None.
        limit (int, optional): limit chanes. Defaults to 0.
        order (Optional[Orders], optional): What order of change?. Defaults to None.

    Returns:
        tuple[str, dict[str, Any]]: query, query data
    """
    new_data = remove_null(new_data)
    cond, order_, ndata = setup_hashable(condition, order, new_data)
    params = QueryParams(
        table_name=table_name,
        condition=cond,
        limit=limit,
        order=order_,  # type: ignore
        data=ndata,
    )
    query, check, updated = _build_update(params)
    return query, check | combine_keyvals(updated, new_data)


def build_delete(
    table_name: str,
    condition: Condition = None,
    limit: int = 0,
    order: Optional[Orders] = None,
) -> tuple[str, dict[str, Any]]:
    """Build delete query

    Args:
        table_name (str): Table name
        condition (Condition, optional): Condition to limit deletion. Defaults to None.
        limit (int, optional): Limit to limit deletion. Defaults to 0.
        order (Optional[Orders], optional): Order. Defaults to None.

    Returns:
        tuple[str, dict[str, Any]]: query, query data
    """

    cond, order_, _ = setup_hashable(condition, order)
    params = QueryParams(
        table_name, condition=cond, limit=limit, order=order_  # type: ignore
    )
    return _build_delete(params)


def build_insert(table_name: str, data: Data) -> tuple[str, dict[str, Any]]:
    """Build insert query

    Args:
        table_name (str): table name
        data (Data): Data to insert

    Returns:
        tuple[str, dict[str, Any]]: query, query data
    """
    data = remove_null(data)
    _, _, ndata = setup_hashable(None, None, data)
    query, _ = _build_insert(table_name, ndata)
    return query, data
