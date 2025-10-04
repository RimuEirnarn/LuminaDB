"""Utility"""

from typing import Optional, Any

from ..typings import Orders, Data
from ..functions import ParsedFn, _function_extract
from ..utils import check_one, check_iter, Null
from ..locals import _SQLITETYPES

from .typings import Condition, CacheOrders, CacheData


DEFAULT_MAPPINGS = {value: value for value in _SQLITETYPES}
SQL_ACTIONS = {"null": "set null"}
MAX_SUBQUERY_STACK_LIMIT = 10

NAMING_FORMAT = "{key}{suffix}__{call_id}_{depth}_{condition_id}"


def set_subquery_stack_limit(value: int):
    """Set subquery stack limit"""
    global MAX_SUBQUERY_STACK_LIMIT  # pylint: disable=global-statement
    if MAX_SUBQUERY_STACK_LIMIT <= 0:
        raise ValueError("Cannot set limit below 1")
    MAX_SUBQUERY_STACK_LIMIT = value


def select_onlyparam_parse(data: str | ParsedFn):
    """Select() parse `what` parameter"""
    if isinstance(data, str):
        return data
    x = _function_extract(data)
    if isinstance(x, tuple):
        return x[0]
    return x


def setup_hashable(
    condition: Condition, order: Optional[Orders] = None, data: Data | None = None
):
    """Setup hashable conditions, orders, and data"""
    cond = None
    order_ = None
    data_ = ()
    if isinstance(condition, dict):
        cond = tuple(condition.items())
    if isinstance(condition, list):
        cond = tuple(condition)

    if isinstance(order, tuple):
        order_ = order

    if data:
        data_ = tuple(data.keys())
    return cond, order_, data_


def setup_limit_patch(table_name: str, condition: str, limit):
    """Setup limit for patch/update"""
    check_one(table_name)
    if not isinstance(limit, int):
        limit = 1
    return f"where rowid in (select rowid from {table_name}\
{' '+condition if condition else ''} limit {limit})"


def parse_orders(order: CacheOrders):
    """Parse `orders` param"""
    if isinstance(order, tuple) and not isinstance(order[0], tuple):
        ord_, order_by = order
        # print('here')
        check_iter(order, ("asc", "desc"))  # type: ignore
        return f"{ord_} {order_by}"
    if isinstance(order, tuple) and isinstance(order[0], tuple):

        return ", ".join(
            f"{ord_} {order_by}"
            for ord_, order_by in order
            if check_iter((ord_, order_by), ("asc", "desc"))
        )
    raise TypeError("What?", type(order))


def remove_null(condition: dict[str, Any]) -> dict[str, Any]:
    """Remove Null from current Data"""
    new = condition.copy()
    for key, value in condition.items():
        if value is Null:
            del new[key]
    if not new:
        raise ValueError(
            "After removing Null sentinel value, new data that would be inserted"
            "/updated returns empty dictionary."
        )
    return new


def filter_extraction(string: str, shlexed: list[str]):
    """
    A function step of table extraction. Used to replace quoted and parens with parameter.

    Args:
        string (str): The input string containing the table creation SQL.
        shlexed (list[str]): The tokenized list of the input string.

    Returns:
        tuple[dict[str, str], dict[str, str], str]: A tuple containing:
            - A dictionary mapping placeholders to quoted strings.
            - A dictionary mapping placeholders to parenthesized strings.
            - The modified string with placeholders.
    """

    quoted_wrap = {}
    paren_wrap = {}
    new_string = string
    for index, shlex_string in enumerate(shlexed):
        # ? this attempt to replace all quoted string to a format-able stuff.
        if shlex_string.startswith("'") or shlex_string.startswith('"'):
            quoted_wrap[f":index{index}"] = shlex_string
            new_string = new_string.replace(shlex_string, f":index{index}")

    while (index := new_string.find("(")) != -1:
        # ? this attempt to replace all brackets with format-able stuff
        last = new_string.find(")") + 1
        selected = new_string[index:last]
        new_string = new_string.replace(selected, f":wrap{len(paren_wrap)}")
        paren_wrap[f":wrap{len(paren_wrap)}"] = selected
    return quoted_wrap, paren_wrap, new_string


def format_paramable(data: dict[str, Any] | tuple[str, ...]):
    """Format a data to parameterized data."""
    that: dict[str, str] = {}
    if isinstance(data, dict):
        for key in data:
            check_one(key)
            that[key] = f":{key}"
    else:
        for key in data:
            check_one(key)
            that[key] = f":{key}"
    return that


def combine_keyvals(keydict: dict[str, Any], valuedict: dict[str, Any] | CacheData):
    """Combine key dictionary with value dictionary. The first dictionary will only
    ignore the values while value dict ignore the keys.
    Mapping[key, _] -> keydict
    Mapping[_, value] -> valuedict"""
    if len(keydict) != len(valuedict):
        raise IndexError("One dictionary is larger. It must be equal.")
    new: dict[str, Any] = {}
    if isinstance(valuedict, dict):
        for key0, key1 in zip(keydict, valuedict):
            new[key0] = valuedict[key1]
    else:
        for key0, val1 in zip(keydict, valuedict):
            new[key0] = val1
    return new
