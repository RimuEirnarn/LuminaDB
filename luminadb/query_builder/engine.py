"""Generic query building engine"""

from typing import Optional, Any
from dataclasses import dataclass
from functools import lru_cache

from .typings import CacheCond, OnlyColumn, CacheOrders, CacheData, Condition, SubQuery
from .utils import (
    parse_orders,
    format_paramable,
    setup_limit_patch,
    MAX_SUBQUERY_STACK_LIMIT,
    NAMING_FORMAT,
)

from ..errors import SecurityError
from ..functions import ParsedFn
from ..signature import Signature
from ..utils import check_one, check_iter, null
from ..utils import generate_ids


@dataclass(frozen=True)
class QueryParams:
    """Encapsulates parameters for building SQL queries."""

    table_name: str
    condition: Optional[CacheCond] = None
    only: Optional[OnlyColumn] = None
    limit: int = 0
    offset: int = 0
    order: Optional[CacheOrders] = None
    data: Optional[CacheData] = None

    def __post_init__(self):
        if not all(
            (isinstance(getattr(self, item), int) for item in ("limit", "offset"))
        ):
            raise TypeError("Expected limit/offset to be integer")

    def __hash__(self):
        """Custom hash function to ensure compatibility with lru_cache."""
        return hash(
            (
                self.table_name,
                self.condition,
                self.only,
                self.limit,
                self.offset,
                self.order,
                self.data,
            )
        )


@lru_cache
def _build_select(query_params: QueryParams, depth: int = 0):
    if depth < 0 or depth >= MAX_SUBQUERY_STACK_LIMIT:
        raise RecursionError(
            "Subquery builder has reached recursion limit of"
            f"{MAX_SUBQUERY_STACK_LIMIT}"
        )
    check_one(query_params.table_name)
    cond, data = extract_signature(query_params.condition, depth=depth)
    what_ = "*"
    if query_params.only and isinstance(query_params.only, ParsedFn):
        what_, databin = query_params.only.parse_sql()
        check_iter(
            (query_params.only.name, *(a for a in query_params.only.values if a != "*"))
        )
        data.update(databin)
    elif isinstance(query_params.only, tuple):
        generator = (
            column_name for column_name in query_params.only if check_one(column_name)
        )
        what_ = f"{', '.join(generator)}"  # type: ignore
    elif query_params.only != "*" and isinstance(query_params.only, str):
        what_ = check_one(query_params.only)  # type: ignore

    query = f"select {what_} from {query_params.table_name}"
    if cond:
        query += f" {cond}"
    if query_params.order and isinstance(query_params.order, tuple):
        query += f" order by {parse_orders(query_params.order)}"
    if query_params.limit:
        query += f" limit {query_params.limit}"
    if query_params.offset:
        query += f" offset {query_params.offset}"

    return query, data


@lru_cache
def _build_update(query_params: QueryParams):
    check_one(query_params.table_name)
    cond, data = extract_signature(query_params.condition)
    new_str, updated = build_update_data(query_params.data)  # type: ignore
    query = f"update {query_params.table_name} set {new_str} {cond}"
    if query_params.limit:
        query = query.replace(cond, "")
        query += setup_limit_patch(query_params.table_name, cond, query_params.limit)
    if query_params.order:
        query += f" order by {parse_orders(query_params.order)}"
    # ? Require manual intervention to make sure updated is sync as
    # print(query)
    return query, data, updated
    # ? ... combine_keyvals(updated, NEW DATA)
    # ? our cache data only contain keys not values (v0.3.0)


@lru_cache
def _build_delete(query_params: QueryParams):
    check_one(query_params.table_name)
    cond, data = extract_signature(query_params.condition)
    query = f"delete from {query_params.table_name} {cond}"
    if query_params.limit:
        query = query.replace(cond, "")
        query += setup_limit_patch(query_params.table_name, cond, query_params.limit)
    if query_params.order:
        query += f" order by {parse_orders(query_params.order)}"
    return query, data


@lru_cache
def _build_insert(table_name: str, data: CacheData):
    check_one(table_name)
    converged = format_paramable(data)
    query = f"insert into {table_name} ({', '.join(val for val in converged)}) \
values ({', '.join(val for val in converged.values())})"
    return query, data


def build_update_data(data: dict[str, Any] | CacheData, suffix: str = "_set"):
    """Build update data, used to parameterized update data.
    Suffix is used to make sure there's no collisions with others. Use this with caution.
    """
    string = ""
    that: dict[str, str] = {}
    for key in data:
        check_one(key)
        string += f"{key}=:{key}{suffix}, "
        that[f"{key}{suffix}"] = f":{key}{suffix}"
    return string[:-2], that


def _handle_in(key, middle, val, condition_id):
    vals = tuple(
        f":prop_{condition_id}_val_in{index}" for index, _ in enumerate(val.data)
    )
    clause = f" {key} {middle} ({', '.join(vals)})"
    data = {key0[1:]: val0 for key0, val0 in zip(vals, val.data)}
    return clause, data


def _handle_between(key, middle, val):
    vdata = val.data
    if not all(isinstance(x, (int, float)) for x in vdata):
        raise SecurityError("Values for between constraint is not int/float")
    clause = f" {key} {middle} {vdata[0]!r} and {vdata[1]!r}"
    return clause


def _handle_like(key, middle, val):
    vdata = val.data
    clause = f" {key} {middle} {vdata!r}"
    return clause


def extract_signature(  # pylint: disable=too-many-locals
    filter_: Condition | CacheCond = None, suffix: str = "_check", depth: int = 0
):
    """Extract filter signature."""
    if depth < 0 or depth >= MAX_SUBQUERY_STACK_LIMIT:
        raise RecursionError(
            "Subquery builder has reached recursion limit of"
            f"{MAX_SUBQUERY_STACK_LIMIT}"
        )
    if filter_ is None:
        return "", {}

    if isinstance(filter_, (list, tuple)):
        filter_ = dict(filter_)

    call_id = generate_ids()
    clauses = []
    data: dict[str, Any] = {}

    for key, value in filter_.items():
        check_one(key)
        condition_id = generate_ids()
        if not isinstance(value, Signature):
            value = Signature(value, "=")
        old_data = value.value

        val = (
            Signature(
                ":"
                + NAMING_FORMAT.format(
                    key=key,
                    suffix=suffix,
                    call_id=call_id,
                    depth=depth,
                    condition_id=condition_id,
                ),
                value.generate(),
                value.data,
            )
            if value.value is not null
            else value
        )

        if isinstance(value.value, SubQuery):
            clause, subq_data = handle_subquery(key, value, depth)
            clauses.append(clause)
            data.update(subq_data)
            continue

        middle = val.generate()
        if val.normal_operator:
            clauses.append(f" {key}{middle}{val.value}")
        elif val.is_in:
            clause, in_data = _handle_in(key, middle, val, condition_id)
            clauses.append(clause)
            data.update(in_data)
            continue
        elif val.is_between:
            clause = _handle_between(key, middle, val)
            clauses.append(clause)
        elif val.is_like:
            clause = _handle_like(key, middle, val)
            clauses.append(clause)

        if val.value is not null:
            data[
                NAMING_FORMAT.format(
                    key=key,
                    suffix=suffix,
                    call_id=call_id,
                    depth=depth,
                    condition_id=condition_id,
                )
            ] = old_data

    if not clauses:
        return "", data

    where_clause = "where" + " and".join(clauses)
    return where_clause, data


@lru_cache
def extract_subquery(subquery: SubQuery, depth: int = 1):
    """Extract subquery into a valid SQL statement"""
    return _build_select(
        QueryParams(
            subquery.table,
            subquery.where,
            subquery.cols,
            subquery.limit,
            0,  # type: ignore
            subquery.orders,  # type: ignore
        ),
        depth=depth,
    )


def handle_subquery(key, value, depth):
    """Handle subquery data"""
    subq, subq_data = extract_subquery(value.value, depth=depth + 1)
    clause = f" {key} in ({subq})"
    return clause, subq_data
