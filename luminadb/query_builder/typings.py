"""Typings"""

from typing import TypeAlias, Any, Literal

from ..subquery import SubQuery
from ..functions import ParsedFn
from ..signature import Signature
from ..operators import Alias

ConditionDict: TypeAlias = dict[str, Signature | ParsedFn | SubQuery | Any]
ConditionList: TypeAlias = list[tuple[str, Signature | SubQuery | ParsedFn]]
Condition: TypeAlias = ConditionDict | ConditionList | None
CacheCond: TypeAlias = tuple[tuple[str, Signature | SubQuery | ParsedFn], ...] | None
CacheOrders: TypeAlias = tuple[str, Literal["asc", "desc"]] | ParsedFn | None
CacheData: TypeAlias = tuple[str, ...]
OnlyColumn: TypeAlias = tuple[str | Alias, ...] | str | Alias | ParsedFn
