"""SQLite Functions, this provide 'low level interface'."""

from hashlib import md5
from typing import NamedTuple, Any

from .utils import check_one  # type: ignore


def get_uid_from_args(fn_name, args):
    """Get unique id suffix from args"""
    hasher = md5(usedforsecurity=False)
    hasher.update(fn_name.encode())
    for arg in args:
        hasher.update(str(arg).encode())
    return int(hasher.hexdigest()[:8], 16)

# It was moved here, so users don't just import anything from query builder.


def _function_extract(
    parsed: "ParsedFn", depth: int = 0
) -> tuple[str, dict[str, Any]]:
    """Extract function into SQL function syntax."""
    check_one(parsed.name)
    if len(parsed.values) == 1:
        if parsed.values[0] in (None, ..., "*"):
            return f"{parsed.name}(*)", {}
        # need to find a check if input is column name or some values.
        return f"{parsed.name}({check_one(parsed.values[0])})", {}
    string = parsed.name + "("
    data = {}
    # ? we don't know how many same function calls at the same time, though we can use count param.
    suffix = get_uid_from_args(parsed.name, parsed.values)
    for i, _ in enumerate(parsed.values):
        key = f"fncall_{parsed.name}_{depth}_{i}__{suffix}"
        if isinstance(i, ParsedFn):
            sql, databin = _function_extract(i)
            data[key] = sql
            data.update(databin)
            continue
        data[key] = i
        string += f":{key}, "
    string = string[:-2] + ")"
    return string, data


class ParsedFn(NamedTuple):
    """Next step to query_builder"""

    name: str
    values: tuple[Any, ...]

    def parse_sql(self):
        """Parse the ParsedFn into SQL"""
        return _function_extract(self)


class Function:  # pylint: disable=too-few-public-methods
    """Provide Function API calls"""

    def __init__(self, name: str):
        self._name = check_one(name)

    def __call__(self, *args):
        return ParsedFn(self._name, args)

    @property
    def name(self):
        """Name of the function"""
        return self._name


count = Function("COUNT")
