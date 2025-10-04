"""Signature"""

from typing import Any, Optional

from .utils import matches, null
from .errors import SecurityError
from .locals import _NO_UNLIKE
from .typings import tuple_list


class Signature:
    """Signature

    This class is used at most filter parameters at `Table` class.

    You can use variable op (operator) to do all conditional logic or operator.
    There are ==, <=, <, !=, >, and >=

    .negate() adds 'not' to the operator, so from == would equal to != when used with
    .negate()

    .like() only search for alike characters. Please refer to sqlite like command.

    .between() is a ranging operator, there's low and high value.

    Example of everything:
        >>> op == 5
        >>> op <= 2
        >>> op < 2
        >>> op != 5
        >>> op > 7
        >>> op >= 7
        >>> op.like('Ar%')
        >>> op.like('Ar_')
        >>>op.between(1, 10)

    You can now use function from :operators:"""

    def __init__(
        self,
        value: Any = null,
        operator: Optional[str] = None,
        data: Optional[tuple[int, int] | str] | tuple_list[Any] = None,
        negate=False,
    ) -> None:
        self._value = value
        self._operator: str = "" if operator is None else operator
        self._data = data
        self._negate = negate

    def __eq__(self, __o) -> "Signature":
        return Signature(__o, "=")

    def __lt__(self, __o) -> "Signature":
        return Signature(__o, "<")

    def __le__(self, __o) -> "Signature":
        return Signature(__o, "<=")

    def __gt__(self, __o) -> "Signature":
        return Signature(__o, ">")

    def __ge__(self, __o) -> "Signature":
        return Signature(__o, ">=")

    def __ne__(self, __o) -> "Signature":
        return Signature(__o, "!=")

    def like(self, str_condition: str):
        """Like"""
        is_valid = not matches(_NO_UNLIKE, str_condition)
        if not is_valid:
            raise SecurityError("Cannot understand other character.")
        return Signature(null, "like", str_condition, self._negate)

    def in_(self, values: list[Any]):
        """IN"""
        return Signature(null, operator="in", data=tuple(values), negate=self._negate)

    def between(self, low: int, high: int):
        """Betweeen"""
        return Signature(null, "between", (low, high), self._negate)

    def negate(self):
        """Negate or adding NOT"""
        return Signature(
            self._value, self._operator, tuple(self._data), not self._negate  # type: ignore
        )

    @property
    def value(self):
        """value"""
        return self._value

    @property
    def data(self):
        """data"""
        return self._data

    @property
    def kind_sign(self):
        """Signature kind"""
        return {
            "=": "eq",
            "<": "lt",
            "<=": "le",
            ">": "gt",
            ">=": "ge",
            "!=": "ne",
        }.get(self._operator, self._operator)

    @property
    def normal_operator(self):
        """Is it normal?"""
        return self._operator in ("!=", "=", "<", "<=", ">", ">=")

    @property
    def negated(self):
        """Is negated"""
        return self._negate

    @property
    def is_between(self):
        """Is operator between"""
        return self._operator == "between"

    @property
    def is_like(self):
        """Is operator like"""
        return self._operator == "like"

    @property
    def is_in(self):
        """Is operator in?"""
        return self._operator == "in"

    def generate(self):
        """Generate operator string"""
        string = "not " if self.negated else ""
        string += self._operator
        return string

    def __hash__(self):
        return hash((self._value, self._data, self._negate, self._operator))

    def __repr__(self) -> str:
        return f"<Signature -> {self._operator} {self._data!r}>"


op = Signature()
