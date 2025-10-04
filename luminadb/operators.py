"""Custom operators for shorthand."""

from typing import Any
from .signature import Signature, op


def eq(name: str, other: Any) -> tuple[str, Signature]:
    """Same as using op == other"""
    return (name, op == other)


def lt(name: str, other: Any) -> tuple[str, Signature]:
    """Same as using op < other"""
    return (name, op < other)


def le(name: str, other: Any) -> tuple[str, Signature]:
    """Same as using op <= other"""
    return (name, op <= other)


def gt(name: str, other: Any) -> tuple[str, Signature]:
    """Same as using op > other"""
    return (name, op > other)


def ge(name: str, other: Any) -> tuple[str, Signature]:
    """Same as using op >= other"""
    return (name, op >= other)


def ne(name: str, other: Any) -> tuple[str, Signature]:
    """Same as using op != other"""
    return (name, op != other)


def like(name: str, condition: str) -> tuple[str, Signature]:
    """Like constraint"""
    return (name, op.like(condition))


def between(name: str, low: int, high: int) -> tuple[str, Signature]:
    """Between constraint"""
    return (name, op.between(low, high))

def in_(values: list[Any]):
    """In VALUES"""
    return this.in_(values)


this = op

__all__ = ["eq", "lt", "le", "gt", "ge", "ne", "like", "between", "this", 'in_']
