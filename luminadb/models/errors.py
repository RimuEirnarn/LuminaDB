"""Error modules"""

from ..errors import UnexpectedResultError

class ValidationError(ValueError):
    """Specific validation error; a value fails validation test"""

class ConstraintError(UnexpectedResultError):
    """Specific constraint is found twice, thrice, etc."""

class NoDataReturnedError(ValueError):
    """Data fetched from model cannot be retrieved"""
