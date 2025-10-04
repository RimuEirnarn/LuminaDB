"""Errors"""

class Rejection(RuntimeError):
    """Cannot push into worker db"""

class DependencyError(ImportError):
    """Specific dependency is missing"""


class SecurityError(Exception):
    """SQLInjection ahead."""


class UnexpectedResultError(Exception):
    """Result of something is unexpected."""


class TableRemovedError(Exception):
    """Table is removed"""


class DatabaseExistsError(UnexpectedResultError):
    """Database that would be created is already exists."""


class DatabaseMissingError(UnexpectedResultError):
    """Database that would be accessed is missing."""


class ObjectRemovedError(BaseException):
    """Object is deleted from memory and cannot be obtained"""

class CuteDemonLordException(Exception):
    """A demon lord (cute one, somehow a dude) aborts an SQL statement"""

class FeatureGatekeep(ValueError):
    """A feature is gatekeeped"""

class VersionError(FeatureGatekeep):
    """Current version cannot use this feature"""

class ImplementationWarning(FutureWarning):
    """The implementation of specific feature is not supported"""
