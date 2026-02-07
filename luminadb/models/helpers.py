"""Model helpers"""

# pylint: disable=invalid-name,too-few-public-methods,abstract-method,protected-access

from typing import Any, Callable, Type, TypeAlias, TypeVar, overload
from enum import StrEnum

import luminadb
from .errors import ValidationError
from ..column import BuilderColumn, text, integer, blob, boolean, real

TypeFunction: TypeAlias = Callable[[str], BuilderColumn]
Model = TypeVar("Model", bound="luminadb.BaseModel")
FuncT = TypeVar("FuncT", bound=Callable[..., bool])
BaseModel: TypeAlias = "luminadb.BaseModel"

TYPES: dict[Type[Any], TypeFunction] = (
    {  # pylint: disable=possibly-used-before-assignment
        int: integer,
        str: text,
        bytes: blob,
        bool: boolean,
        float: real
    }
)

VALID_HOOKS_NAME = (
    "before_create",
    "after_create",
    "before_update",
    "after_update",
    "before_delete",
    "after_delete",
)


class ConstraintEnum(StrEnum):
    """Constraints for update/delete"""

    RESTRICT = "restrict"
    SETNULL = "null"
    CASCADE = "cascade"
    NOACT = "no act"
    DEFAULT = "default"


RESTRICT = ConstraintEnum.RESTRICT
SETNULL = ConstraintEnum.SETNULL
CASCADE = ConstraintEnum.CASCADE
NOACT = ConstraintEnum.NOACT
DEFAULT = ConstraintEnum.DEFAULT


class Constraint:
    """Base constraint class for models"""

    def __init__(self, column: str) -> None:
        self._column = column

    @property
    def column(self):
        """Columns"""
        return self._column

    def apply(self, type_: BuilderColumn):
        """Apply this constraint to an column"""
        raise NotImplementedError()


class Unique(Constraint):
    """Unique constraint"""

    def apply(self, type_: BuilderColumn):
        type_.unique()


class Foreign(Constraint):
    """Foreign constraint"""

    def __init__(self, column: str, target: str | Type[Model]) -> None:
        super().__init__(column)
        self._target = target
        self.resolve()
        self._base = target
        self._on_delete = DEFAULT
        self._on_update = DEFAULT

    @property
    def target(self):
        """Target foreign constraint"""
        return self._target

    def on_delete(self, constraint: ConstraintEnum):
        """On delete constraint"""
        self._on_delete = constraint
        return self

    def on_update(self, constraint: ConstraintEnum):
        """On update constraint"""
        self._on_update = constraint
        return self

    def resolve(self):
        """Resolve if current target is a Model"""
        if issubclass(self._target, luminadb.BaseModel):  # type: ignore
            name = self._target.__table_name__
            target = self._target._primary  # pylint: disable=protected-access
            if not target:
                raise ValueError(f"{type(self._target)} does not have primary key")
            self._target = f"{name}/{target}"

    def apply(self, type_: BuilderColumn):
        type_.foreign(self._target)  # type: ignore
        if self._on_delete != DEFAULT:
            type_.on_delete(self._on_delete.value)
        if self._on_update != DEFAULT:
            type_.on_update(self._on_update.value)


class Primary(Constraint):
    """Primary constraint"""

    def apply(self, type_: BuilderColumn):
        """Apply this constraint as primary"""
        type_.primary()

class Validators:
    """Base class to hold validators"""

    def __init__(self, fn: Callable[[Any], bool], if_fail: str) -> None:
        self._callable = fn
        self._reason = if_fail

    def validate(self, instance: BaseModel):
        """Validate a value"""
        if not self._callable(instance):
            err = ValidationError(self._reason)
            err.add_note(str(instance))
            raise err
        return True


@overload
def hook(fn_or_name: Callable[[Model], None]) -> "staticmethod[[Callable[[Model], None]], None]":
    pass


@overload
def hook(fn_or_name: str):
    pass


def hook(fn_or_name):
    """Register a hook"""

    def decorator(func):
        fn = staticmethod(func)
        fn_name = func.__name__
        final_name = fn_or_name if fn_name not in VALID_HOOKS_NAME else fn_name

        if final_name is None:
            raise ValueError("Hooks name is not valid. Provide with @hook(name)")
        fn._hooks_info = (fn_or_name  # type: ignore
                          if fn_name not in VALID_HOOKS_NAME else fn_name,)
        return fn

    return decorator(fn_or_name) if callable(fn_or_name) else decorator


@overload
def validate(fn_or_column: FuncT) -> "staticmethod[[FuncT], bool]":
    pass


@overload
def validate(fn_or_column: str, reason: str | None = None):  # type: ignore
    pass


def validate(fn_or_column, reason=None):
    """Register a validator"""

    def decorator(func: Callable):
        print(func, fn_or_column)
        fn = staticmethod(func)
        name = func.__name__
        inferred_col = (
            name[len("validate_") :] if name.startswith("validate_") else None
        )

        col = fn_or_column or inferred_col
        if callable(col):
            col = inferred_col
        if col is None:
            raise ValueError("Validator must have a column name.")

        msg = reason or func.__doc__ or f"Validation failed for '{col}'"
        fn._validators_info = (col, msg)  # type: ignore
        return fn

    return decorator(fn_or_column) if callable(fn_or_column) else decorator


def initiate_hook(cls: Type[BaseModel], member: Callable):
    """Initiate hooks"""
    if hasattr(member, "_hooks_info"):
        info = member._hooks_info
        cls._register("hook", info[0])(member)


def initiate_validators(cls: Type[BaseModel], member: Callable):
    """Initiate validators"""
    if hasattr(member, "_validators_info"):
        info = member._validators_info
        cls._register("validator", info[0], info[1])(member)
