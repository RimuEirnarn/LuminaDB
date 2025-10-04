"""Type checking"""

from typing import TYPE_CHECKING, Any, Type
from dataclasses import fields, is_dataclass
from .helpers import validate

if TYPE_CHECKING:
    from . import BaseModel

def infer_type(name: str, type_: "Type[Any]"):
    """Infer type checking for specific columns"""
    if not isinstance(type_, type):
        return None

    @validate(name, f"{name} type is not {type_.__name__}") # type: ignore
    def function(instance: "Type[BaseModel]"):
        return isinstance(getattr(instance, name), type_)
    return function

def typecheck(cls: "Type[BaseModel]"):
    """Automatically pushed Runtime type checking"""
    if not is_dataclass(cls):
        raise TypeError(f"{cls.__name__} is not a dataclass")

    for field in fields(cls):
        name, type_ = field.name, field.type
        yield infer_type(name, type_) # type: ignore
