"""Indexes"""

from json import dumps
from .utils import check_one, check_iter

class Index:
    """Index"""

    def __init__(
        self,
        name: str = "",
        target: str = "",
        columns: tuple[str] | None = None,
        unique: bool = False,
    ) -> None:
        self._name = name
        self._target = target
        self._columns = columns if columns is not None else ()
        self._unique = unique

    def name(self, name: str):
        """Set this index name"""
        self._name = name
        return self

    def target(self, target: str):
        """Set this index target table"""
        self._target = target
        return self

    def columns(self, *columns: str):
        """Set which index this index is based on"""
        self._columns = columns
        return self

    def unique(self):
        """Set whether this index is unique"""
        self._unique = True
        return self

    def build_sql(self):
        """Return current information as SQL"""
        if not all(map(bool, (self._name, self._target, self._columns))):
            raise ValueError("Either name, target, or column is empty.")
        check_one(self._name)
        check_one(self._target)
        check_iter(self._columns)
        unique = "unique" if self._unique else ""
        columns = ", ".join(self._columns)
        return f"create {unique} index {self._name} on {self._target} ({columns})"

    @property
    def index_name(self):
        """Return index name"""
        return self._name

    @property
    def index_target(self):
        """Return index target"""
        return self._target

    @property
    def index_columns(self):
        """Return columns target"""
        return self._columns

    @property
    def index_unique(self):
        """Return unique property"""
        return self._unique

    def to_json(self):
        """Return current information as JSON"""
        return dumps(
            {
                "name": self._name,
                "target": self._target,
                "columns": self._columns,
                "unique": self._unique,
            }
        )
