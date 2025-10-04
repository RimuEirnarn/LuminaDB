"""Table Creation"""

from typing import Iterable, Any
from shlex import shlex

from .utils import filter_extraction, SQL_ACTIONS, DEFAULT_MAPPINGS

from ..typings import MasterQuery
from ..column import Column
from ..locals import SQLACTION, _SQLITETYPES

class TableCreationExtractor:
    """Base processor for table creation"""

    def __init__(self, columns: Iterable[Column], type_mappings: dict[str, str] | None):
        self.columns = columns
        self.type_mappings = _get_type_mappings(type_mappings)
        self.primaries: list[Column] = []
        self.foreigns: list[Column] = []
        self.string = ""

    def process_columns(self):
        """Process each columns"""
        self.string = _iterate_etbc_step1(
            self.columns, self.string, self.primaries, self.foreigns, self.type_mappings
        )

    def add_primary_keys(self):
        """Add primary key constraints"""
        if self.primaries:
            self.string += (
                f" primary key ({', '.join((col.name for col in self.primaries))}),"
            )

    def add_foreign_keys(self):
        """Add foreign key constraints"""
        for column in self.foreigns:
            stable, sname = column.source, column.source_column
            self.string += (
                f" foreign key ({column.name}) references {stable} ({sname})"
                f" on delete {_iterate_sql_action(column.on_delete)}"  # type: ignore
                f" on update {_iterate_sql_action(column.on_update)},"  # type: ignore
            )

    def extract(self) -> str:
        """Extract everything into a string"""
        self.process_columns()
        self.add_primary_keys()
        self.add_foreign_keys()
        return self.string[1:-1]

def _iterate_sql_action(action: SQLACTION):
    return SQL_ACTIONS.get(action, action)


def _get_type_mappings(type_mappings: dict[str, str] | None) -> dict[str, str]:
    """Get the merged type mappings."""
    maps = DEFAULT_MAPPINGS.copy()
    if type_mappings:
        maps.update(type_mappings)
    return maps


def extract_table_creations(
    columns: Iterable[Column], type_mappings: dict[str, str] | None = None
):
    """Extract columns classes to sqlite table creation query."""
    extractor = TableCreationExtractor(columns, type_mappings)
    return extractor.extract()


def _iterate_etbc_step1(
    columns: Iterable[Column],
    string: str,
    primaries: list[Column],
    foreigns: list[Column],
    maps: dict[str, str],
):
    for column in columns:
        ctype = maps.get(column.type, column.type)
        string += _process_column_constraints(column, ctype)
        if column.raw_source:
            foreigns.append(column)
        if column.primary:
            primaries.append(column)
        string += ","
    return string


def _process_column_constraints(column: Column, ctype: str) -> str:
    """Process constraints for a single column."""
    constraints = f" {column.name} {ctype}"
    if not column.nullable:
        constraints += " not null"
    if column.unique:
        constraints += " unique"
    if column.default:
        constraints += f" default {repr(column.default)}"
    return constraints

def extract_single_column(column: Column):
    """Extract a column class to sqlite column creation query"""
    foreign = column.foreign
    primary = column.primary
    string = f"{column.name} {column.type}"
    if not column.nullable:
        string += " not null"
    if column.unique:
        string += " unique"
    if column.default:
        string += f" default {repr(column.default)}"

    if primary:
        string += "primary key"
    if foreign:
        string += f"foreign key references {column.source} ({column.source_column})"
    return string

def basic_extract(table_creation: str):  # pylint: disable=too-many-locals
    """basic extraction for table"""
    data = table_creation[table_creation.find("(") + 1 : -1]
    cols = []
    upheld: dict[str, list[Any]] = {}
    for constr in data.split(","):
        name, type_, defaults, sources = "", "", None, ()
        primary = foreign = notnull = unique = False
        base_columns = list(shlex(constr))
        if base_columns[1] not in _SQLITETYPES:
            break  # Other constraint
        (
            name,
            type_,
        ) = (
            base_columns[0],
            base_columns[1],
        )
        if len(base_columns) == 2:
            # cols.append(Column(name, type_))  # type: ignore
            upheld[name] = [name, type_, False, None, False, False, True, None]
            continue
        for token in base_columns:
            token_lowered = token.lower()
            if token_lowered == "defaults":
                defaults = base_columns[base_columns.index(token) + 1]
                defaults = (
                    defaults
                    if defaults[0] != '"' or defaults[0] != "'"
                    else defaults[1:-1]
                )
            if token_lowered == "primary":
                primary = True
            if token_lowered == "foreign":
                foreign = True
            if token_lowered == "reference":
                tb_index = base_columns.index(token) + 1
                tb_col = tb_index + 1
                sources = (base_columns[tb_index], base_columns[tb_col][1:-1])
            if token_lowered == "null":
                notnull = base_columns[base_columns.index(token) - 1].lower() == "not"
            if token_lowered == "unique":
                unique = True
        upheld[name] = [
            name,
            type_,
            foreign,
            f"{sources[0]}/{sources[1]}" if sources else None,
            primary,
            unique,
            not notnull,
            defaults if defaults else None,
        ]
    return cols, upheld

def extract_table(  # pylint: disable=too-many-locals
    table_creation: str,
) -> list[Column]:
    """Extract SQLite table string"""
    data = table_creation[table_creation.find("(") + 1 : -1]
    cols, upheld = basic_extract(table_creation)
    shlexed = list(shlex(data))
    _, paren_wrap, filtered = filter_extraction(data, shlexed)

    # for efficiency, is this part efficient though?
    # although, can SOMEONE have a million columns on a sqlite table?
    # So, the efficiency on this blob isn't a concern.
    for column_string in filtered.split(","):
        column_shlexed = list(shlex(column_string))
        for tindex, token in enumerate(column_shlexed):
            if token.lower() == "primary":
                next_ = tindex + 2
                str_wrap = "".join(column_shlexed[next_ : next_ + 2])
                if str_wrap.startswith(":wrap"):
                    wrap = paren_wrap[str_wrap]
                else:
                    wrap = str_wrap
                for name in (
                    wrap[1:-1].split(",") if wrap.startswith("(") else wrap.split(",")
                ):
                    upheld[name][4] = True
                continue
            if token.lower() == "foreign":
                next_ = tindex + 2
                str_wrap = "".join(column_shlexed[next_ : next_ + 2])
                if str_wrap.startswith(":wrap"):
                    wrap = paren_wrap[str_wrap]
                else:
                    wrap = str_wrap
                name = (
                    wrap[1:-1] if wrap.startswith("'") or wrap.startswith('"') else wrap
                )
                name = wrap[1:-1] if wrap.startswith("(") else wrap
                tb_index = next_ + 3
                tb_col = tb_index + 1
                source_col_str = paren_wrap[f":{column_shlexed[tb_col+1]}"][1:-1]
                sources = f"{column_shlexed[tb_index]}/{source_col_str}"
                upheld[name][3] = sources
                upheld[name][2] = True
                if "delete" in column_shlexed:
                    delete_index = column_shlexed.index("delete")
                    upheld[name].append(column_shlexed[delete_index + 1])
                if "update" in column_shlexed:
                    uindex = column_shlexed.index("update")
                    upheld[name].append(column_shlexed[uindex + 1])

    for _, upheld_column in upheld.items():
        cols.append(Column(*upheld_column))
    return cols

def fetch_columns(_master_query: MasterQuery):
    """Fetch columns of a table. `master_query` is originated from select()
    on sqlite_master table"""
    sql = _master_query["sql"]
    return extract_table(sql)
