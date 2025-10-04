"""CSV module, used to export database/table to csv"""

from csv import DictWriter, DictReader
from io import StringIO
from os import mkdir
from os.path import join as join_path, isfile, exists

from . import Database, Table


def _export_table(table: Table) -> str:
    data = StringIO()
    tb_data = table.select_one()
    if tb_data is None:
        return ""
    fields = tuple(tb_data.keys())
    csv_writer = DictWriter(data, fields)
    csv_writer.writeheader()
    for selecteds in table.paginate_select(length=1):
        if len(selecteds) == 0:
            break
        selected = selecteds[0]
        csv_writer.writerow(selected)
    return data.getvalue()


def _export_database(database: Database) -> list[tuple[str, str]]:
    exported = []
    for table in database.tables():
        exported.append((table.name, _export_table(table)))
    return exported


def _process_database(filename: str, data: list[tuple[str, str]]):
    for table in data:
        base = join_path(filename, f"{table[0]}.csv")
        with open(base, "w", encoding="utf-8") as file:
            file.write(table[1])


def to_csv_string(table_or_database: Table | Database):
    """Export database/table to csv"""
    if isinstance(table_or_database, Table):
        return _export_table(table_or_database)
    if isinstance(table_or_database, Database):
        return _export_database(table_or_database)
    raise TypeError(
        f"Expected Table or Database, got {type(table_or_database).__name__}"
    )


def to_csv_file(table_or_database: Table | Database, file: str):
    """Export database/table to csv file, will act different if database is exported.

    If database is passed, it tries to create a directory then export all table"""
    readed = to_csv_string(table_or_database)
    if isinstance(readed, list):
        if not exists(file):
            mkdir(file)
        if isfile(file):
            raise NotADirectoryError(f"{file} is a file, export failed!")

        _process_database(file, readed)
        return True
    with open(file, "w", encoding="utf-8") as fio:
        fio.write(readed)
    return True

def from_csv_string(table: Table, data: str):
    """Insert from CSV data"""
    reader = DictReader(data)
    for entry in reader:
        table.insert(entry)

def from_csv_file(table: Table, data: str):
    """Insert from CSV file"""
    with open(data, encoding='utf-8') as f:
        fdata = f.read()
    return from_csv_string(table, fdata)
