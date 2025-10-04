"""Test CSV"""

from luminadb import Database
from luminadb.csv import to_csv_file, to_csv_string

from ..setup import setup_database, pstdout, temp_dir

def test_export_csv():
    """Test 0600 Export to CSV"""
    database = Database(":memory:")
    setup_database(database)
    csv = to_csv_string(database)
    print("test_07_00_export_csv\n", csv, "\n", file=pstdout, flush=True)
    assert csv


def test_export_file():
    """Test 0701 Export to CSV file"""
    database = Database(":memory:")
    setup_database(database)
    assert to_csv_file(database.table("users"), temp_dir / "users.csv")  # type: ignore


def test_export_directory():
    """Test 0702 Export to CSV as directory"""
    database = Database(":memory:")
    setup_database(database)
    assert to_csv_file(database, temp_dir / "MemDB")  # type: ignore
