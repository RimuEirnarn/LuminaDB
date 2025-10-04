"""Test Custom"""

from luminadb import Database, this
from luminadb.operators import in_
from luminadb._debug import STATE

from .setup import setup_orderable, file, pstdout

def test_98_00_test():
    """Gradual test"""
    db = Database(":memory:")
    setup_orderable(db)
    items = db.table("items")
    v0 = items.select({"quantity": this == 99})
    STATE["DEBUG"] = True
    values = items.select({"quantity": in_([99, 98, 97])})
    assert v0
    assert values
    STATE["DEBUG"] = False


def test_99_99_save_report():
    """FINAL 9999 Save reports"""
    with open(file, "w", encoding="utf-8") as xfile:
        xfile.write(pstdout.getvalue())
