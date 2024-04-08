import duckdb


def test_duckdb_connection():
    cursor = duckdb.connect("warehouse.db")
    assert list(cursor.execute("SELECT 1").fetchall()) == [(1,)]
