import duckdb
from equalexperts_dataeng_exercise import db


def test_duckdb_connection():
    cursor = duckdb.connect("warehouse.db")
    assert list(cursor.execute("SELECT 1").fetchall()) == [(1,)]


def test_duckdb_service():
    db_conn = db.DuckDBService.connect_db("warehouse.db")
    assert list(db_conn.execute("SELECT 1").fetchall()) == [(1,)]
