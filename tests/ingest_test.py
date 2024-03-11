import logging
import os
import subprocess
import time

import duckdb
import pytest

logger = logging.getLogger()


@pytest.fixture(autouse=True)
def delete_existing_db():
    if os.path.exists("warehouse.db"):
        os.remove("warehouse.db")


def run_ingestion() -> float:
    """
    Returns time in seconds that the
    ingestion process took to run
    """
    logger.info("Running ingestion")
    tic = time.perf_counter()
    result = subprocess.run(
        args=[
            "python",
            "-m",
            "equalexperts_dataeng_exercise.ingest",
            "uncommitted/votes.jsonl",
        ],
        capture_output=True,
    )
    toc = time.perf_counter()
    result.check_returncode()
    return toc - tic


def run_incorrect_ingestion() -> float:
    """
    Returns time in seconds that the ingestion process took to run
    """
    logger.info("Running ingestion")
    tic = time.perf_counter()
    result = subprocess.run(
        args=[
            "python",
            "-m",
            "equalexperts_dataeng_exercise.ingest",
            "uncommitted/votes.mp3",
        ],
        capture_output=True,
    )
    toc = time.perf_counter()
    result.check_returncode()
    return toc - tic


def test_check_table_exists():
    run_ingestion()
    sql = """
        SELECT table_name
        FROM information_schema.tables
        WHERE table_type LIKE '%TABLE' AND table_name='votes' AND table_schema='blog_analysis';
    """
    con = duckdb.connect("warehouse.db", read_only=True)
    result = con.sql(sql)
    assert len(result.fetchall()) == 1, "Expected table 'votes' to exist"


def test_check_table_not_exist():
    run_incorrect_ingestion()
    sql = """
        SELECT table_name
        FROM information_schema.tables
        WHERE table_type LIKE '%TABLE' AND table_name='votes' AND table_schema='blog_analysis';
    """
    try:
        con = duckdb.connect("warehouse.db", read_only=True)
        con.sql(sql)
        assert False
    except duckdb.CatalogException:
        assert True
