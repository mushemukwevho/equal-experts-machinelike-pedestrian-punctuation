import duckdb
from typing import Any
import logging
import traceback

# Settings

# TODO: Can be secret on AWS/GCP
DB_NAME = "warehouse.db"

logger = logging.getLogger()


class DuckDBService:
    """DuckDB Service"""

    @staticmethod
    def connect_db(db_name: str = DB_NAME) -> Any:
        """Connect to duckdb database.
        params:
            db_name = duckdb database name
        returns: Connection object or None
        """
        try:
            logger.info(f"Connecting to duckdb: db_name={db_name}")
            return duckdb.connect(db_name)
        except Exception:
            traceback.print_exc()
            logger.error("Error occured connecting to duckdb")
            return None
