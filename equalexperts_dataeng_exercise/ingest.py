import logging
import sys
import traceback

from .db import DuckDBService

logger = logging.getLogger()

DATA_FORMATS = (".json", ".jsonl")


class IngestDuckDBService:
    # """Ingest data to duckdb."""

    @staticmethod
    def ingest_data() -> None:
        # """Ingest votes from json file into duckdb."""
        try:
            data_file = sys.argv[1]
            if data_file and not data_file.endswith(DATA_FORMATS):
                logger.error(f"Incorrect data file format: data_file = {data_file}")
                raise TypeError("Only accepting json/jsonl file formats.")

            with DuckDBService.connect_db() as duckdb_conn:
                if duckdb_conn is None:
                    logger.error("No duckdb connection.")
                    raise ConnectionError("No duckdb connection.")

                duckdb_conn.execute("CREATE SCHEMA IF NOT EXISTS blog_analysis")
                duckdb_conn.execute(
                    """CREATE TABLE IF NOT EXISTS blog_analysis.votes(Id VARCHAR,
                    PostId VARCHAR, VoteTypeId VARCHAR, CreationDate TIMESTAMP,
                    PRIMARY KEY(Id));"""
                )

                logger.info(f"Ingesting data from data file: data_file = {data_file}")

                duckdb_conn.execute(
                    f"""INSERT OR REPLACE INTO blog_analysis.votes SELECT Id, PostId,
                    VoteTypeId, CreationDate FROM read_json_auto('{data_file}',
                    format=newline_delimited);"""
                )

        except FileNotFoundError:
            print("Please download the dataset using 'poetry run exercise fetch-data'")

        # Duckdb query related errors
        except Exception:
            traceback.print_exc()
            logger.error("Error occurred ingesting data to db")


if __name__ == "__main__":
    IngestDuckDBService.ingest_data()
