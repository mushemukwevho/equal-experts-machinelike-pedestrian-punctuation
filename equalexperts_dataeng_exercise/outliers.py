from datetime import datetime
from epiweeks import Week
import multiprocessing
from collections import Counter
from .db import DuckDBService
import logging
import traceback
from typing import Any

logger = logging.getLogger()


def epi_yearweek(date_obj: datetime) -> Any:
    try:
        logger.info(f"Getting epidemiological week from date: date = {date_obj}.")
        no_offset_yearweek = str(Week.fromdate(date_obj))

        # Week numbers are starting from 0 instead of 1
        offset_week = "{:02d}".format(int(no_offset_yearweek[-3:]) - 1)
        logger.info(
            f"Epidemiological week from date: date = {date_obj}, Year={no_offset_yearweek[:-3]} and week_number={offset_week}."
        )
        return no_offset_yearweek[:-3] + offset_week
    except Exception:
        traceback.print_exc()
        logger.error(
            f"Failed getting epidemiological week from date: date = {date_obj}."
        )
        return None


def calc_outlier(current_sum: int, vote_count: int, week_number: int) -> bool:
    try:
        logger.info(
            f"Calculating the outlier: current_sum = {current_sum}, vote_count = {vote_count} and week_number = {week_number}."
        )
        # edit week number to be for weeks that were calculated or observed...
        # count the number of weeks in weeks data or
        # we are assume weeks are sequential starting from the first week of the year
        result = abs(1 - ((current_sum / week_number) / vote_count))
        logger.info(f"calculation result = {result}.")
        if result > 0.2:
            return True
        return False
    except ZeroDivisionError:
        return True
    except Exception:
        traceback.print_exc()
        logger.error("Failed calculating outlier.")
        return False


def vote_count(vote_dates: list) -> dict:
    results = []
    with multiprocessing.Pool(processes=16) as process_pool:
        results = process_pool.starmap(epi_yearweek, vote_dates)
    return dict(Counter(results))


def outlier_weeks(vote_count_dict: dict) -> list:
    data_to_insert = []
    current_sum = 0
    for key, value in vote_count_dict.items():
        if calc_outlier(
            current_sum=current_sum, vote_count=int(value), week_number=int(key[-2:])
        ):
            week_data = (key[:4], int(key[-2:]), int(value))
            data_to_insert.append(week_data)
        current_sum += int(value)

    return data_to_insert


def outlier_weeks_calculation() -> list:
    with DuckDBService.connect_db() as duckdb_conn:
        if duckdb_conn is None:
            logger.error("No duckdb connection.")
            raise ConnectionError("No duckdb connection.")

        sql = """SELECT CreationDate::Date FROM
        blog_analysis.votes ORDER BY CreationDate::Date ASC;"""

        vote_dates = duckdb_conn.execute(sql).fetchall()

        vote_count_dict = vote_count(vote_dates=vote_dates)

        outlier_weeks_data = outlier_weeks(vote_count_dict=vote_count_dict)

        logger.info("Making queries to DuckDB.")

        duckdb_conn.execute("CREATE SCHEMA IF NOT EXISTS blog_analysis")
        duckdb_conn.execute(
            """CREATE TABLE IF NOT EXISTS
            blog_analysis.outlier_weeks_data(Year VARCHAR, WeekNumber INT, VoteCount INT);"""
        )
        duckdb_conn.executemany(
            """INSERT INTO blog_analysis.outlier_weeks_data VALUES (?, ?, ?);""",
            outlier_weeks_data,
        )
        duckdb_conn.execute(
            """CREATE OR REPLACE VIEW blog_analysis.outlier_weeks AS
            SELECT Year, WeekNumber, VoteCount FROM blog_analysis.outlier_weeks_data
            ORDER BY Year ASC,WeekNumber ASC;"""
        )
        return duckdb_conn.execute(
            """SELECT Year, WeekNumber, VoteCount FROM blog_analysis.outlier_weeks
            ORDER BY Year ASC,WeekNumber ASC;"""
        ).fetchall()


if __name__ == "__main__":
    outlier_weeks_calculation()
