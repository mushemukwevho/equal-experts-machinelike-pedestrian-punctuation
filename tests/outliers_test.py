from equalexperts_dataeng_exercise import outliers
import pytest
from datetime import date


epi_yearweek_test_cases = [(date(2022, 1, 2), "202200"), (date(2022, 1, 9), "202201")]

calc_outlier_test_cases = [(2, 3, 2, True), (2, 1, 2, False)]

outlier_weeks_test_cases = [
    ({"202200": 3, "202201": 1, "202201": 1}, [("2022", 0, 3), ("2022", 1, 1)])
]


@pytest.mark.parametrize(
    "date_obj, results",
    epi_yearweek_test_cases,
)
def test_epi_yearweek(date_obj, results):
    _epi_yearweek = outliers.epi_yearweek(date_obj=date_obj)
    assert _epi_yearweek == results


@pytest.mark.parametrize(
    "current_sum, vote_count, week_number, results",
    calc_outlier_test_cases,
)
def test_calc_outlier(current_sum, vote_count, week_number, results):
    _calc_outlier = outliers.calc_outlier(
        current_sum=current_sum, vote_count=vote_count, week_number=week_number
    )
    assert _calc_outlier == results


@pytest.mark.parametrize(
    "vote_count_dict, results",
    outlier_weeks_test_cases,
)
def test_outlier_weeks(vote_count_dict, results):
    _outlier_weeks = outliers.outlier_weeks(vote_count_dict=vote_count_dict)
    assert _outlier_weeks == results
