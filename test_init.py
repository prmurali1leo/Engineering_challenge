import pandas as pd
from process import get_weather_fact
import pytest
import pyarrow.parquet as pq
import pyarrow as pa

import pandas as pd
from pandas.util.testing import assert_frame_equal
import pytest


def read_csv(file_suffix):

    input_df = pd.read_csv(f"./tests/input{file_suffix}.csv")
    expected_df = pd.read_csv(f"./tests/expected{file_suffix}.csv")
    return input_df, expected_df


@pytest.mark.parametrize("input_df, expected_df", [
    read_csv("1"), read_csv("2"), read_csv("3")
])
def test_get_weather_fact(input_df, expected_df):
    actual_df = get_weather_fact(input_df)
    actual_sorted = actual_df.sort_values(by=["ForecastSiteCode", "ObservationDate", "ObservationTime"]).reset_index(
        drop=True)
    expected_sorted = expected_df.sort_values(
        by=["ForecastSiteCode", "ObservationDate", "ObservationTime"]).reset_index(drop=True)
    actual_sorted = actual_sorted.reindex(sorted(actual_sorted.columns), axis=1)
    expected_sorted = expected_sorted.reindex(sorted(expected_sorted.columns), axis=1)
    columnsTitles = ['ForecastSiteCode', 'ObservationTime', 'ObservationDate', 'WindDirection', 'WindSpeed', 'WindGust',
                     'Visibility', 'ScreenTemperature', 'Pressure', 'SignificantWeatherCode', 'SiteName', 'Latitude',
                     'Longitude', 'Region', 'Country', 'day_night', 'avg_temp', 'count_temp']

    expected_sorted = expected_sorted.reindex(columns=columnsTitles)
    actual_sorted = actual_sorted.reindex(columns=columnsTitles)

    assert_frame_equal(actual_sorted, expected_sorted)
