import pytest
import pandas as pd
from main import YellowTaxiData


@pytest.fixture
def taxi_data():
    data_instance = YellowTaxiData(start_date='2022-03-01', end_date='2022-03-31')
    data_instance.data = pd.read_parquet('yellow_tripdata_2022-03.parquet')
    return data_instance


def test_import_data(taxi_data):
    taxi_data.import_data()
    assert not taxi_data.data.empty


def test_clean_data(taxi_data):
    initial_len = taxi_data.data.shape[0]
    taxi_data.clean_data()
    assert len(taxi_data.data) <= initial_len


def test_generate_weeks(taxi_data):
    taxi_data.generate_weeks()
    assert not taxi_data.weeks_ranges.empty
    assert "num_week" in taxi_data.weeks_ranges.columns


def test_generate_months(taxi_data):
    taxi_data.generate_months()
    assert not taxi_data.months_ranges.empty
    assert "start_day" in taxi_data.months_ranges.columns
