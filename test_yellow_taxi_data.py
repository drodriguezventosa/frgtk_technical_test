import pytest
import pandas as pd
import os
from main import YellowTaxiData


@pytest.fixture
def taxi_data():
    data_instance = YellowTaxiData(start_date='2022-03-01', end_date='2022-03-31')
    data_instance.data = pd.read_parquet('yellow_tripdata_2022-03.parquet')
    return data_instance


@pytest.fixture
def clean_taxi_data(taxi_data):
    taxi_data.clean_data()
    return taxi_data


@pytest.fixture
def full_taxi_data(clean_taxi_data):
    clean_taxi_data.add_more_columns()
    clean_taxi_data.generate_week_metrics()
    clean_taxi_data.generate_month_metrics()
    clean_taxi_data.format_data()
    return clean_taxi_data


# --- Import ---
def test_import_data(taxi_data):
    taxi_data.import_data()
    assert not taxi_data.data.empty


# --- Clean data ---
def test_clean_data_reduces_rows(taxi_data):
    initial_len = taxi_data.data.shape[0]
    taxi_data.clean_data()
    assert len(taxi_data.data) <= initial_len


def test_clean_no_null_critical_columns(clean_taxi_data):
    for col in ['tpep_pickup_datetime', 'tpep_dropoff_datetime', 'passenger_count']:
        assert clean_taxi_data.data[col].isnull().sum() == 0


def test_clean_passenger_count_positive(clean_taxi_data):
    assert (clean_taxi_data.data['passenger_count'] > 0).all()


def test_clean_trip_distance_positive(clean_taxi_data):
    assert (clean_taxi_data.data['trip_distance'] > 0).all()


def test_clean_total_amount_valid(clean_taxi_data):
    assert (clean_taxi_data.data['total_amount'] > 0).all()
    assert (clean_taxi_data.data['total_amount'] <= 5000).all()


def test_clean_min_trip_duration(clean_taxi_data):
    duration = (clean_taxi_data.data['tpep_dropoff_datetime'] - clean_taxi_data.data['tpep_pickup_datetime']).dt.total_seconds()
    assert (duration >= 60).all()


def test_clean_max_speed(clean_taxi_data):
    duration_hours = (clean_taxi_data.data['tpep_dropoff_datetime'] - clean_taxi_data.data['tpep_pickup_datetime']).dt.total_seconds() / 3600
    speed = clean_taxi_data.data['trip_distance'] / duration_hours
    assert (speed <= 100).all()


def test_clean_dates_in_range(clean_taxi_data):
    assert (clean_taxi_data.data['tpep_pickup_datetime'] >= clean_taxi_data.start_date).all()
    assert (clean_taxi_data.data['tpep_dropoff_datetime'] <= clean_taxi_data.end_date).all()


# --- Add more columns ---
def test_add_columns_exist(clean_taxi_data):
    clean_taxi_data.add_more_columns()
    for col in ['year_month', 'year_week', 'year_month_day']:
        assert col in clean_taxi_data.data.columns
        assert clean_taxi_data.data[col].isnull().sum() == 0


def test_year_week_format(clean_taxi_data):
    clean_taxi_data.add_more_columns()
    pattern = r'^\d{4}-\d{2,3}$'
    assert clean_taxi_data.data['year_week'].str.match(pattern).all()


# --- Week metrics ---
def test_week_metrics_not_empty(full_taxi_data):
    assert not full_taxi_data.csv_df.empty


def test_week_metrics_columns(full_taxi_data):
    expected = ['year_week', 'min_trip_time', 'max_trip_time', 'mean_trip_time',
                'min_trip_distance', 'max_trip_distance', 'mean_trip_distance',
                'min_trip_amount', 'max_trip_amount', 'mean_trip_amount',
                'total_services', 'percentage_variation']
    assert list(full_taxi_data.csv_df.columns) == expected


def test_week_metrics_min_max_consistency(full_taxi_data):
    df = full_taxi_data.csv_df
    assert (df['min_trip_time'] <= df['mean_trip_time']).all()
    assert (df['mean_trip_time'] <= df['max_trip_time']).all()


def test_week_metrics_positive_services(full_taxi_data):
    assert (full_taxi_data.csv_df['total_services'] > 0).all()


# --- Month metrics ---
def test_month_metrics_not_empty(full_taxi_data):
    assert not full_taxi_data.jfk_df.empty
    assert not full_taxi_data.regular_df.empty
    assert not full_taxi_data.other_df.empty


def test_month_metrics_columns(full_taxi_data):
    expected = ['year_month', 'day_type', 'services', 'distances', 'passengers']
    for df in [full_taxi_data.jfk_df, full_taxi_data.regular_df, full_taxi_data.other_df]:
        assert all(col in df.columns for col in expected)


def test_month_metrics_day_type_values(full_taxi_data):
    for df in [full_taxi_data.jfk_df, full_taxi_data.regular_df, full_taxi_data.other_df]:
        assert set(df['day_type'].unique()).issubset({1, 2})


# --- Export ---
def test_export_csv(full_taxi_data):
    full_taxi_data.export_csv_data()
    assert os.path.exists('processed_data.csv')
    exported = pd.read_csv('processed_data.csv', sep='|')
    assert len(exported) == len(full_taxi_data.csv_df)


def test_export_excel(full_taxi_data):
    full_taxi_data.export_excel_data()
    assert os.path.exists('processed_data.xlsx')
    for sheet in ['JFK', 'Regular', 'Others']:
        df = pd.read_excel('processed_data.xlsx', sheet_name=sheet)
        assert not df.empty
