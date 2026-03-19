import pandas as pd
import numpy as np
import time
from concurrent.futures import ThreadPoolExecutor

REQUIRED_COLUMNS = ['tpep_pickup_datetime', 'tpep_dropoff_datetime', 'passenger_count',
                    'trip_distance', 'RatecodeID', 'total_amount']


class YellowTaxiData:
    def __init__(self, start_date, end_date):
        self.start_date = start_date
        self.end_date = end_date
        self.dates_list = pd.date_range(self.start_date, self.end_date, freq='MS').strftime("%Y-%m").tolist()
        self.end_date_weeks = pd.date_range(start=self.start_date, end=self.end_date, freq='W-SUN')
        self.urls_list = [
            'https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{dt}.parquet'.format(dt=dt)
            for dt in self.dates_list
        ]
        self.data = pd.DataFrame()
        self.weeks_ranges = pd.DataFrame()
        self.months_ranges = pd.DataFrame()
        self.jfk_df = pd.DataFrame()
        self.regular_df = pd.DataFrame()
        self.other_df = pd.DataFrame()
        self.csv_df = pd.DataFrame()

    def import_data(self):
        def _read(url):
            return pd.read_parquet(path=url, engine='pyarrow', columns=REQUIRED_COLUMNS)

        with ThreadPoolExecutor() as pool:
            dataframes_list = list(pool.map(_read, self.urls_list))

        self.data = pd.concat(dataframes_list, ignore_index=True)

    def clean_data(self):
        self.data.drop_duplicates(inplace=True)
        self.data.dropna(subset=['tpep_pickup_datetime', 'tpep_dropoff_datetime', 'passenger_count'], inplace=True)

        duration = (self.data['tpep_dropoff_datetime'] - self.data['tpep_pickup_datetime']).dt.total_seconds()
        speed = self.data['trip_distance'] / (duration / 3600)

        mask = (
            (self.data['tpep_pickup_datetime'] >= self.start_date) &
            (self.data['tpep_dropoff_datetime'] <= self.end_date) &
            (duration > 0) &
            (duration >= 60) &
            (speed <= 100) &
            (self.data['trip_distance'] > 0) &
            (self.data['total_amount'] > 0) &
            (self.data['total_amount'] <= 5000) &
            (self.data['passenger_count'] > 0)
        )

        self.data = self.data.loc[mask].copy()
        self.data['trip_time_in_seconds'] = (self.data['tpep_dropoff_datetime'] - self.data['tpep_pickup_datetime']).dt.total_seconds()
        self.data['RatecodeID'] = self.data['RatecodeID'].astype(int)

    def add_more_columns(self):
        dt = self.data['tpep_dropoff_datetime']
        self.data['year_month'] = dt.dt.to_period('M')
        iso = dt.dt.isocalendar()
        year_arr = iso.year.values
        week_arr = iso.week.values
        self.data['year_week'] = np.char.add(
            np.char.add(year_arr.astype('U4'), np.full(len(year_arr), '-', dtype='U1')),
            np.char.zfill(week_arr.astype('U2'), 2)
        )
        self.data['year_month_day'] = dt.dt.date

    def generate_week_metrics(self):
        self.csv_df = self.data.groupby('year_week').agg(
            min_trip_time=('trip_time_in_seconds', 'min'),
            max_trip_time=('trip_time_in_seconds', 'max'),
            mean_trip_time=('trip_time_in_seconds', 'mean'),
            min_trip_distance=('trip_distance', 'min'),
            max_trip_distance=('trip_distance', 'max'),
            mean_trip_distance=('trip_distance', 'mean'),
            min_trip_amount=('total_amount', 'min'),
            max_trip_amount=('total_amount', 'max'),
            mean_trip_amount=('total_amount', 'mean'),
            total_services=('total_amount', 'count')
        ).reset_index()

        self.csv_df['percentage_variation'] = (
            self.csv_df['total_services'] - self.csv_df['total_services'].shift(1)
        ) / self.csv_df['total_services'].shift(1) * 100

    def generate_month_metrics(self):
        self.data['day_type'] = np.where(self.data['tpep_dropoff_datetime'].dt.dayofweek >= 5, 2, 1)

        conditions = [
            self.data['RatecodeID'] == 1,
            self.data['RatecodeID'] == 2,
        ]
        choices = ['regular', 'jfk']
        self.data['rate_category'] = np.select(conditions, choices, default='other')

        grouped = self.data.groupby(['rate_category', 'year_month', 'day_type']).agg(
            services=('trip_distance', 'count'),
            distances=('trip_distance', 'sum'),
            passengers=('passenger_count', 'sum')
        ).reset_index()

        self.regular_df = grouped[grouped['rate_category'] == 'regular'].drop(columns='rate_category')
        self.jfk_df = grouped[grouped['rate_category'] == 'jfk'].drop(columns='rate_category')
        self.other_df = grouped[grouped['rate_category'] == 'other'].drop(columns='rate_category')

    def format_data(self):
        self.csv_df = self.csv_df.round(2)
        self.jfk_df = self.jfk_df.reset_index(drop=True)
        self.regular_df = self.regular_df.reset_index(drop=True)
        self.other_df = self.other_df.reset_index(drop=True)

    def export_csv_data(self):
        self.csv_df.to_csv('processed_data_optimized.csv', sep='|', index=False)

    def export_excel_data(self):
        common_columns = ['year_month', 'day_type', 'services', 'distances', 'passengers']
        with pd.ExcelWriter("processed_data_optimized.xlsx", engine="openpyxl") as writer:
            self.jfk_df[common_columns].to_excel(writer, sheet_name="JFK", index=False)
            self.regular_df[common_columns].to_excel(writer, sheet_name="Regular", index=False)
            self.other_df[common_columns].to_excel(writer, sheet_name="Others", index=False)

    def export_data(self):
        self.export_csv_data()
        self.export_excel_data()


if __name__ == '__main__':
    global_start_time = time.perf_counter()

    print('Init objects ...')
    start_time = time.perf_counter()
    yellow_taxi_data = YellowTaxiData(start_date='2022-01-01', end_date='2022-03-31')
    print("*** {t} seconds ***".format(t=time.perf_counter() - start_time))

    print('Importing data ...')
    start_time = time.perf_counter()
    yellow_taxi_data.import_data()
    print("*** {t} seconds ***".format(t=time.perf_counter() - start_time))

    print('Cleaning data ...')
    start_time = time.perf_counter()
    yellow_taxi_data.clean_data()
    print("*** {t} seconds ***".format(t=time.perf_counter() - start_time))

    print('Adding more columns ...')
    start_time = time.perf_counter()
    yellow_taxi_data.add_more_columns()
    print("*** {t} seconds ***".format(t=time.perf_counter() - start_time))

    print('Generating week metrics ...')
    start_time = time.perf_counter()
    yellow_taxi_data.generate_week_metrics()
    print("*** {t} seconds ***".format(t=time.perf_counter() - start_time))

    print('Generating month metrics ...')
    start_time = time.perf_counter()
    yellow_taxi_data.generate_month_metrics()
    print("*** {t} seconds ***".format(t=time.perf_counter() - start_time))

    print('Formatting results ...')
    start_time = time.perf_counter()
    yellow_taxi_data.format_data()
    print("*** {t} seconds ***".format(t=time.perf_counter() - start_time))

    print('Exporting results ...')
    start_time = time.perf_counter()
    yellow_taxi_data.export_data()
    print("*** {t} seconds ***".format(t=time.perf_counter() - start_time))

    print("Execution time: {t} seconds".format(t=time.perf_counter() - global_start_time))
