import pandas as pd
import numpy as np
import time

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
        dataframes_list = [
            pd.read_parquet(
                path=url,
                engine='pyarrow'
            ) for url in self.urls_list
        ]

        self.data = pd.concat(dataframes_list, ignore_index=True)
        self.data = self.data[['tpep_pickup_datetime', 'tpep_dropoff_datetime', 'passenger_count', 'trip_distance',
                               'RatecodeID','total_amount']] # Filter columns, only necessary columns
        self.data.set_index(['tpep_pickup_datetime', 'tpep_dropoff_datetime', 'RatecodeID'],
                            inplace=True, drop=False)


    def clean_data(self):
        self.data.drop_duplicates(inplace=True)
        self.data.dropna(subset=['tpep_pickup_datetime', 'tpep_dropoff_datetime', 'passenger_count'], inplace=True)

        self.data = self.data[
            (self.data['tpep_pickup_datetime'] >= self.start_date) &
            (self.data['tpep_dropoff_datetime'] <= self.end_date)
        ]

        self.data = self.data[self.data['tpep_dropoff_datetime'] > self.data['tpep_pickup_datetime']]

        self.data = self.data[
            (self.data['tpep_dropoff_datetime'] - self.data['tpep_pickup_datetime']).dt.total_seconds() >= 60
        ]

        #100mph = 160km/h
        self.data = self.data[
            (self.data['trip_distance']) /
            ((self.data['tpep_dropoff_datetime'] - self.data['tpep_pickup_datetime']).dt.total_seconds() / 3600) <= 100
        ]

        self.data = self.data[self.data['trip_distance'] > 0]
        self.data = self.data[(self.data['total_amount'] > 0) & (self.data['total_amount'] <= 5000)]

        self.data = self.data[self.data['passenger_count'] > 0]


    def add_more_columns(self):
        self.data['year_month'] = self.data['tpep_dropoff_datetime'].dt.strftime('%Y-%m')
        self.data['year_dt'] = self.data['tpep_dropoff_datetime'].dt.year.astype(str)
        self.data['week_dt'] = self.data['tpep_dropoff_datetime'].dt.isocalendar().week.astype(str).str.zfill(3)
        self.data['year_week'] = self.data['year_dt'].str.cat(self.data['week_dt'], sep='-')
        self.data['year_month_day'] = self.data['tpep_dropoff_datetime'].dt.strftime('%Y-%m-%d')


    def generate_week_metrics(self):
        self.data['trip_time'] = self.data['tpep_dropoff_datetime'] - self.data['tpep_pickup_datetime']
        self.data['trip_time_in_seconds'] = self.data['trip_time'].dt.total_seconds()

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
        rate_code_id_dict = {
            'regular_df': 1,
            'jfk_df': 2,
            'other_df': -1
        }

        self.data['day_type'] = np.where(self.data['tpep_dropoff_datetime'].dt.dayofweek >= 5, 2, 1)
        self.data['RatecodeID'] = self.data['RatecodeID'].astype(int)

        for rc_id in rate_code_id_dict.keys():
            attr = getattr(self, rc_id)

            if rate_code_id_dict[rc_id] in [1, 2]:
                df = self.data[self.data['RatecodeID'] == rate_code_id_dict[rc_id]]
            else:
                df = self.data[(self.data['RatecodeID'] != 1) & (self.data['RatecodeID'] != 2)]

            df = df[['year_month', 'day_type', 'trip_distance', 'passenger_count']]

            df = df.groupby(['year_month', 'day_type']).agg(
                services=('trip_distance', 'count'),
                distances=('trip_distance', 'sum'),
                passengers=('passenger_count', 'sum')
            ).reset_index()

            attr = pd.concat([attr, df])
            setattr(self, rc_id, attr)


    def format_data(self):
        self.csv_df = self.csv_df.round(2)

        self.jfk_df = self.jfk_df.reset_index()
        self.regular_df = self.regular_df.reset_index()
        self.other_df = self.other_df.reset_index()


    def export_csv_data(self):
        self.csv_df.to_csv('processed_data.csv', sep='|', index=False)


    def export_excel_data(self):
        common_columns = ['year_month', 'day_type', 'services', 'distances', 'passengers']
        with pd.ExcelWriter("processed_data.xlsx", engine="openpyxl") as writer:
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





