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


    def generate_weeks(self):
        df = pd.DataFrame(self.end_date_weeks, columns=['end_week'])
        df['start_week'] = df['end_week'] - pd.to_timedelta(df['end_week'].dt.weekday, unit='D')

        if df.loc[0]['start_week'] != self.start_date: # Remove week if start_date is the first day of week
            df.drop(index=0, inplace=True)
            df.reset_index(drop=True, inplace=True)

        """if df['end_week'].iloc[-1] < pd.to_datetime(self.end_date):
            df = pd.concat([
                df,
                pd.DataFrame({
                    'start_week': [
                        pd.to_datetime(self.end_date) - pd.to_timedelta(pd.to_datetime(self.end_date).weekday(),
                                                                            unit='D')],
                    'end_week': [pd.to_datetime(self.end_date)]
                })
            ], ignore_index=True)"""

        self.weeks_ranges = df.copy()
        self.weeks_ranges = self.weeks_ranges[['start_week', 'end_week']]

        self.weeks_ranges['num_week'] = self.weeks_ranges['end_week'].dt.isocalendar().week.astype(str).str.zfill(3)


    def generate_months(self):
        date_range = pd.date_range(self.start_date, self.end_date, freq='MS')
        dates_df = pd.DataFrame(date_range, columns=['date_dt'])

        dates_df['start_day'] = dates_df['date_dt']
        dates_df['end_day'] = dates_df['date_dt'] + pd.offsets.MonthEnd(0)
        dates_df['date_dt'] = dates_df['date_dt'].dt.strftime('%Y-%m')

        self.months_ranges = dates_df


    def generate_week_metrics(self):
        def get_total_services(row):
            return self.data[(self.data['tpep_dropoff_datetime'] >= row['start_week']) & (
                        self.data['tpep_dropoff_datetime'] <= row['end_week'])].shape[0]

        def get_min_for_field(row, field):
            return self.data[(self.data['tpep_dropoff_datetime'] >= row['start_week']) & (
                    self.data['tpep_dropoff_datetime'] <= row['end_week'])][field].min()

        def get_max_for_field(row, field):
            return self.data[(self.data['tpep_dropoff_datetime'] >= row['start_week']) & (
                    self.data['tpep_dropoff_datetime'] <= row['end_week'])][field].max()

        def get_mean_for_field(row, field):
            return self.data[(self.data['tpep_dropoff_datetime'] >= row['start_week']) & (
                    self.data['tpep_dropoff_datetime'] <= row['end_week'])][field].mean()

        self.data['trip_time'] = self.data['tpep_dropoff_datetime'] - self.data['tpep_pickup_datetime']
        self.data['trip_time_in_seconds'] = self.data['trip_time'].dt.total_seconds()

        # TODO: Change apply methods
        self.weeks_ranges['min_trip_time'] = self.weeks_ranges.apply(
            lambda x: get_min_for_field(x, 'trip_time_in_seconds'), axis=1
        )
        self.weeks_ranges['max_trip_time'] = self.weeks_ranges.apply(
            lambda x: get_max_for_field(x, 'trip_time_in_seconds'), axis=1
        )
        self.weeks_ranges['mean_trip_time'] = self.weeks_ranges.apply(
            lambda x: get_mean_for_field(x, 'trip_time_in_seconds'), axis=1
        )

        self.weeks_ranges['min_trip_distance'] = self.weeks_ranges.apply(
            lambda x: get_min_for_field(x, 'trip_distance'), axis=1
        )
        self.weeks_ranges['max_trip_distance'] = self.weeks_ranges.apply(
            lambda x: get_max_for_field(x, 'trip_distance'), axis=1
        )
        self.weeks_ranges['mean_trip_distance'] = self.weeks_ranges.apply(
            lambda x: get_mean_for_field(x, 'trip_distance'), axis=1
        )

        self.weeks_ranges['min_trip_amount'] = self.weeks_ranges.apply(
            lambda x: get_min_for_field(x, 'total_amount'), axis=1
        )
        self.weeks_ranges['max_trip_amount'] = self.weeks_ranges.apply(
            lambda x: get_max_for_field(x, 'total_amount'), axis=1
        )
        self.weeks_ranges['mean_trip_amount'] = self.weeks_ranges.apply(
            lambda x: get_mean_for_field(x, 'total_amount'), axis=1
        )

        self.weeks_ranges['total_services'] = self.weeks_ranges.apply(lambda x: get_total_services(x), axis=1)

        self.weeks_ranges['percentage_variation'] = (
            self.weeks_ranges['total_services'] - self.weeks_ranges['total_services'].shift(1)
            ) / self.weeks_ranges['total_services'].shift(1) * 100


    def generate_month_metrics(self):
        rate_code_id_dict = {
            'regular_df': 1,
            'jfk_df': 2,
            'other_df': -1
        }
        for rc_id in rate_code_id_dict.keys():
            attr = getattr(self, rc_id)
            for row in self.months_ranges.itertuples(index=False):
                month_df = self.data[(self.data['tpep_pickup_datetime'] >= row.start_day) &
                                     (self.data['tpep_dropoff_datetime'] <= row.end_day)].copy()

                month_df['day_type'] = np.where(month_df['tpep_dropoff_datetime'].dt.dayofweek >= 5, 2, 1)
                month_df['RatecodeID'] = month_df['RatecodeID'].astype(int)

                if rate_code_id_dict[rc_id] in [1, 2]:
                    df = month_df[month_df['RatecodeID'] == rate_code_id_dict[rc_id]]
                else:
                    df = month_df[(month_df['RatecodeID'] != 1) & (month_df['RatecodeID'] != 2)]

                df = df[['day_type', 'trip_distance', 'passenger_count']]
                df['service'] = 1

                df = df.groupby('day_type').agg(
                    services=('service', 'count'),
                    distances=('trip_distance', 'sum'),
                    passengers=('passenger_count', 'sum')
                )

                df['month'] = row.date_dt

                attr = pd.concat([attr, df])
                setattr(self, rc_id, attr)


    def format_data(self):
        self.weeks_ranges = self.weeks_ranges.round(2)
        self.weeks_ranges['year_week'] = self.weeks_ranges['end_week'].dt.isocalendar().year.astype(str) + '-' + \
                                           self.weeks_ranges['num_week']

        self.jfk_df = self.jfk_df.reset_index()
        self.regular_df = self.regular_df.reset_index()
        self.other_df = self.other_df.reset_index()


    def export_csv_data(self):
        ordered_columns = ['year_week', 'min_trip_time', 'max_trip_time','mean_trip_time',
            'min_trip_distance', 'max_trip_distance','mean_trip_distance', 'min_trip_amount', 'max_trip_amount',
            'mean_trip_amount', 'total_services', 'percentage_variation'
       ]
        csv_weeks_ranges = self.weeks_ranges[ordered_columns]
        csv_weeks_ranges.to_csv('processed_data.csv', sep='|', index=False)


    def export_excel_data(self):
        common_columns = ['month', 'day_type', 'services', 'distances', 'passengers']
        with pd.ExcelWriter("processed_data.xlsx", engine="openpyxl") as writer:
            self.jfk_df[common_columns].to_excel(writer, sheet_name="JFK", index=False)
            self.regular_df[common_columns].to_excel(writer, sheet_name="Regular", index=False)
            self.other_df[common_columns].to_excel(writer, sheet_name="Others", index=False)


    def export_data(self):
        self.export_csv_data()
        self.export_excel_data()


if __name__ == '__main__':
    start_time = time.perf_counter()

    yellow_taxi_data = YellowTaxiData(start_date='2022-01-01', end_date='2022-03-31')
    print('Importing data ...')
    yellow_taxi_data.import_data()

    print('Cleaning data ...')
    yellow_taxi_data.clean_data()

    print('Generating months ...')
    yellow_taxi_data.generate_months()

    print('Generating weeks ...')
    yellow_taxi_data.generate_weeks()

    print('Generating week metrics ...')
    yellow_taxi_data.generate_week_metrics()

    print('Generating month metrics ...')
    yellow_taxi_data.generate_month_metrics()

    print('Formatting results ...')
    yellow_taxi_data.format_data()

    print('Exporting results ...')
    yellow_taxi_data.export_data()

    end_time = time.perf_counter()

    print("Execution time: {t} seconds".format(t=end_time - start_time))





