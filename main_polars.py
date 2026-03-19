import polars as pl
import time
from concurrent.futures import ThreadPoolExecutor

REQUIRED_COLUMNS = ['tpep_pickup_datetime', 'tpep_dropoff_datetime', 'passenger_count',
                    'trip_distance', 'RatecodeID', 'total_amount']


class YellowTaxiData:
    def __init__(self, start_date, end_date):
        self.start_date = start_date
        self.end_date = end_date
        self.dates_list = pl.date_range(
            pl.Series([start_date]).cast(pl.Date).item(),
            pl.Series([end_date]).cast(pl.Date).item(),
            interval='1mo', eager=True
        ).to_list()
        self.urls_list = [
            'https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{y}-{m}.parquet'.format(
                y=dt.year, m=str(dt.month).zfill(2))
            for dt in self.dates_list
        ]
        self.data = pl.DataFrame()
        self.jfk_df = pl.DataFrame()
        self.regular_df = pl.DataFrame()
        self.other_df = pl.DataFrame()
        self.csv_df = pl.DataFrame()

    def import_data(self):
        def _read(url):
            import pandas as pd
            df = pd.read_parquet(path=url, engine='pyarrow', columns=REQUIRED_COLUMNS)
            return pl.from_pandas(df)

        with ThreadPoolExecutor() as pool:
            dataframes_list = list(pool.map(_read, self.urls_list))

        self.data = pl.concat(dataframes_list)

    def clean_data(self):
        self.data = self.data.unique()
        self.data = self.data.drop_nulls(subset=['tpep_pickup_datetime', 'tpep_dropoff_datetime', 'passenger_count'])

        self.data = self.data.with_columns(
            ((pl.col('tpep_dropoff_datetime') - pl.col('tpep_pickup_datetime')).dt.total_seconds()).alias('trip_time_in_seconds'),
            pl.col('RatecodeID').cast(pl.Int32),
        ).filter(
            (pl.col('tpep_pickup_datetime') >= pl.lit(self.start_date).str.to_datetime('%Y-%m-%d')) &
            (pl.col('tpep_dropoff_datetime') <= pl.lit(self.end_date).str.to_datetime('%Y-%m-%d')) &
            (pl.col('trip_time_in_seconds') >= 60) &
            ((pl.col('trip_distance') / (pl.col('trip_time_in_seconds') / 3600)) <= 100) &
            (pl.col('trip_distance') > 0) &
            (pl.col('total_amount') > 0) &
            (pl.col('total_amount') <= 5000) &
            (pl.col('passenger_count') > 0)
        )

    def add_more_columns(self):
        self.data = self.data.with_columns(
            pl.col('tpep_dropoff_datetime').dt.strftime('%Y-%m').alias('year_month'),
            (pl.col('tpep_dropoff_datetime').dt.iso_year().cast(pl.Utf8) + '-' +
             pl.col('tpep_dropoff_datetime').dt.week().cast(pl.Utf8).str.pad_start(2, '0')).alias('year_week'),
            pl.col('tpep_dropoff_datetime').dt.date().cast(pl.Utf8).alias('year_month_day'),
        )

    def generate_week_metrics(self):
        self.csv_df = self.data.group_by('year_week').agg(
            pl.col('trip_time_in_seconds').min().alias('min_trip_time'),
            pl.col('trip_time_in_seconds').max().alias('max_trip_time'),
            pl.col('trip_time_in_seconds').mean().alias('mean_trip_time'),
            pl.col('trip_distance').min().alias('min_trip_distance'),
            pl.col('trip_distance').max().alias('max_trip_distance'),
            pl.col('trip_distance').mean().alias('mean_trip_distance'),
            pl.col('total_amount').min().alias('min_trip_amount'),
            pl.col('total_amount').max().alias('max_trip_amount'),
            pl.col('total_amount').mean().alias('mean_trip_amount'),
            pl.col('total_amount').count().alias('total_services'),
        ).sort('year_week')

        self.csv_df = self.csv_df.with_columns(
            ((pl.col('total_services').cast(pl.Float64) - pl.col('total_services').shift(1).cast(pl.Float64)) /
             pl.col('total_services').shift(1).cast(pl.Float64) * 100).alias('percentage_variation')
        )

    def generate_month_metrics(self):
        data = self.data.with_columns(
            pl.when(pl.col('tpep_dropoff_datetime').dt.weekday() >= 6)
            .then(pl.lit(2)).otherwise(pl.lit(1)).alias('day_type'),
            pl.when(pl.col('RatecodeID') == 1).then(pl.lit('regular'))
            .when(pl.col('RatecodeID') == 2).then(pl.lit('jfk'))
            .otherwise(pl.lit('other')).alias('rate_category'),
        )

        grouped = data.group_by(['rate_category', 'year_month', 'day_type']).agg(
            pl.col('trip_distance').count().alias('services'),
            pl.col('trip_distance').sum().alias('distances'),
            pl.col('passenger_count').sum().alias('passengers'),
        ).sort(['rate_category', 'year_month', 'day_type'])

        self.regular_df = grouped.filter(pl.col('rate_category') == 'regular').drop('rate_category')
        self.jfk_df = grouped.filter(pl.col('rate_category') == 'jfk').drop('rate_category')
        self.other_df = grouped.filter(pl.col('rate_category') == 'other').drop('rate_category')

    def format_data(self):
        # Round numeric columns in csv_df
        numeric_cols = [c for c in self.csv_df.columns if self.csv_df[c].dtype in (pl.Float64, pl.Float32)]
        self.csv_df = self.csv_df.with_columns([pl.col(c).round(2) for c in numeric_cols])

    def export_csv_data(self):
        self.csv_df.write_csv('processed_data_polars.csv', separator='|')

    def export_excel_data(self):
        import pandas as pd
        common_columns = ['year_month', 'day_type', 'services', 'distances', 'passengers']
        with pd.ExcelWriter("processed_data_polars.xlsx", engine="openpyxl") as writer:
            self.jfk_df.select(common_columns).to_pandas().to_excel(writer, sheet_name="JFK", index=False)
            self.regular_df.select(common_columns).to_pandas().to_excel(writer, sheet_name="Regular", index=False)
            self.other_df.select(common_columns).to_pandas().to_excel(writer, sheet_name="Others", index=False)

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
