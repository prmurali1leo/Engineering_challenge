import pandas as pd
import numpy as np
from hashlib import md5
import datetime
import pyarrow.parquet as pq
import pyarrow as pa
from src.dimension_surrogate_resolver import DimensionSurrogateResolver


def run():
    for dt in ["20160201", "20160301"]:
        create_fact_dimension_tables(dt)
    display_output()


def create_fact_dimension_tables(dt):
    data = pd.read_csv(f"input_data/weather.{dt}.csv")
    pd_geo_location_dim = get_geo_location(data)
    save_geo_dimension(pd_geo_location_dim)
    pd_site_info_dim = get_site_info(data)
    save_site_dimension(pd_site_info_dim)
    pd_weather_fact = get_weather_fact(data)
    fact_df, agg_df = perform_transformations(pd_weather_fact)
    save_fact(fact_df, dt)
    save_aggregate(agg_df, dt)


def get_geo_location(data):
    geo_location = data[['Region', 'Country']].copy()
    geo_location = geo_location.drop_duplicates(subset=['Region', 'Country'])
    geo_location = geo_location[pd.notnull(geo_location['Country'])]
    geo_location['key_column'] = geo_location.apply(lambda row: row.Region + row.Country, axis=1)
    geo_location['location_id'] = (geo_location
                                   .apply(lambda row: str(int(md5(row.key_column.encode('utf-8')).hexdigest(), 16)),
                                          axis=1)
                                   )
    geo_location = geo_location.drop(columns='key_column').reset_index(drop=True)
    return geo_location


def get_site_info(data):
    site_info = data[['ForecastSiteCode', 'SiteName', 'Latitude', 'Longitude']].copy()
    site_info = site_info.drop_duplicates(subset=['ForecastSiteCode', 'SiteName', 'Latitude', 'Longitude'])
    site_info['SiteName'] = site_info.apply(lambda row: row.SiteName[:-1 * (len(str(row.ForecastSiteCode)) + 3)],
                                            axis=1)
    site_info['site_id'] = (site_info
                            .apply(lambda row: str(int(md5(str(row.ForecastSiteCode)
                                                           .encode('utf-8')).hexdigest(), 16)), axis=1)
                            )
    site_info = site_info.sort_values('ForecastSiteCode').reset_index(drop=True)
    return site_info


def time_conversion(t):
    return datetime.datetime.strptime(str(t), '%H').strftime("%H:%M")


def fill_values(series):
    values_counted = series.value_counts()
    if values_counted.empty:
        return series
    most_frequent = values_counted.index[0]
    new_country = series.fillna(most_frequent)
    return new_country


def get_day_night(time):
    if 8 <= time <= 16:
        return 'D'
    else:
        return 'N'


def get_weather_fact(data):
    weather_fact = data.copy()
    weather_fact.loc[weather_fact['ScreenTemperature'] == -99, 'ScreenTemperature'] = np.nan
    weather_fact['SiteName'] = weather_fact.apply(lambda row: row.SiteName[:-1 * (len(str(row.ForecastSiteCode)) + 3)],
                                                  axis=1)
    group_country = weather_fact.groupby('Region')['Country']
    weather_fact.loc[:, 'Country'] = group_country.transform(fill_values)
    weather_fact = weather_fact.sort_values(['ForecastSiteCode', 'ObservationDate', 'ObservationTime'],
                                            ascending=(True, True, True))
    weather_fact.loc[weather_fact['ScreenTemperature'].isnull(), 'ScreenTemperature'] = (
            (weather_fact['ScreenTemperature'].shift() +
             weather_fact['ScreenTemperature'].shift(-1)) / 2
    )
    weather_fact['day_night'] = weather_fact['ObservationTime'].apply(lambda row: get_day_night(row))
    weather_fact['avg_temp'] = weather_fact.fillna(0).groupby(['ForecastSiteCode', 'ObservationDate', 'day_night'])[
        'ScreenTemperature'].transform('mean')
    weather_fact['count_temp'] = weather_fact.groupby(['ForecastSiteCode', 'ObservationDate', 'day_night'])[
        'ScreenTemperature'].transform('count')
    weather_fact['avg_temp'] = np.where(weather_fact.count_temp == 0, np.nan, weather_fact.avg_temp)

    weather_fact['ObservationDate'] = weather_fact.ObservationDate.str[:-9]
    weather_fact['ObservationTime'] = weather_fact['ObservationTime'].apply(lambda x: time_conversion(x))
    return weather_fact


def write_to_parquet(source, destination):
    return source.to_parquet(f"output_data/{destination}.parquet", engine="pyarrow", index=False)


def save_geo_dimension(data):
    try:
        geo_dim = pd.read_parquet("output_data/geo_dimension.parquet", engine="pyarrow")
    except OSError:
        write_to_parquet(data, "geo_dimension")
        return
    pd_geo_dim = pd.concat([geo_dim, data]).drop_duplicates().reset_index(drop=True)
    write_to_parquet(pd_geo_dim, "geo_dimension")
    return


def save_site_dimension(data):
    try:
        site_dim = pd.read_parquet("output_data/site_dimension.parquet", engine="pyarrow")
    except OSError:
        write_to_parquet(data, "site_dimension")
        return
    pd_site_dim = pd.concat([site_dim, data]).drop_duplicates().reset_index(drop=True)
    write_to_parquet(pd_site_dim, "site_dimension")
    return


def perform_transformations(pdf):
    columns_to_keep = ["ObservationDate",
                       "ObservationTime",
                       "WindDirection",
                       "WindSpeed",
                       "WindGust",
                       "Visibility",
                       "ScreenTemperature",
                       "Pressure",
                       "SignificantWeatherCode",
                       "ForecastSiteCode",
                       "Region",
                       "Country",
                       "avg_temp",
                       "day_night"]
    df = pdf[columns_to_keep].copy()
    df = add_fk(df)
    agg_df = (
        df[['ObservationDate', "ObservationTime", 'avg_temp', "ScreenTemperature", 'fk_location_id', 'fk_site_id', ]][
            df['day_night'] == "D"].copy()
    )

    agg_df = (agg_df.groupby(['fk_location_id', 'fk_site_id', "ObservationDate", 'avg_temp'],
                             as_index=False).apply(
        lambda x: dict(zip(x['ObservationTime'], x['ScreenTemperature']))).reset_index(name='Temperature'))
    df = df.drop(
        columns=["Region", "Country", "ForecastSiteCode", "SiteName", "avg_temp", "Latitude", "Longitude", "day_night"])
    df = df.sort_values(['ObservationDate', 'ObservationTime'], ascending=(True, True))

    return df, agg_df


def add_fk(df):
    df = DimensionSurrogateResolver.add_fk(
        "geo_location", df, "fk_location_id", {'Region': 'Region', 'Country': 'Country'},
    )
    df = DimensionSurrogateResolver.add_fk(
        "site", df, "fk_site_id", {'ForecastSiteCode': 'ForecastSiteCode'},
    )
    return df


def save_fact(df, dt):
    unique_dates = df['ObservationDate'].unique().tolist()
    print(unique_dates)
    table = pa.Table.from_pandas(df, preserve_index=False)
    with pq.ParquetWriter(f"output_data/weather_fact/dt={dt}/weather_fact.parquet", table.schema) as writer:
        for date in unique_dates:
            df1 = df[df['ObservationDate'] == date]
            table = pa.Table.from_pandas(df1, preserve_index=False)
            writer.write_table(table)


def save_aggregate(data, dt):
    obs_date = dt[:4] + "-" + dt[4:6]

    try:
        agg_fact = pd.read_parquet("output_data/fact_aggregate.parquet", engine="pyarrow")
        agg_fact = agg_fact[~agg_fact['ObservationDate'].str.contains(obs_date)]
    except OSError:
        write_to_parquet(data, "fact_aggregate")
        return
    pd_agg_fact = pd.concat([agg_fact, data]).reset_index(drop=True)
    write_to_parquet(pd_agg_fact, "fact_aggregate")
    return


def display_output():
    df = pd.read_parquet("output_data/fact_aggregate.parquet", engine="pyarrow")
    geo_dim = pd.read_parquet("output_data/geo_dimension.parquet", engine="pyarrow")
    site_dim = pd.read_parquet("output_data/site_dimension.parquet", engine="pyarrow")
    df = df[df.avg_temp == df.avg_temp.max()]
    df1 = pd.merge(df, geo_dim, left_on="fk_location_id", right_on="location_id", how='left')
    df1 = pd.merge(df1, site_dim, left_on="fk_site_id", right_on="site_id", how='left')
    df1 = df1.drop(columns=["fk_location_id", "location_id", "fk_site_id", "site_id"])
    df1.to_csv("output_data/final_output.csv", index=False)
    print("Highest temperature was recorded on {0}".format(df1['ObservationDate']))
    print("The average temperature on that Day from 8am to 4pm was {0}".format(df1['avg_temp']))
    print("The temperature from 8am to 4pm was {0}".format(df1['Temperature']))
    print("The hottest region was {0}".format(df1['Region']))
