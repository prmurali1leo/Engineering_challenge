import numpy as np

from utils import init_loggers, time_conversion, fill_values, get_day_night, add_fk, display_output
from generate_surrogate_keys  import get_site_info, get_geo_location
from write_to_disk import *


def run():
    """ function  to run the processing """
    # ./output_data/weather_fact

    init_loggers()
    logger.info("Start of processing")

    for dt in ["20160201", "20160301"]:
        fact_dim_tbl(dt)
    display_output()

    logger.info("End of processing, bye..")


def fact_dim_tbl(dt):
    """ function  to create fact table, arg: dt -> input """
    logger.info("creating facts & dimensions")

    try:
        data = pd.read_csv(f"./input_artifacts/weather.{dt}.csv")

        pd_geo_location_dim = get_geo_location(data)
        write_geo_dim(pd_geo_location_dim)
        pd_site_info_dim = get_site_info(data)
        write_site_dim(pd_site_info_dim)

        pd_weather_fact = get_weather_fact(data)
        fact_df, agg_df = perform_transformations(pd_weather_fact)
        save_facts(fact_df, dt)
        save_agg(agg_df, dt)

    except Exception as ex:
        logger.error("Error occurred .." + str(ex))
        exit()
    else:
        logger.info("Success..")


def get_weather_fact(data):
    """ function to read the source and transform as required, input var = data """

    # replace -99 with np.nan
    # strip down the site it found at the end of site name
    # site name is transformed to categorical data type to save space,
    # after the sting handling is completed in early steps
    logger.info("preparing fact table information")
    try:
        wh_fact = data.copy()
        wh_fact.loc[wh_fact['ScreenTemperature'] == -99, 'ScreenTemperature'] = np.nan
        wh_fact['SiteName'] = wh_fact.apply(
            lambda row: row.SiteName[:-1 * (len(str(row.ForecastSiteCode)) + 3)],
            axis=1)
        group_country = wh_fact.groupby('Region')['Country']
        wh_fact.loc[:, 'Country'] = group_country.transform(fill_values)
        wh_fact = wh_fact.sort_values(['ForecastSiteCode', 'ObservationDate', 'ObservationTime'],
                                      ascending=(True, True, True))
        wh_fact.loc[wh_fact['ScreenTemperature'].isnull(), 'ScreenTemperature'] = (
                (wh_fact['ScreenTemperature'].shift() +
                 wh_fact['ScreenTemperature'].shift(-1)) / 2
        )
        # too many write will be if logged in the get_day_nigh, one info to trace the flow
        logger.info("About to call wh_fact['day_night']->get_day_night formatting as D or N")

        wh_fact['day_night'] = wh_fact['ObservationTime'].apply(lambda row: get_day_night(row))
        logger.info("End of call to wh_fact['day_night']->get_day_night formatting as D or N")

        wh_fact['avg_temp'] = wh_fact.fillna(0).groupby(['ForecastSiteCode', 'ObservationDate', 'day_night'])[
            'ScreenTemperature'].transform('mean')
        wh_fact['count_temp'] = wh_fact.groupby(['ForecastSiteCode', 'ObservationDate', 'day_night'])[
            'ScreenTemperature'].transform('count')
        wh_fact['avg_temp'] = np.where(wh_fact.count_temp == 0, np.nan, wh_fact.avg_temp)

        wh_fact['ObservationDate'] = wh_fact.ObservationDate.str[:-9]

        # too many write will be if logged in the get_day_nigh, one info to trace the flow
        logger.info("About to call wh_fact['ObservationTime']->time_conversion")

        wh_fact['ObservationTime'] = wh_fact['ObservationTime'].apply(lambda x: time_conversion(x))
        logger.info("End of call to wh_fact['ObservationTime']->time_conversion")


    except Exception as ex:
        logger.error("Error occurred .." + str(ex))
        exit()
    else:
        logger.info("Success")
        return wh_fact


def perform_transformations(pdf):
    """function to do transformation of the fact table that inserts the foreign key"""

    logger.info("starting to process fact table critical transformation..")
    try:
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
            df[['ObservationDate', "ObservationTime", 'avg_temp', "ScreenTemperature", 'fk_location_id',
                'fk_site_id', ]][
                df['day_night'] == "D"].copy()
        )

        agg_df = (agg_df.groupby(['fk_location_id', 'fk_site_id', "ObservationDate", 'avg_temp'],
                                 as_index=False).apply(
            lambda x: dict(zip(x['ObservationTime'], x['ScreenTemperature']))).reset_index(name='Temperature'))
        df = df.drop(
            columns=["Region", "Country", "ForecastSiteCode", "SiteName", "avg_temp", "Latitude", "Longitude",
                     "day_night"])
        df = df.sort_values(['ObservationDate', 'ObservationTime'], ascending=(True, True))
    except Exception as ex:
        logger.error("Error occurred .." + str(ex))
        exit()
    else:
        return df, agg_df
        logger.info("Success..")


