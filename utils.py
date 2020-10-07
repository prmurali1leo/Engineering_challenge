import datetime
import pandas as pd

from loguru import logger
from cls_utils.dim_surr_rslvr import Cls_Dim_Surr


def init_loggers():
    """ function  to run initialize loggers error/info """
    # add loggers

    logger.add("./log/log.err",
               rotation="1 MB",
               retention="10 days",
               level=40  # 20= INFO, 40=ERROR, 30=WARNING
               )
    logger.add("./log/log.info",
               rotation="1 MB",
               retention="10 days",
               level=20  # 20= INFO, 40=ERROR, 30=WARNING
               )


def add_fk(df):
    """function to insert the foreign key"""

    logger.info("starting core function to add foreign key..")

    try:
        df = Cls_Dim_Surr.add_fk(
            "geo_location", df, "fk_location_id", {'Region': 'Region', 'Country': 'Country'},
        )
        df = Cls_Dim_Surr.add_fk(
            "site", df, "fk_site_id", {'ForecastSiteCode': 'ForecastSiteCode'},
        )
    except Exception as ex:
        logger.error("Error occurred ..hh" + str(ex))
        exit()
    else:
        logger.info("Success..")
    return df


def time_conversion(t):
    """ function  to format datetime, arg: t -> input """
    try:
        return datetime.datetime.strptime(str(t), '%H').strftime("%H:%M")
    except Exception as ex:
        logger.error("Error occurred.." + str(ex))
        exit()


def fill_values(series):
    """ function  to fill values, arg: series -> input """
    try:
        values_counted = series.value_counts()
        if values_counted.empty:
            return series
        most_frequent = values_counted.index[0]
        new_country = series.fillna(most_frequent)
    except Exception as ex:
        logger.error("Error occurred.." + str(ex))
        exit()
    else:
        return new_country


def get_day_night(time):
    """ function  to designate day/night, arg: time -> input """
    try:
        if 8 <= time <= 16:
            return 'D'
        else:
            return 'N'
    except Exception as ex:
        logger.error("Error occurred.." + str(ex))
        exit()


def display_output():
    """ function to display output """
    logger.info("Displaying the final output...")
    try:

        # ds = pq.ParquetDataset("./output_data/weather_fact/dt=20160201/fact_aggregate.parquet")
        # table = ds.read()
        # df = table.to_pandas()

        df = pd.read_parquet("output_data/fact_aggregate.parquet", engine="pyarrow")
        geo_dim = pd.read_parquet("output_data/geo_dimension.parquet", engine="pyarrow")
        site_dim = pd.read_parquet("output_data/site_dimension.parquet", engine="pyarrow")
        df = df[df.avg_temp == df.avg_temp.max()]
        df1 = pd.merge(df, geo_dim, left_on="fk_location_id", right_on="location_id", how='left')
        df1 = pd.merge(df1, site_dim, left_on="fk_site_id", right_on="site_id", how='left')
        df1 = df1.drop(columns=["fk_location_id", "location_id", "fk_site_id", "site_id"])
        df1.to_csv("output_data/final_output.csv", index=False)
        print("======================================================================================")
        print("======================================================================================")

        print("Highest temperature was recorded on {0}".format(df1['ObservationDate']))
        print(df1['ObservationDate'])

        print("The average temperature on that Day from 8am to 4pm was {0}".format(df1['avg_temp']))
        print("The temperature from 8am to 4pm was {}".format(df1['Temperature']))
        print("The hottest region was {0}".format(df1['Region']))

        print("======================================================================================")
        print("======================================================================================")

    except Exception as ex:
        logger.error("Error occurred .." + str(ex))
        exit()
    else:
        logger.info("Success..")
