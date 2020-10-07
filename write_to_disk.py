import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from loguru import logger
import os


def write_to_parquet(source, destination):
    """function to write parquet file -> source, destination"""
    logger.info("Writing the data to disk in parquet format")
    try:
        return source.to_parquet(f"./output_data/{destination}.parquet", engine="pyarrow", index=False)
    except Exception as ex:
        logger.error("Error occurred.." + str(ex))
        exit()
    else:
        logger.inf("Success..")


def write_geo_dim(data):
    """function to write geo dimension, one input_artifacts arg passed -> data"""

    logger.info("About to write geo dimension..")

    try:
        if not os.path.exists("./output_data/geo_dimension.parquet"):
            write_to_parquet(data, "geo_dimension")
        else:
            geo_dim = pd.read_parquet("./output_data/geo_dimension.parquet", engine="pyarrow")
            pd_geo_dim = pd.concat([geo_dim, data]).drop_duplicates().reset_index(drop=True)
            write_to_parquet(pd_geo_dim, "geo_dimension")
    except Exception as ex:
        logger.error("Error occurred .." + str(ex))
        exit()
    else:
        logger.info("Success")
        return


def write_site_dim(data):
    logger.info("saving prepared site info...")
    try:
        # file if found drop the duplicates and return
        if os.path.exists(f"output_data/site_dimension.parquet"):
            site_dim = pd.read_parquet("output_data/site_dimension.parquet", engine="pyarrow")
            pd_site_dim = pd.concat([site_dim, data]).drop_duplicates().reset_index(drop=True)
            write_to_parquet(pd_site_dim, "site_dimension")
        else:
            # file if not found, read the file and write to disk
            write_to_parquet(data, "site_dimension")
    except Exception as ex:
        logger.error("Error occurred .." + str(ex))
        exit()
    else:
        logger.info("Success..")
        return


def save_facts(df, dt):
    """function to save the parquet file to for every single day df -> dataframe, dt -> each day"""
    # full dataset is input_artifacts, results are saved to disk for every single day

    logger.info("About to write the results to disk for every single day ..")

    try:
        if not os.path.exists(f"./output_data/weather_fact/dt={dt}"):
            os.makedirs(f"./output_data/weather_fact/dt={dt}")

        unique_dates = df['ObservationDate'].unique().tolist()
        table = pa.Table.from_pandas(df, preserve_index=False)
        with pq.ParquetWriter(f"./output_data/weather_fact/dt={dt}/weather_fact.parquet", table.schema) as writer:
            for date in unique_dates:
                table = pa.Table.from_pandas(df[df['ObservationDate'] == date], preserve_index=False)
                writer.write_table(table)
    except Exception as ex:
        logger.error("Error occurred .." + str(ex))
        exit()
    else:
        logger.info("Success..")


def save_agg(data, dt):
    """ function to save the fact aggregated parquet file to disk """

    logger.info("About to write the aggregated facts ..")

    obs_date = dt[:4] + "-" + dt[4:6]

    try:
        # first time check file if not found, write and then return back
        # /Users/murali_mac/Python_code/DLG_Test/output_data/fact_aggregate.parquet

        if not os.path.exists(f"output_data/fact_aggregate.parquet"):
            write_to_parquet(data, "fact_aggregate")
        else:
            # if file exists already

            agg_fact = pd.read_parquet(f"output_data/fact_aggregate.parquet", engine="pyarrow")
            agg_fact = agg_fact[~agg_fact['ObservationDate'].str.contains(obs_date)]
            pd_agg_fact = pd.concat([agg_fact, data]).reset_index(drop=True)
            write_to_parquet(pd_agg_fact, "fact_aggregate")
    except Exception as ex:
        logger.error("Error occurred .." + str(ex))
        exit()
    else:
        logger.info("Success..")
        return
