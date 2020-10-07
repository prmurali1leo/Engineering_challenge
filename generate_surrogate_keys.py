import pandas as pd

from hashlib import md5
from loguru import logger


def get_geo_location(data):
    """ function  to create geo location with uniqe key , data: dt -> input """
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
    logger.info("getting required site info from the source file..")
    try:
        site_info = data[['ForecastSiteCode', 'SiteName', 'Latitude', 'Longitude']].copy()
        site_info = site_info.drop_duplicates(subset=['ForecastSiteCode', 'SiteName', 'Latitude', 'Longitude'])
        site_info['SiteName'] = site_info.apply(lambda row: row.SiteName[:-1 * (len(str(row.ForecastSiteCode)) + 3)],
                                                axis=1)
        site_info['site_id'] = (site_info
                                .apply(lambda row: str(int(md5(str(row.ForecastSiteCode)
                                                               .encode('utf-8')).hexdigest(), 16)), axis=1)
                                )
        site_info = site_info.sort_values('ForecastSiteCode').reset_index(drop=True)

    except Exception as ex:
        logger.error("Error occurred .." + str(ex))
        exit()
    else:
        logger.info("Success..")
        return site_info
