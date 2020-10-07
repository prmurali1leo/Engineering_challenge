from typing import Dict
from cls_utils.geo_location_surrogate import GeoLocationSurrogate
from cls_utils.site_surrogate import SiteSurrogate


class InvalidDimensionNameException(ValueError):
    pass


class Cls_Dim_Surr:
    @staticmethod
    def add_fk(
            dimension_name: str,
            df,
            fk_name: str,
            col_names: Dict[str, str],
    ):

        dimension_map = {
            "geo_location": GeoLocationSurrogate(),
            "site": SiteSurrogate(),
        }

        dimension = dimension_map.get(dimension_name, None)
        if dimension is not None:
            return dimension.add_fk(
                df, fk_name, col_names
            )
        else:
            raise InvalidDimensionNameException(
                f"The dimension {dimension_name} is not found"
            )
