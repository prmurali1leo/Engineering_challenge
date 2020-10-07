from typing import Dict
from cls_utils.dimension_surrogate import DimensionSurrogate, MissingColsException
import pandas as pd


class GeoLocationSurrogate(DimensionSurrogate):
    REQUIRED_FIELDS = {"Region", "Country"}

    def add_fk(self, df, fk_name: str = None, col_names: Dict[str, str] = None):
        if not self.REQUIRED_FIELDS.issubset(set(col_names.keys())):
            raise MissingColsException(
                f"Columns provided {col_names} are missing columns required {self.REQUIRED_FIELDS}")

        fk_name = "fk_location_id" if fk_name is None else fk_name

        geo_location_df = pd.read_parquet("output_data/geo_dimension.parquet", engine="pyarrow")

        df = pd.merge(df, geo_location_df, on=["Region", "Country"], how='left')
        df = df.rename(columns={"location_id": fk_name})
        return df
