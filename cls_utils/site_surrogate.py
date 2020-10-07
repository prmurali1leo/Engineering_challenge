from typing import Dict
from cls_utils.dimension_surrogate import DimensionSurrogate, MissingColsException
import pandas as pd


class SiteSurrogate(DimensionSurrogate):
    REQUIRED_FIELDS = {"ForecastSiteCode"}

    def add_fk(self, df, fk_name: str = None, col_names: Dict[str, str] = None):
        if not self.REQUIRED_FIELDS.issubset(set(col_names.keys())):
            raise MissingColsException(
                f"Columns provided {col_names} are missing columns required {self.REQUIRED_FIELDS}")

        fk_name = "fk_site_id" if fk_name is None else fk_name

        site_df = pd.read_parquet("output_data/site_dimension.parquet", engine="pyarrow")

        df = pd.merge(df, site_df, on=["ForecastSiteCode"], how='left')
        df = df.rename(columns={"site_id": fk_name})
        return df
