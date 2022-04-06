import logging
import os
from pathlib import Path
from typing import Any, Dict, Optional

import pandas as pd
from kedro.framework.context import load_context
from kedro.framework.hooks import hook_impl
from kedro.io import DataSetError
from popmon import df_stability_report


class KedroPopmonHook:
    def __init__(self):
        self.logger = logging.getLogger("KedroPopmon")
        self.config = {}
        self.default_date_format_pyspark = "%y-%M-%d %H:%m:%s"
        self.default_date_format = "%Y-%m-%d %H:%M:%S"

    @hook_impl
    def before_pipeline_run(self, run_params: Dict[str, Any]) -> None:
        self.context = load_context(
            project_path=run_params["project_path"],
            env=run_params["env"],
            extra_params=run_params["extra_params"],
        )
        self.config = self.context.config_loader.get("popmon*", "popmon*/**")
        self.logger.info(f"{self.config}")

    @hook_impl
    def after_pipeline_run(self, catalog: Optional[Dict[str, Dict[str, Any]]]):
        datasets = catalog._data_sets
        self.logger.info(f"Found datasets: {datasets}")
        for dataset in datasets.keys():
            if dataset in self.config:
                column = self.config[dataset]["date_column"]
                columns = self.config[dataset]["columns"]
                df_type = type(datasets[dataset]).__name__
                self.logger.info(f"Dataset type: {df_type}")

                if df_type == "CSVDataSet":
                    date_format = self.config[dataset].get(
                        "date_column_format", self.default_date_format
                    )
                    df = datasets[dataset].load()
                    self.logger.info("Using Pandas data frame API for Popmon")
                    df[column] = pd.to_datetime(df[column], format=date_format)
                elif df_type == "SparkDataSet" or df_type == "SparkHiveDataSet":
                    try:
                        import pyspark
                        from pyspark.sql.functions import to_timestamp
                        from pyspark.sql.types import DateType
                    except ImportError:
                        self.logger.error(
                            "PySpark module not found, please install it."
                        )
                    self.logger.info("Using PySpark data frame API for Popmon")
                    df = datasets[dataset].load()
                    if not isinstance(df.schema[column].dataType, DateType):
                        self.logger.info(
                            f"Adding date column with DateType "
                            f"using `{column}` column"
                        )
                        date_format = self.config[dataset].get(
                            "date_column_format", self.default_date_format_pyspark
                        )
                        df = df.withColumn(
                            column, to_timestamp(df[column], date_format,),
                        )
                    else:
                        self.logger.info(
                            "Date column in proper format, " "not adding a new one"
                        )
                else:
                    raise DataSetError("Unsupported Kedro Dataset type")
                report = df_stability_report(df, time_axis=column, features=columns)
                if "data" not in os.listdir(Path.cwd()):
                    os.mkdir("data")
                report.to_file(
                    f"data{os.path.sep}08_reporting{os.path.sep}popmon{os.path.sep}{dataset}.html"
                )


hooks = KedroPopmonHook()
