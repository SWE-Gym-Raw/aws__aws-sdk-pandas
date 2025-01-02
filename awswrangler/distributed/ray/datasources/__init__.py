"""Ray Datasources Module."""

import ray

from awswrangler.distributed.ray.datasources.arrow_csv_datasink import ArrowCSVDatasink
from awswrangler.distributed.ray.datasources.arrow_csv_datasource import ArrowCSVDatasource
from awswrangler.distributed.ray.datasources.arrow_json_datasource import ArrowJSONDatasource
from awswrangler.distributed.ray.datasources.arrow_orc_datasink import ArrowORCDatasink
from awswrangler.distributed.ray.datasources.arrow_orc_datasource import ArrowORCDatasource
from awswrangler.distributed.ray.datasources.arrow_parquet_base_datasource import ArrowParquetBaseDatasource
from awswrangler.distributed.ray.datasources.arrow_parquet_datasink import ArrowParquetDatasink
from awswrangler.distributed.ray.datasources.arrow_parquet_datasource import ArrowParquetDatasource
from awswrangler.distributed.ray.datasources.file_datasink import _BlockFileDatasink
from awswrangler.distributed.ray.datasources.pandas_text_datasink import PandasCSVDatasink, PandasJSONDatasink
from awswrangler.distributed.ray.datasources.pandas_text_datasource import (
    PandasCSVDataSource,
    PandasFWFDataSource,
    PandasJSONDatasource,
    PandasTextDatasource,
)
from awswrangler.distributed.ray.datasources.write_result import (
    WriteResult,
    gen_datasink_write_result,
    generate_collect_write_stats_fn,
    generate_write_fn,
)

__all__ = [
    "ArrowCSVDatasink",
    "ArrowORCDatasink",
    "ArrowParquetDatasink",
    "ArrowCSVDatasource",
    "ArrowJSONDatasource",
    "ArrowORCDatasource",
    "ArrowParquetBaseDatasource",
    "ArrowParquetDatasource",
    "PandasCSVDataSource",
    "PandasFWFDataSource",
    "PandasJSONDatasource",
    "PandasTextDatasource",
    "PandasCSVDatasink",
    "PandasJSONDatasink",
    "_BlockFileDatasink",
]


# Monkeypatch ray methods to return write results
ray.data._internal.planner.plan_write_op.gen_datasink_write_result = gen_datasink_write_result  # type: ignore[attr-defined]
ray.data._internal.planner.plan_write_op.generate_write_fn = generate_write_fn  # type: ignore[attr-defined]
ray.data._internal.planner.plan_write_op.generate_collect_write_stats_fn = generate_collect_write_stats_fn  # type: ignore[attr-defined]
ray.data.datasource.datasink.WriteResult = WriteResult
