"""The parts of Foundry's transforms.api that the library uses."""

from typing import Any, Protocol

from pyspark.sql import DataFrame
from pyspark.sql import types as T

# Extra keyword arguments passed unchanged to Foundry's out.write_dataframe(df, **options),
# e.g. {"partition_cols": ["date"], "output_format": "parquet"}.
type WriteOptions = dict[str, Any]


class TransformOutput(Protocol):
    """A Foundry transform output. Only incremental outputs have `set_mode`."""

    def dataframe(
        self, mode: str = "current", schema: T.StructType | None = None
    ) -> DataFrame: ...

    def set_mode(self, mode: str) -> None: ...

    def write_dataframe(self, df: DataFrame, **kwargs: Any) -> None: ...
