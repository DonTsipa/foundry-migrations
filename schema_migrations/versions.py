"""Reading the schema version stored in a dataset's rows."""
from dataclasses import dataclass

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql import types as T

from .exceptions import VersionColumnTypeError

VERSION_COL = "_schema_version"


@dataclass(frozen=True)
class VersionStats:
    """Version summary of a dataframe, from one aggregation without a shuffle.
    With spark.sql.parquet.aggregatePushdown=true it is read from parquet footers."""
    total_rows: int
    versioned_rows: int = 0
    min_version: int | None = None
    max_version: int | None = None

    @classmethod
    def of(cls, df: DataFrame) -> "VersionStats":
        match _version_type(df):
            case None:
                return cls(total_rows=df.count())
            case T.IntegralType():
                pass
            case other:
                raise VersionColumnTypeError(VERSION_COL, other.simpleString())
        (stats,) = df.agg(
            F.count(F.lit(1)).alias("total_rows"),
            F.count(VERSION_COL).alias("versioned_rows"),
            F.min(VERSION_COL).cast("int").alias("min_version"),
            F.max(VERSION_COL).cast("int").alias("max_version"),
        ).collect()
        return cls(**stats.asDict())

    @property
    def is_empty(self) -> bool:
        return self.total_rows == 0

    @property
    def is_mixed(self) -> bool:
        return self.versioned_rows > 0 and (
            self.versioned_rows < self.total_rows or self.min_version != self.max_version)

    @property
    def version(self) -> int | None:
        """The version shared by all rows: None if empty, 0 if unversioned."""
        if self.is_empty:
            return None
        return self.min_version if self.versioned_rows else 0


def _version_type(df: DataFrame) -> T.DataType | None:
    """Type of the version column, matched case-insensitively like Spark does."""
    return next((field.dataType for field in df.schema.fields
                 if field.name.lower() == VERSION_COL), None)


def _rows_per_version(df: DataFrame) -> dict[int, int]:
    """Row count per version (null counts as 0). Needs a shuffle, so only used to explain errors."""
    version = F.coalesce(F.col(VERSION_COL).cast("int"), F.lit(0)).alias("version")
    return {row["version"]: row["count"] for row in df.groupBy(version).count().collect()}
