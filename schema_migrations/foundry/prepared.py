"""An output checked before business logic runs, ready to be written."""

from collections.abc import Callable
from dataclasses import dataclass
from typing import TYPE_CHECKING

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import types as T

from ..enums import WriteMode
from ..exceptions import NotADataFrameError
from .protocols import TransformOutput, WriteOptions
from ..core.schema import _without_version, check_schema

if TYPE_CHECKING:
    from ..core.migrations import Migrations


@dataclass(frozen=True)
class PreparedOutput:
    """An output whose previous rows are already checked and whose migrations are planned.
    Created by `Migrations.prepare`; write to it once."""

    migrations: "Migrations"
    out: TransformOutput
    stored: DataFrame | None  # previous output as stored; None for snapshot outputs
    migrated: DataFrame | None  # stored rows after pending migrations; None if empty
    current: int | None  # version of the stored rows; None if empty

    def write(
        self, new_rows: DataFrame, write_options: WriteOptions | None = None
    ) -> None:
        """Append `new_rows`, or, if migrations were pending, rewrite the whole output with
        the migrated previous rows plus `new_rows`. The previous rows must match the new
        rows' schema.

        write_options: extra keyword arguments for out.write_dataframe, passed unchanged,
                       e.g. {"partition_cols": ["date"]}."""
        if not isinstance(new_rows, DataFrame):
            raise NotADataFrameError("new_rows", new_rows)
        df, mode = self._resolve(self.migrations._stamp(new_rows))
        self._write(df, mode, write_options)

    def rewrite(
        self,
        merge: Callable[[DataFrame], DataFrame],
        *,
        expected: T.StructType,
        write_options: WriteOptions | None = None,
    ) -> None:
        """Rewrite the whole output, for transforms that merge the previous output with new
        rows themselves (upserts, dedup).

        merge:    gets the previous rows migrated to the latest schema, without the version
                  column (empty on the first run), and returns everything to write.
        expected: the latest schema, usually new_rows.schema. The migrated previous rows and
                  the merge result must match it.
        write_options: see `write`."""
        result = merge(self._previous_rows(expected))
        if not isinstance(result, DataFrame):
            raise NotADataFrameError("the result of merge", result)
        result = check_schema(result, _without_version(expected))
        self._write(self.migrations._stamp(result), WriteMode.REPLACE, write_options)

    def _resolve(self, new_rows: DataFrame) -> tuple[DataFrame, WriteMode]:
        """What to write, and how: new rows appended, or everything replaced after a migration."""
        # replace, so an empty old-schema output never gets new rows appended
        if self.current is None or self.stored is None or self.migrated is None:
            return new_rows, WriteMode.REPLACE
        previous = self.migrations._check_previous(
            self.migrated, self.current, new_rows.schema
        )
        if self.current == self.migrations._latest:  # append in the stored column order
            return new_rows.select(*self.stored.columns), WriteMode.APPEND
        return self.migrations._stamp(previous).unionByName(new_rows), WriteMode.REPLACE

    def _previous_rows(self, expected: T.StructType) -> DataFrame:
        if self.current is None or self.migrated is None:
            spark = self.stored.sparkSession if self.stored else SparkSession.active()
            return spark.createDataFrame([], _without_version(expected))
        return self.migrations._check_previous(self.migrated, self.current, expected)

    def _write(
        self, df: DataFrame, mode: WriteMode, write_options: WriteOptions | None
    ) -> None:
        if hasattr(self.out, "set_mode"):  # snapshot outputs always replace
            self.out.set_mode(mode)
        self.out.write_dataframe(df, **(write_options or {}))
