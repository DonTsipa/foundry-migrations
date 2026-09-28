"""An output checked before business logic runs, ready to be written."""

from dataclasses import dataclass
from typing import TYPE_CHECKING

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import types as T

from ..enums import WriteMode
from ..exceptions import NotADataFrameError
from .protocols import TransformOutput, WriteOptions
from ..core.schema import _without_version

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
        """Write `new_rows` to the output, with `_schema_version` added: append them, or
        replace the whole output with the migrated previous rows plus `new_rows` if
        migrations were pending. Replaces on the first run (empty previous output) and for
        snapshot outputs. Raises `SchemaMismatchError`, before writing, if the previous
        rows don't match the new rows' schema.

        write_options: extra keyword arguments for out.write_dataframe, passed unchanged,
                       e.g. {"partition_cols": ["date"]}."""
        if not isinstance(new_rows, DataFrame):
            raise NotADataFrameError("new_rows", new_rows)
        df, mode = self._resolve(self.migrations._stamp(new_rows))
        self._write(df, mode, write_options)

    def previous(self, schema: T.StructType) -> DataFrame:
        """The previous output migrated to the latest schema and checked against `schema`
        (usually new_rows.schema), without the version column. Empty with `schema`'s
        columns on the first run. Write what you build from it with `rewrite`, never with
        out.write_dataframe: an unstamped output makes the next run re-apply every migration.
        """
        if self.current is None or self.migrated is None:
            spark = self.stored.sparkSession if self.stored else SparkSession.active()
            return spark.createDataFrame([], _without_version(schema))
        return self.migrations._check_previous(self.migrated, self.current, schema)

    def rewrite(
        self, all_rows: DataFrame, write_options: WriteOptions | None = None
    ) -> None:
        """Replace the whole output with `all_rows`, marked with the latest version. Doesn't
        check `all_rows` against the previous output. For transforms that reconcile
        `previous(...)` with new rows themselves (upserts, dedup) or recompute everything.

        write_options: see `write`."""
        if not isinstance(all_rows, DataFrame):
            raise NotADataFrameError("all_rows", all_rows)
        self._write(self.migrations._stamp(all_rows), WriteMode.REPLACE, write_options)

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

    def _write(
        self, df: DataFrame, mode: WriteMode, write_options: WriteOptions | None
    ) -> None:
        if hasattr(self.out, "set_mode"):  # snapshot outputs always replace
            self.out.set_mode(mode)
        self.out.write_dataframe(df, **(write_options or {}))
