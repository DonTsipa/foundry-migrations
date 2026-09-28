"""A Foundry output checked before business logic runs, ready to be written."""

from dataclasses import dataclass
from typing import Any, Protocol

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

from .errors import UsageError
from .schema import VERSION_COL, _without_version, check_schema

# Foundry output write modes
APPEND, REPLACE = "modify", "replace"

# Extra keyword arguments passed unchanged to Foundry's out.write_dataframe(df, **options),
# e.g. {"partition_cols": ["date"], "output_format": "parquet"}.
type WriteOptions = dict[str, Any]


class TransformOutput(Protocol):
    """The parts of a Foundry transform output the library uses (`transforms.api` isn't
    available locally). Only incremental outputs have `set_mode`."""

    def dataframe(
        self, mode: str = "current", schema: T.StructType | None = None
    ) -> DataFrame: ...

    def set_mode(self, mode: str) -> None: ...

    def write_dataframe(self, df: DataFrame, **kwargs: Any) -> None: ...


@dataclass(frozen=True)
class PlannedWrite:
    """A write the library planned but didn't make: `df` is stamped with `_schema_version`,
    `mode` is APPEND ("modify") or REPLACE ("replace")."""

    df: DataFrame
    mode: str


@dataclass(frozen=True)
class PreparedOutput:
    """An output whose previous rows are already checked and whose migrations are planned.
    Created by `Migrations.prepare`; write to it once."""

    out: TransformOutput
    latest: int
    previous_rows: DataFrame | None  # stored rows after pending migrations; None if empty or snapshot
    pending: bool  # whether migrations were applied to previous_rows

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
        self._write(self.plan_write(new_rows), write_options)

    def plan_write(self, new_rows: DataFrame) -> "PlannedWrite":
        """What `write` would write, without writing it, for calling out.write_dataframe
        yourself: `out.set_mode(plan.mode)` (incremental outputs only), then
        `out.write_dataframe(plan.df, ...)`. Always use both."""
        if not isinstance(new_rows, DataFrame):
            raise UsageError.not_a_dataframe("new_rows", new_rows)
        new_rows = _stamp(new_rows, self.latest)
        if self.previous_rows is None:
            # replace, so an empty old-schema output never gets new rows appended
            return PlannedWrite(new_rows, REPLACE)
        previous = self._checked_previous(new_rows.schema)
        if self.pending:
            return PlannedWrite(_stamp(previous, self.latest).unionByName(new_rows), REPLACE)
        return PlannedWrite(new_rows.select(*self.previous_rows.columns), APPEND)  # stored column order

    def previous(self, schema: T.StructType) -> DataFrame:
        """The previous output migrated to the latest schema and checked against `schema`
        (usually new_rows.schema), without the version column. Empty with `schema`'s
        columns on the first run. Write what you build from it with `rewrite` (or
        `plan_rewrite`), never with out.write_dataframe directly: an unstamped output makes
        the next run re-apply every migration.
        """
        if self.previous_rows is None:
            return SparkSession.active().createDataFrame([], _without_version(schema))
        return self._checked_previous(schema)

    def rewrite(
        self, all_rows: DataFrame, write_options: WriteOptions | None = None
    ) -> None:
        """Replace the whole output with `all_rows`, marked with the latest version. Doesn't
        check `all_rows` against the previous output. For transforms that reconcile
        `previous(...)` with new rows themselves (upserts, dedup) or recompute everything.

        write_options: see `write`."""
        self._write(self.plan_rewrite(all_rows), write_options)

    def plan_rewrite(self, all_rows: DataFrame) -> "PlannedWrite":
        """What `rewrite` would write, without writing it; see `plan_write`."""
        if not isinstance(all_rows, DataFrame):
            raise UsageError.not_a_dataframe("all_rows", all_rows)
        return PlannedWrite(_stamp(all_rows, self.latest), REPLACE)

    def _checked_previous(self, schema: T.StructType) -> DataFrame:
        assert self.previous_rows is not None
        if self.pending:
            return check_schema(self.previous_rows, schema, where="previous output after migrations")
        return check_schema(
            self.previous_rows, schema, where="up-to-date previous output",
            hint="No migrations were pending: did the schema change without adding a Migration?")

    def _write(self, plan: "PlannedWrite", write_options: WriteOptions | None) -> None:
        if _is_incremental(self.out):  # snapshot outputs always replace
            self.out.set_mode(plan.mode)
        self.out.write_dataframe(plan.df, **(write_options or {}))


def _is_incremental(out: TransformOutput) -> bool:
    return hasattr(out, "set_mode")


def _stamp(df: DataFrame, version: int) -> DataFrame:
    """df marked with `version` (an existing version column is overwritten)."""
    return df.withColumn(VERSION_COL, F.lit(version).cast("int"))
