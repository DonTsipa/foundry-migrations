"""The migration chain of one output dataset."""

from collections.abc import Callable, Sequence

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

from .decorators import _migrated_one
from ..enums import CheckedData
from ..exceptions import (
    DataNewerThanCodeError,
    DataOlderThanBaselineError,
    DuplicateVersionError,
    MixedVersionsError,
    VersionGapError,
)
from .migration import Migration
from ..foundry.prepared import PreparedOutput
from ..foundry.protocols import TransformOutput, WriteOptions
from .schema import _without_version, check_schema
from .versions import VERSION_COL, VersionStats, _rows_per_version


class Migrations:
    """The migration chain of one output dataset. Versions are unique and without gaps.
    The oldest migrations may be deleted (e.g. [1, 2, 3, 4] -> [4]); data is then
    migrated only from version 3 (the baseline) or later."""

    def __init__(self, migrations: Sequence[Migration]) -> None:
        self._by_version = _index_by_version(migrations)

    @property
    def _latest(self) -> int:
        return max(self._by_version, default=0)

    @property
    def _baseline(self) -> int:
        """Oldest version data can still be migrated from."""
        return min(self._by_version, default=1) - 1

    # ---------- public API ----------

    def prepare(
        self, out: TransformOutput, previous_schema: T.StructType | None = None
    ) -> PreparedOutput:
        """Check the previous output of `out` now and plan its pending migrations, before
        any business logic runs. Raises if it can't be migrated. Costs one aggregation,
        which isn't repeated when writing to the returned handle.

        previous_schema: passed to out.dataframe("previous", schema=...), if your
                         transforms version needs it."""
        if not hasattr(out, "set_mode"):  # snapshot output: nothing to migrate
            return PreparedOutput(self, out, stored=None, migrated=None, current=None)
        stored = (
            out.dataframe("previous")
            if previous_schema is None
            else out.dataframe("previous", schema=previous_schema)
        )
        current = self._current_version(stored)
        migrated = None if current is None else self._upgrade(stored, current)
        return PreparedOutput(self, out, stored, migrated, current)

    def migrated[**TransformParams](
        self,
        output: str | None = None,
        previous_schema: T.StructType | None = None,
        write_options: WriteOptions | None = None,
    ) -> Callable[
        [Callable[TransformParams, DataFrame]], Callable[TransformParams, None]
    ]:
        """Decorator for a transform that returns its new rows. The output is prepared
        before the function runs, then the rows are written to it.

        output:          name of the output parameter, needed only when there are several.
        previous_schema: see `prepare`; write_options: see `PreparedOutput.write`."""
        return _migrated_one(self, output, previous_schema, write_options)

    def dry_run(
        self,
        spark: SparkSession,
        baseline_schema: T.StructType,
        expected: T.StructType | None = None,
    ) -> T.StructType:
        """Run the whole chain on an empty frame with the baseline schema; return the result schema."""
        df = self._upgrade(spark.createDataFrame([], baseline_schema), self._baseline)
        if expected is not None:
            df = self._check_previous(df, self._baseline, expected)
        return df.schema

    # ---------- building blocks ----------

    def _current_version(self, prev: DataFrame) -> int | None:
        """Version of every row in `prev` (0 = unversioned, None = empty).
        Raises if the rows can't be migrated to the latest version."""
        stats = VersionStats.of(prev)
        if stats.is_mixed:
            raise MixedVersionsError(_rows_per_version(prev), self._latest)
        match stats.version:
            case None:
                return None
            case version if version > self._latest:
                raise DataNewerThanCodeError(version, self._latest)
            case version if version < self._baseline:
                raise DataOlderThanBaselineError(
                    version, self._baseline, self._by_version.get(self._baseline + 1)
                )
            case version:
                return version

    def _pending(self, current: int) -> list[Migration]:
        return [
            self._by_version[version]
            for version in range(current + 1, self._latest + 1)
        ]

    def _upgrade(self, prev: DataFrame, current: int) -> DataFrame:
        """prev with the migrations after `current` applied (planned; Spark runs them on write)."""
        df = prev
        for migration in self._pending(current):
            df = migration._apply(df, current)
        return df

    def _check_previous(
        self, migrated: DataFrame, current: int, expected: T.StructType
    ) -> DataFrame:
        """Migrated (or up-to-date) previous rows checked against the latest schema,
        without the version column."""
        checked = (
            CheckedData.UP_TO_DATE_PREVIOUS
            if current == self._latest
            else CheckedData.MIGRATED_PREVIOUS
        )
        return check_schema(migrated, _without_version(expected), checked)

    def _stamp(self, df: DataFrame) -> DataFrame:
        """df marked with the latest version (an existing version column is overwritten)."""
        return df.withColumn(VERSION_COL, F.lit(self._latest).cast("int"))


def _index_by_version(migrations: Sequence[Migration]) -> dict[int, Migration]:
    by_version: dict[int, Migration] = {}
    for migration in migrations:
        if (existing := by_version.get(migration.version)) is not None:
            raise DuplicateVersionError(
                migration.version, existing.description, migration.description
            )
        by_version[migration.version] = migration
    expected = range(min(by_version, default=1), max(by_version, default=0) + 1)
    if missing := sorted(set(expected) - by_version.keys()):
        raise VersionGapError(missing)
    return by_version
