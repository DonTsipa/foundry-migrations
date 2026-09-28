"""Migrations of one output dataset: each Migration is one schema change, Migrations the chain."""

from collections.abc import Callable, Sequence
from dataclasses import dataclass

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

from .errors import ConfigError, DataStateError, MigrationFailedError
from .prepared import PreparedOutput, TransformOutput, _is_incremental
from .schema import VERSION_COL, check_schema

type Upgrade = Callable[[DataFrame], DataFrame]


@dataclass(frozen=True)
class Migration:
    """One schema change: `upgrade` brings rows from `version - 1` to `version`.

    checks: Foundry `Check`s for the output from this version on; `Migrations.checks`
            collects them for `Output(..., checks=...)`."""

    version: int
    description: str
    upgrade: Upgrade
    checks: Sequence[object] = ()

    def __post_init__(self) -> None:
        if self.version < 1:
            raise ConfigError.invalid_version(self.version)

    def __str__(self) -> str:
        return f"{self.version} ({self.description})"

    def _apply(self, df: DataFrame, from_version: int) -> DataFrame:
        try:
            result = self.upgrade(df)
        except Exception as error:
            raise MigrationFailedError(self, from_version, str(error)) from error
        if not isinstance(result, DataFrame):
            raise MigrationFailedError(self, from_version, "it did not return a DataFrame")
        return result


class Migrations:
    """The migration chain of one output dataset. Versions are unique and without gaps.
    The oldest migrations may be deleted (e.g. [1, 2, 3, 4] -> [4]); data is then
    migrated only from version 3 (the baseline) or later."""

    def __init__(self, migrations: Sequence[Migration]) -> None:
        self._by_version = _index_by_version(migrations)
        self._latest = max(self._by_version, default=0)
        self._baseline = min(self._by_version, default=1) - 1  # oldest version data can be migrated from

    @property
    def checks(self) -> list[object]:
        """For `Output(..., checks=m.checks)`: the version check (every row at the latest
        version), then the Foundry `Check`s of all kept migrations, oldest first."""
        from .checks import version_check  # needs Foundry's `transforms`

        return [version_check(self._latest), *(
            check
            for version in sorted(self._by_version)
            for check in self._by_version[version].checks
        )]

    def prepare(self, out: TransformOutput) -> PreparedOutput:
        """Check the previous output of `out` now and plan its pending migrations, before
        any business logic runs. Raises if it can't be migrated. Writes nothing: write
        through the returned handle. Costs one aggregation, which isn't repeated when
        writing to the handle."""
        if not _is_incremental(out):  # snapshot output: nothing to migrate
            return PreparedOutput(out, self._latest, previous_rows=None, pending=False)
        stored = out.dataframe("previous")
        found = self._current_version(stored)
        if found is None:
            return PreparedOutput(out, self._latest, previous_rows=None, pending=False)
        current, stamped = found
        # unstamped rows are rewritten even with nothing to migrate, to stamp them
        return PreparedOutput(out, self._latest, previous_rows=self._upgrade(stored, current),
                              pending=current < self._latest or not stamped)

    def dry_run(
        self,
        spark: SparkSession,
        baseline_schema: T.StructType,
        expected: T.StructType | None = None,
    ) -> T.StructType:
        """Run the whole chain on an empty frame with the baseline schema; return the result
        schema. Raises `SchemaMismatchError` if `expected` is given and differs."""
        df = self._upgrade(spark.createDataFrame([], baseline_schema), self._baseline)
        if expected is not None:
            df = check_schema(df, expected, where="dry run result")
        return df.schema

    def _current_version(self, prev: DataFrame) -> tuple[int, bool] | None:
        """(version of every row in `prev`, whether they're stamped): None if empty, (0, False)
        if unversioned. Raises unless the rows can be migrated to the latest version. One aggregation without a shuffle; with
        spark.sql.parquet.aggregatePushdown=true it is read from parquet footers."""
        match _version_type(prev):
            case None:
                return (0, False) if prev.count() else None
            case T.IntegralType():
                pass
            case other:
                raise DataStateError.version_column_type(VERSION_COL, other.simpleString())
        (total, versioned, low, high), = prev.agg(
            F.count(F.lit(1)),
            F.count(VERSION_COL),
            F.min(VERSION_COL).cast("int"),
            F.max(VERSION_COL).cast("int"),
        ).collect()
        if total == 0:
            return None
        if versioned and (versioned < total or low != high):
            raise DataStateError.mixed_versions(_rows_per_version(prev), self._latest)
        version: int = low if versioned else 0  # all null counts as unversioned
        if version > self._latest:
            raise DataStateError.newer_than_code(version, self._latest)
        if version < self._baseline:
            raise DataStateError.older_than_baseline(
                version, self._baseline, self._by_version.get(self._baseline + 1))
        return version, bool(versioned)

    def _upgrade(self, prev: DataFrame, current: int) -> DataFrame:
        """prev with the migrations after `current` applied (planned; Spark runs them on write)."""
        df = prev
        for version in range(current + 1, self._latest + 1):
            df = self._by_version[version]._apply(df, current)
        return df


def _index_by_version(migrations: Sequence[Migration]) -> dict[int, Migration]:
    by_version: dict[int, Migration] = {}
    for migration in migrations:
        if (existing := by_version.get(migration.version)) is not None:
            raise ConfigError.duplicate_version(
                migration.version, existing.description, migration.description)
        by_version[migration.version] = migration
    expected = range(min(by_version, default=1), max(by_version, default=0) + 1)
    if missing := sorted(set(expected) - by_version.keys()):
        raise ConfigError.version_gap(missing)
    return by_version


def _version_type(df: DataFrame) -> T.DataType | None:
    """Type of the version column, matched case-insensitively like Spark does."""
    return next((field.dataType for field in df.schema.fields
                 if field.name.lower() == VERSION_COL), None)


def _rows_per_version(df: DataFrame) -> dict[int, int]:
    """Row count per version (null counts as 0). Needs a shuffle, so only used to explain errors."""
    version = F.coalesce(F.col(VERSION_COL).cast("int"), F.lit(0)).alias("version")
    return {row["version"]: row["count"] for row in df.groupBy(version).count().collect()}
