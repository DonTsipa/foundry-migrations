"""Versioned schema migrations for incremental Foundry PySpark transforms. See README.md."""

import functools
import inspect
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from typing import get_type_hints, is_typeddict

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

from .enums import CheckedData, WriteMode
from .exceptions import (
    DataNewerThanCodeError,
    DataOlderThanBaselineError,
    DuplicateVersionError,
    InvalidVersionError,
    MigrationFailedError,
    MixedVersionsError,
    NotADataFrameError,
    OutputNotFoundError,
    ReturnAnnotationError,
    ReturnedOutputsError,
    VersionGapError,
)
from .protocols import TransformOutput, WriteOptions
from .schema import _without_version, check_schema
from .versions import VERSION_COL, VersionStats, _rows_per_version

type Upgrade = Callable[[DataFrame], DataFrame]


@dataclass(frozen=True)
class Migration:
    """One schema change: `upgrade` brings rows from `version - 1` to `version`."""

    version: int
    description: str
    upgrade: Upgrade

    def __post_init__(self) -> None:
        if self.version < 1:
            raise InvalidVersionError(self.version)

    def __str__(self) -> str:
        return f"{self.version} ({self.description})"

    def _apply(self, df: DataFrame, from_version: int) -> DataFrame:
        try:
            result = self.upgrade(df)
        except Exception as error:
            raise MigrationFailedError(self, from_version, str(error)) from error
        if not isinstance(result, DataFrame):
            raise MigrationFailedError(
                self, from_version, "it did not return a DataFrame"
            )
        return result


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
    ) -> "PreparedOutput":
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

    def migrated[**P](
        self,
        output: str | None = None,
        previous_schema: T.StructType | None = None,
        write_options: WriteOptions | None = None,
    ) -> Callable[[Callable[P, DataFrame]], Callable[P, None]]:
        """Decorator for a transform that returns its new rows. The output is prepared
        before the function runs, then the rows are written to it.

        output:          name of the output parameter, needed only when there are several.
        previous_schema: see `prepare`; write_options: see `PreparedOutput.write`."""

        def decorator(fn: Callable[P, DataFrame]) -> Callable[P, None]:
            signature = inspect.signature(fn)
            if output is not None and output not in signature.parameters:
                raise OutputNotFoundError(
                    fn.__name__, list(signature.parameters), output
                )

            # keeps the signature, so @transform still maps inputs/outputs
            @functools.wraps(fn)
            def wrapper(*args: P.args, **kwargs: P.kwargs) -> None:
                arguments = signature.bind(*args, **kwargs).arguments
                out = arguments[output or _only_output(fn, arguments)]
                prepared = self.prepare(out, previous_schema)
                new_rows = fn(*args, **kwargs)
                if not isinstance(new_rows, DataFrame):
                    raise NotADataFrameError(
                        f"@migrated: the return value of '{fn.__name__}'", new_rows
                    )
                prepared.write(new_rows, write_options)

            return wrapper

        return decorator

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


@dataclass(frozen=True)
class PreparedOutput:
    """An output whose previous rows are already checked and whose migrations are planned.
    Created by `Migrations.prepare`; write to it once."""

    migrations: Migrations
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


def migrated[**P](
    migrations: Mapping[str, Migrations],
    previous_schemas: Mapping[str, T.StructType] | None = None,
    write_options: Mapping[str, WriteOptions] | None = None,
) -> Callable[[Callable[P, Mapping[str, object]]], Callable[P, None]]:
    """Decorator for a transform with several migrated outputs.

    migrations:       output parameter name -> that output's Migrations.
    previous_schemas: output name -> schema for out.dataframe("previous", schema=...).
    write_options:    output name -> extra keyword arguments for out.write_dataframe.

    The function is annotated to return a TypedDict with one DataFrame field per output
    (checked at import) and returns the new rows in it. Every output is prepared before the
    function runs; all returned rows are checked before any is written.
    """
    schemas, options = previous_schemas or {}, write_options or {}

    def decorator(fn: Callable[P, Mapping[str, object]]) -> Callable[P, None]:
        signature = inspect.signature(fn)
        for name in migrations:
            if name not in signature.parameters:
                raise OutputNotFoundError(fn.__name__, list(signature.parameters), name)
        _check_return_annotation(fn, sorted(migrations))

        # keeps the signature, so @transform still maps inputs/outputs
        @functools.wraps(fn)
        def wrapper(*args: P.args, **kwargs: P.kwargs) -> None:
            arguments = signature.bind(*args, **kwargs).arguments
            prepared = {
                name: m.prepare(arguments[name], schemas.get(name))
                for name, m in migrations.items()
            }
            new_rows = fn(*args, **kwargs)
            if not isinstance(new_rows, dict) or new_rows.keys() != prepared.keys():
                raise ReturnedOutputsError(fn.__name__, new_rows, sorted(prepared))
            for name, rows in new_rows.items():
                if not isinstance(rows, DataFrame):
                    raise NotADataFrameError(
                        f"@migrated: '{fn.__name__}' output '{name}'", rows
                    )
            for name, output in prepared.items():
                output.write(new_rows[name], options.get(name))

        return wrapper

    return decorator


def _check_return_annotation(fn: Callable[..., object], outputs: list[str]) -> None:
    """Raise unless fn is annotated to return a TypedDict of exactly `outputs`, all DataFrames."""
    try:
        returns = get_type_hints(fn).get("return")
    except NameError as error:  # e.g. a TypedDict defined inside a function
        raise ReturnAnnotationError(fn.__name__, outputs, f"can't resolve it ({error})")
    if not is_typeddict(returns):
        raise ReturnAnnotationError(fn.__name__, outputs, f"got {returns!r}")
    fields = get_type_hints(returns)
    if sorted(fields) != outputs:
        raise ReturnAnnotationError(fn.__name__, outputs, f"got fields {sorted(fields)}")
    if wrong := sorted(name for name, tp in fields.items() if tp is not DataFrame):
        raise ReturnAnnotationError(fn.__name__, outputs, f"{wrong} aren't DataFrame")


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


def _only_output(fn: Callable[..., object], arguments: dict[str, object]) -> str:
    outputs = [
        param for param, value in arguments.items() if hasattr(value, "write_dataframe")
    ]
    if len(outputs) != 1:
        raise OutputNotFoundError(fn.__name__, outputs)
    return outputs[0]
