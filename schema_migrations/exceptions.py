"""Errors raised by schema_migrations. All subclass MigrationError, so one
`except MigrationError` catches them all."""
from typing import TYPE_CHECKING

from .enums import CheckedData

if TYPE_CHECKING:
    from .core.migration import Migration


class MigrationError(Exception):
    """Base class: migrations are misconfigured or the data is in an unexpected state."""


# ---------- errors: configuration (raised at import) ----------

class InvalidVersionError(MigrationError):
    def __init__(self, version: int) -> None:
        self.version = version
        super().__init__(f"Migration version must be >= 1, got {version!r}")


class DuplicateVersionError(MigrationError):
    def __init__(self, version: int, first: str, second: str) -> None:
        self.version = version
        super().__init__(
            f"Migration version {version} used twice: '{first}' and '{second}' "
            f"(likely two branches merged). Renumber one.")


class VersionGapError(MigrationError):
    def __init__(self, missing: list[int]) -> None:
        self.missing = missing
        super().__init__(
            f"Migration versions must be contiguous; missing {missing}. Only the oldest "
            f"migrations may be removed, never ones in the middle.")


# ---------- errors: state of the previous output ----------

class MixedVersionsError(MigrationError):
    def __init__(self, rows_per_version: dict[int, int], latest: int) -> None:
        self.rows_per_version = rows_per_version
        super().__init__(
            f"Previous output mixes schema versions {sorted(rows_per_version)} "
            f"(rows per version: {rows_per_version}; null counts as 0). Something wrote to it "
            f"outside the migration flow: an API or notebook transaction, or an old version "
            f"of this transform appending (e.g. from another branch). Nothing was written.\n"
            f"To recover:\n"
            f"  1. Find the writer in the dataset's History tab (a transaction not made by "
            f"this transform's build) and stop it, or this will happen again.\n"
            f"  2. If nothing needed was written after it, roll the dataset back to the "
            f"transaction before it and rerun the build.\n"
            f"  3. Otherwise rebuild the output from scratch (bump semantic_version in "
            f"@incremental); every row is then written at version {latest}.")


class DataNewerThanCodeError(MigrationError):
    def __init__(self, data_version: int, latest: int) -> None:
        self.data_version, self.latest = data_version, latest
        super().__init__(
            f"Data has schema version {data_version} but code only knows up to {latest}. "
            f"Was the latest migration removed or the code rolled back? Refusing to downgrade.")


class DataOlderThanBaselineError(MigrationError):
    def __init__(self, data_version: int, baseline: int, oldest_kept: "Migration | None") -> None:
        self.data_version, self.baseline = data_version, baseline
        super().__init__(
            f"Data is at schema version {data_version}, but migrations up to {baseline} were "
            f"removed from the code (oldest kept: {oldest_kept or 'none'}). "
            f"Restore the removed migrations to upgrade this data.")


class VersionColumnTypeError(MigrationError):
    def __init__(self, column: str, data_type: str) -> None:
        self.column, self.data_type = column, data_type
        super().__init__(
            f"Previous output has '{column}' of type {data_type}; expected an integer. "
            f"Something wrote to it outside the migration flow.")


# ---------- errors: running migrations and checking schemas ----------

class MigrationFailedError(MigrationError):
    def __init__(self, migration: "Migration", from_version: int, reason: str) -> None:
        self.migration, self.from_version = migration, from_version
        super().__init__(f"Migration {migration} failed on rows at version {from_version}: {reason}")


class SchemaMismatchError(MigrationError):
    def __init__(self, checked: CheckedData, missing: list[str], extra: list[str], wrong: list[str]) -> None:
        self.checked, self.missing, self.extra, self.wrong = checked, missing, extra, wrong
        parts = []
        if missing: parts.append(f"missing {missing}")
        if extra: parts.append(f"unexpected {extra}")
        if wrong: parts.append(f"wrong types {wrong}")
        message = f"Schema mismatch in {checked}: " + "; ".join(parts)
        if checked is CheckedData.UP_TO_DATE_PREVIOUS:
            message += ". No migrations were pending: did the schema change without adding a Migration?"
        super().__init__(message)


# ---------- errors: wrong use of write / @migrated ----------

class NotADataFrameError(MigrationError):
    def __init__(self, what: str, got: object) -> None:
        super().__init__(
            f"{what} must be a DataFrame of new rows (got {type(got).__name__}). "
            f"Don't write to the output yourself.")


class ReturnAnnotationError(MigrationError):
    def __init__(self, fn_name: str, expected: list[str], problem: str) -> None:
        self.expected = expected
        super().__init__(
            f"@migrated: '{fn_name}' must be annotated to return a TypedDict with exactly the "
            f"fields {expected}, each a DataFrame; {problem}.")


class ReturnedOutputsError(MigrationError):
    def __init__(self, fn_name: str, returned: object, expected: list[str]) -> None:
        self.expected = expected
        got = sorted(returned) if isinstance(returned, dict) else type(returned).__name__
        super().__init__(
            f"@migrated: '{fn_name}' must return a dict of new rows for exactly the outputs "
            f"{expected} (got {got}). Don't write to the outputs yourself.")


class OutputNotFoundError(MigrationError):
    def __init__(self, fn_name: str, outputs: list[str], output: str | None = None) -> None:
        self.outputs = outputs
        if output is not None:
            message = f"@migrated: '{fn_name}' has no parameter '{output}'"
        else:
            message = (f"@migrated: '{fn_name}' has {len(outputs)} outputs {outputs}; "
                       f"pass output='<param name>' to say which one is migrated.")
        super().__init__(message)
