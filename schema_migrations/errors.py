"""Errors raised by schema_migrations. All subclass MigrationError, so one
`except MigrationError` catches them all; the message says what went wrong and what to do."""
from typing import TYPE_CHECKING, Self

if TYPE_CHECKING:
    from .migrations import Migration


class MigrationError(Exception):
    """Base class: migrations are misconfigured or the data is in an unexpected state."""


class ConfigError(MigrationError):
    """The migration list is invalid. Raised at import."""

    @classmethod
    def invalid_version(cls, version: int) -> Self:
        return cls(f"Migration version must be >= 1, got {version!r}")

    @classmethod
    def duplicate_version(cls, version: int, first: str, second: str) -> Self:
        return cls(
            f"Migration version {version} used twice: '{first}' and '{second}' "
            f"(likely two branches merged). Renumber one.")

    @classmethod
    def version_gap(cls, missing: list[int]) -> Self:
        return cls(
            f"Migration versions must be contiguous; missing {missing}. Only the oldest "
            f"migrations may be removed, never ones in the middle.")


class DataStateError(MigrationError):
    """The previous output can't be migrated. Raised before anything is written."""

    @classmethod
    def mixed_versions(cls, rows_per_version: dict[int, int], latest: int) -> Self:
        return cls(
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

    @classmethod
    def newer_than_code(cls, data_version: int, latest: int) -> Self:
        return cls(
            f"Data has schema version {data_version} but code only knows up to {latest}. "
            f"Was the latest migration removed or the code rolled back? Refusing to downgrade.")

    @classmethod
    def older_than_baseline(
        cls, data_version: int, baseline: int, oldest_kept: "Migration | None"
    ) -> Self:
        return cls(
            f"Data is at schema version {data_version}, but migrations up to {baseline} were "
            f"removed from the code (oldest kept: {oldest_kept or 'none'}). "
            f"Restore the removed migrations to upgrade this data.")

    @classmethod
    def version_column_type(cls, column: str, data_type: str) -> Self:
        return cls(
            f"Previous output has '{column}' of type {data_type}; expected an integer. "
            f"Something wrote to it outside the migration flow.")


class MigrationFailedError(MigrationError):
    def __init__(self, migration: "Migration", from_version: int, reason: str) -> None:
        self.migration, self.from_version = migration, from_version
        super().__init__(f"Migration {migration} failed on rows at version {from_version}: {reason}")


class SchemaMismatchError(MigrationError):
    def __init__(self, where: str, missing: list[str], extra: list[str], wrong: list[str],
                 hint: str = "") -> None:
        self.missing, self.extra, self.wrong = missing, extra, wrong
        parts = []
        if missing: parts.append(f"missing {missing}")
        if extra: parts.append(f"unexpected {extra}")
        if wrong: parts.append(f"wrong types {wrong}")
        super().__init__(f"Schema mismatch in {where}: " + "; ".join(parts)
                         + (f". {hint}" if hint else ""))


class UsageError(MigrationError):
    """The library is called the wrong way."""

    @classmethod
    def not_a_dataframe(cls, what: str, got: object) -> Self:
        return cls(
            f"{what} must be a DataFrame (got {type(got).__name__}). "
            f"Don't write to the output yourself.")

    @classmethod
    def output_not_found(cls, fn_name: str, missing: list[str], params: list[str]) -> Self:
        return cls(f"@migrated: '{fn_name}' has no parameters {missing} (it has {params})")

    @classmethod
    def return_annotation(cls, fn_name: str, outputs: list[str], problem: str) -> Self:
        return cls(
            f"@migrated: '{fn_name}' must be annotated to return a TypedDict with exactly the "
            f"fields {outputs}, each a DataFrame; {problem}.")

    @classmethod
    def not_one_output(cls, fn_name: str, outputs: list[str]) -> Self:
        return cls(
            f"@migrated(m): '{fn_name}' must have exactly one output, got {outputs}. "
            f"Name the migrated ones: @migrated({{'<output param>': m, ...}}, mode=...).")

    @classmethod
    def bad_return(cls, fn_name: str, returned: object, outputs: list[str] | None) -> Self:
        """outputs: the migrated outputs' names; None for a single output (@migrated(m))."""
        if isinstance(returned, dict):
            got = str({name: type(rows).__name__ for name, rows in returned.items()})
        else:
            got = type(returned).__name__
        expected = ("a DataFrame" if outputs is None
                    else f"a dict with a DataFrame for each of {outputs}")
        return cls(
            f"@migrated: '{fn_name}' must return its rows as {expected} (got {got}). "
            f"Don't write to the outputs yourself.")

    @classmethod
    def writes_itself(cls, fn_name: str, method: str) -> Self:
        return cls(
            f"@migrated: '{fn_name}' called out.{method}. Return the rows instead; the "
            f"decorator writes them. Read the previous output with out.previous(schema).")

    @classmethod
    def bad_mode(cls, problem: str) -> Self:
        return cls(f'@migrated: mode must be "append" (new rows) or "rewrite" (all rows); {problem}.')
