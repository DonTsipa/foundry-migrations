"""Versioned schema migrations for incremental Foundry (PySpark) transforms. See README.md."""
from .decorator import MigratedOutput, Mode, migrated
from .errors import (
    ConfigError,
    DataStateError,
    MigrationError,
    MigrationFailedError,
    SchemaMismatchError,
    UsageError,
)
from .migrations import Migration, Migrations
from .prepared import PlannedWrite, PreparedOutput, TransformOutput, WriteOptions
from .schema import VERSION_COL, check_schema

__all__ = [
    "Migration",
    "Migrations",
    "migrated",
    "Mode",
    "MigratedOutput",
    "PreparedOutput",
    "PlannedWrite",
    "TransformOutput",
    "WriteOptions",
    "VERSION_COL",
    "check_schema",
    "MigrationError",
    "ConfigError",
    "DataStateError",
    "MigrationFailedError",
    "SchemaMismatchError",
    "UsageError",
]


def __getattr__(name: str) -> object:
    # VERSION_CHECK needs Foundry's `transforms` package; import it only when asked for
    if name == "VERSION_CHECK":
        from .checks import VERSION_CHECK

        return VERSION_CHECK
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
