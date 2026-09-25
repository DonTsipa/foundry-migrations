"""Versioned schema migrations for incremental Foundry (PySpark) transforms. See README.md."""
from .enums import CheckedData, WriteMode
from .exceptions import (
    MigrationError,
    InvalidVersionError,
    DuplicateVersionError,
    VersionGapError,
    MixedVersionsError,
    DataNewerThanCodeError,
    DataOlderThanBaselineError,
    VersionColumnTypeError,
    MigrationFailedError,
    SchemaMismatchError,
    NotADataFrameError,
    OutputNotFoundError,
    ReturnAnnotationError,
    ReturnedOutputsError,
)
from .core.decorators import migrated
from .core.migration import Migration
from .core.migrations import Migrations
from .foundry.prepared import PreparedOutput
from .foundry.protocols import TransformOutput
from .core.schema import check_schema
from .core.versions import VERSION_COL

__all__ = [
    "Migration",
    "Migrations",
    "PreparedOutput",
    "migrated",
    "TransformOutput",
    "VERSION_COL",
    "check_schema",
    "CheckedData",
    "WriteMode",
    "MigrationError",
    "InvalidVersionError",
    "DuplicateVersionError",
    "VersionColumnTypeError",
    "VersionGapError",
    "MixedVersionsError",
    "DataNewerThanCodeError",
    "DataOlderThanBaselineError",
    "MigrationFailedError",
    "SchemaMismatchError",
    "NotADataFrameError",
    "OutputNotFoundError",
    "ReturnAnnotationError",
    "ReturnedOutputsError",
]


def __getattr__(name: str) -> object:
    # VERSION_CHECK needs Foundry's `transforms` package; import it only when asked for
    if name == "VERSION_CHECK":
        from .foundry.checks import VERSION_CHECK

        return VERSION_CHECK
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
