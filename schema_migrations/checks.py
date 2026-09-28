"""The version check. Imports Foundry's `transforms`, so it's loaded only when used
(by `Migrations.checks`)."""

from transforms import expectations as E  # type: ignore[import-not-found]
from transforms.api import Check  # type: ignore[import-not-found]

from .schema import VERSION_COL


def version_check(latest: int) -> object:
    """Fails the build unless every row has version `latest`: catches rows written
    without the library (no version) or by older code (lower version). In `m.checks`."""
    return Check(
        E.all(E.col(VERSION_COL).non_null(), E.col(VERSION_COL).equals(latest)),
        f"Every row has schema version {latest}",
        on_error="FAIL",
    )
