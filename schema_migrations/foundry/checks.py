"""Foundry data expectations. Needs Foundry's `transforms` package, so it's imported
only when used (see `schema_migrations.__getattr__`)."""

from transforms import expectations as E  # type: ignore[import-not-found]
from transforms.api import Check  # type: ignore[import-not-found]

from ..core.versions import VERSION_COL

# Fails the build if an output is written without a version on every row, e.g. with
# out.write_dataframe instead of the library. Add it to each migrated output:
#   Output("/.../my_dataset", checks=[VERSION_CHECK])
VERSION_CHECK = Check(
    E.col(VERSION_COL).non_null(),
    "Every row has a schema version",
    on_error="FAIL",
)
