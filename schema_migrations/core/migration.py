"""One schema change."""

from collections.abc import Callable, Sequence
from dataclasses import dataclass

from pyspark.sql import DataFrame

from ..exceptions import InvalidVersionError, MigrationFailedError

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
