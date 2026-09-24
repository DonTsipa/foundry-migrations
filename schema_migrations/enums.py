from enum import StrEnum


class CheckedData(StrEnum):
    """Which data failed a schema check; named in SchemaMismatchError's message."""
    MIGRATED_PREVIOUS = "previous output after migrations"
    UP_TO_DATE_PREVIOUS = "up-to-date previous output"
    DATAFRAME = "dataframe"


class WriteMode(StrEnum):
    """Foundry output write modes."""
    APPEND = "modify"
    REPLACE = "replace"
