"""Schema comparison by column names and types, ignoring nullability and the version column."""
from pyspark.sql import DataFrame
from pyspark.sql import types as T

from .errors import SchemaMismatchError

VERSION_COL = "_schema_version"


def check_schema(df: DataFrame, schema: T.StructType, *, where: str = "dataframe",
                 hint: str = "") -> DataFrame:
    """Raise unless df has exactly the columns and types of `schema`, ignoring the version
    column on both sides. Never casts. Returns df with only schema's columns, in its order.

    where, hint: name the checked data in the error message, and add a hint to it."""
    wanted_schema = _without_version(schema)
    actual = _column_types(_without_version(df.schema))
    wanted = _column_types(wanted_schema)
    missing = sorted(wanted.keys() - actual.keys())
    extra = sorted(actual.keys() - wanted.keys())
    wrong = sorted(f"{col}: {actual[col].simpleString()} (expected {wanted[col].simpleString()})"
                   for col in actual.keys() & wanted.keys() if actual[col] != wanted[col])
    if missing or extra or wrong:
        raise SchemaMismatchError(where, missing, extra, wrong, hint)
    return df.select(*wanted_schema.fieldNames())


def _without_version(schema: T.StructType) -> T.StructType:
    """Version column matched case-insensitively, like Spark does."""
    return T.StructType(
        [field for field in schema.fields if field.name.lower() != VERSION_COL]
    )


def _column_types(schema: T.StructType) -> dict[str, T.DataType]:
    return {field.name: _nullable(field.dataType) for field in schema.fields}


def _nullable(data_type: T.DataType) -> T.DataType:
    """The same type with every nested field nullable. Spark derives nested nullability
    from expressions (F.lit("x") is non-nullable), so it isn't a real difference."""
    match data_type:
        case T.StructType():
            return T.StructType([T.StructField(field.name, _nullable(field.dataType), True)
                                 for field in data_type.fields])
        case T.ArrayType():
            return T.ArrayType(_nullable(data_type.elementType), True)
        case T.MapType():
            return T.MapType(_nullable(data_type.keyType), _nullable(data_type.valueType), True)
        case _:
            return data_type
