"""Schema comparison by column names and types, ignoring nullability."""
from pyspark.sql import DataFrame
from pyspark.sql import types as T

from .enums import CheckedData
from .exceptions import SchemaMismatchError
from .versions import VERSION_COL


def check_schema(df: DataFrame, schema: T.StructType,
                 checked: CheckedData = CheckedData.DATAFRAME) -> DataFrame:
    """Raise unless df has exactly the columns and types of `schema`, ignoring the version
    column. Never casts. Returns df with only schema's columns, in its order."""
    actual = _column_types(_without_version(df.schema))
    wanted = _column_types(schema)
    missing = sorted(wanted.keys() - actual.keys())
    extra = sorted(actual.keys() - wanted.keys())
    wrong = sorted(f"{col}: {actual[col].simpleString()} (expected {wanted[col].simpleString()})"
                   for col in actual.keys() & wanted.keys() if actual[col] != wanted[col])
    if missing or extra or wrong:
        raise SchemaMismatchError(checked, missing, extra, wrong)
    return df.select(*schema.fieldNames())


def _without_version(schema: T.StructType) -> T.StructType:
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
