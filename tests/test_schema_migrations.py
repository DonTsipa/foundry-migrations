import inspect
from collections.abc import Iterator
from typing import Any, TypedDict
import os
import sys

import pytest
from pyspark.sql import DataFrame, SparkSession, functions as F, types as T

from schema_migrations import (
    VERSION_COL, ConfigError, DataStateError, Migration, MigrationError, MigrationFailedError, Migrations,
    PreparedOutput, SchemaMismatchError, UsageError, migrated, MigratedOutput,
)

V0 = T.StructType([T.StructField("id", T.StringType()), T.StructField("amt", T.IntegerType())])
TARGET = T.StructType([
    T.StructField("id", T.StringType()),
    T.StructField("amount", T.DoubleType()),
    T.StructField("status", T.StringType()),
])
M1 = Migration(1, "rename amt", lambda df: df.withColumnRenamed("amt", "amount"))
M2 = Migration(2, "amount to double", lambda df: df.withColumn("amount", F.col("amount").cast("double")))
M3 = Migration(3, "add status", lambda df: df.withColumn("status", F.lit(None).cast("string")))


class OrdersAndItems(TypedDict):
    orders: DataFrame
    items: DataFrame


class OrdersOnly(TypedDict):
    orders: DataFrame


@pytest.fixture(scope="session")
def spark() -> Iterator[SparkSession]:
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)     # workers use the same Python as the driver
    s = (SparkSession.builder.master("local[1]").config("spark.ui.enabled", "false")
         .config("spark.sql.shuffle.partitions", "1").getOrCreate())
    s.sparkContext.setLogLevel("ERROR")
    yield s
    s.stop()


def m() -> Migrations:
    return Migrations([M1, M2, M3])


def stamped(df: DataFrame) -> DataFrame:
    """df as the library writes it at the latest version (3)."""
    return df.withColumn(VERSION_COL, F.lit(3))


def rows(df: DataFrame | None) -> list[tuple]:
    assert df is not None
    return sorted(tuple(r) for r in df.collect())


def new(spark: SparkSession, *data: tuple) -> DataFrame:
    return spark.createDataFrame(list(data), TARGET)


def at_version(spark: SparkSession, data: list[tuple], ddl: str) -> DataFrame:
    return spark.createDataFrame(data, f"{ddl}, {VERSION_COL} int")


def resolve(migrations: Migrations, prev: DataFrame, new_rows: DataFrame) -> tuple[DataFrame, str | None]:
    """What `PreparedOutput.write` would write to an output whose previous rows are `prev`, and in which mode."""
    out = FakeOutput(prev.sparkSession, stored=prev)
    migrations.prepare(out).write(new_rows)
    assert out.written is not None
    return out.written, out.mode


def prepare(migrations: Migrations, prev: DataFrame) -> PreparedOutput:
    return migrations.prepare(FakeOutput(prev.sparkSession, stored=prev))


# ---------- configuration ----------

def test_bad_configuration_fails_at_import() -> None:
    with pytest.raises(ConfigError, match=">= 1"):
        Migration(0, "x", lambda df: df)
    with pytest.raises(ConfigError, match="used twice"):
        Migrations([M1, M2, Migration(2, "other", M3.upgrade)])
    with pytest.raises(ConfigError, match=r"missing \[2\]"):
        Migrations([M1, M3])

    amount_check, status_check = object(), object()                            # stand-ins for Foundry Checks
    with_checks = Migrations([Migration(1, "rename amt", M1.upgrade, checks=[amount_check]), M2,
                              Migration(3, "add status", M3.upgrade, checks=[status_check])])
    assert with_checks.checks == [amount_check, status_check] and m().checks == []


# ---------- versions ----------

def test_write_modes(spark: SparkSession) -> None:
    first_run, mode = resolve(m(), spark.createDataFrame([], T.StructType([])), new(spark, ("a", 1.0, "s")))
    assert mode == "replace" and rows(first_run) == [("a", 1.0, "s", 3)]

    legacy, mode = resolve(m(), spark.createDataFrame([("a", 5)], V0), new(spark, ("b", 2.0, "n")))
    assert mode == "replace" and rows(legacy) == [("a", 5.0, None, 3), ("b", 2.0, "n", 3)]

    at_v2 = at_version(spark, [("a", 5.0, 2)], "id string, amount double")      # only migration 3 runs
    assert rows(resolve(m(), at_v2, new(spark))[0]) == [("a", 5.0, None, 3)]
    assert rows(resolve(Migrations([M3]), at_v2, new(spark))[0]) == [("a", 5.0, None, 3)]  # 1-2 removed

    up_to_date = stamped(new(spark, ("a", 1.0, "s")))
    reordered = new(spark, ("b", 2.0, "t")).select("status", "amount", "id")
    appended, mode = resolve(m(), up_to_date, reordered)
    assert mode == "modify" and rows(appended) == [("b", 2.0, "t", 3)]
    assert appended.columns == ["id", "amount", "status", VERSION_COL]          # stored column order
    plan = prepare(m(), up_to_date).plan_write(reordered)                       # for a native write
    assert plan.mode == "modify" and plan.df.columns == appended.columns


def test_bad_previous_output_fails(spark: SparkSession) -> None:
    ddl = "id string, amount double, status string"
    mixed = at_version(spark, [("a", 1.0, "s", None), ("b", 2.0, "s", 3)], ddl)
    with pytest.raises(DataStateError, match=r"rows per version: \{0: 1, 3: 1\}"):
        prepare(m(), mixed)
    with pytest.raises(DataStateError, match="code only knows up to 3"):
        prepare(m(), at_version(spark, [("a", 1.0, "s", 4)], ddl))
    with pytest.raises(DataStateError, match="were removed"):                             # 1-2 removed
        prepare(Migrations([M3]), at_version(spark, [("a", 5.0, 1)], "id string, amount double"))
    # "abc" would aggregate to a null version and look like an empty output (data loss)
    with pytest.raises(DataStateError, match="expected an integer"):
        prepare(m(), spark.createDataFrame([("a", 1.0, "s", "abc")], f"{ddl}, {VERSION_COL} string"))


# ---------- schema check ----------

def test_mistakes_caught(spark: SparkSession) -> None:
    # Spark ignores a rename of a missing column; comparing with the new rows catches it
    typo = Migrations([Migration(1, "rename", lambda df: df.withColumnRenamed("amnt", "amount")),
                       Migration(2, "add status", M3.upgrade)])
    old = spark.createDataFrame([("a", 1.0)], "id string, amt double")
    with pytest.raises(SchemaMismatchError, match=r"after migrations.*missing \['amount'\].*unexpected \['amt'\]"):
        resolve(typo, old, new(spark))

    changed = spark.createDataFrame([("b", 2.0, "s", 1)], "id string, amount double, status string, extra int")
    with pytest.raises(SchemaMismatchError, match="without adding a Migration"):
        resolve(m(), stamped(new(spark, ("a", 1.0, "s"))), changed)

    bad = Migrations([Migration(1, "broken", lambda df: df.withColumn("y", F.col("nope")))])
    with pytest.raises(MigrationFailedError, match=r"Migration 1 \(broken\)"):
        prepare(bad, spark.createDataFrame([("a", 1)], V0))                    # fails when preparing

    with pytest.raises(MigrationError):
        Migrations([Migration(1, "wrong way", lambda df: df.withColumnRenamed("amount", "amt"))]).dry_run(spark, V0, TARGET)
    assert m().dry_run(spark, V0, TARGET).fieldNames() == ["id", "amount", "status"]


def test_int_to_struct(spark: SparkSession) -> None:
    a_type = T.StructType([T.StructField("value", T.IntegerType()), T.StructField("unit", T.StringType())])
    target = T.StructType([T.StructField("id", T.StringType()), T.StructField("A", a_type)])
    to_struct = Migration(1, "A -> struct", lambda df: df.withColumn("A",
        F.when(F.col("A").isNull(), F.lit(None).cast(a_type))
         .otherwise(F.struct(F.col("A").alias("value"), F.lit("kg").alias("unit")))))  # lit = non-nullable
    out = prepare(Migrations([to_struct]), spark.createDataFrame([("x", 5), ("y", None)], "id string, A int")).previous(target)
    got = {r["id"]: r["A"] for r in out.collect()}
    assert tuple(got["x"]) == (5, "kg") and got["y"] is None

    as_long = Migration(1, "A -> struct", lambda df: df.withColumn(
        "A", F.struct(F.col("A").cast("long").alias("value"), F.lit("kg").alias("unit"))))
    with pytest.raises(SchemaMismatchError, match="wrong types"):              # real type difference
        prepare(Migrations([as_long]), spark.createDataFrame([("x", 5)], "id string, A int")).previous(target)


# ---------- write / @migrated (fake Foundry objects) ----------

class FakeInput:
    def __init__(self, df: DataFrame) -> None:
        self._df = df
    def dataframe(self) -> DataFrame:
        return self._df


class FakeOutput:
    def __init__(self, spark: SparkSession, stored: DataFrame | None = None) -> None:
        self.spark, self.stored = spark, stored
        self.mode: str | None = None
        self.written: DataFrame | None = None
    def dataframe(self, mode: str = "current", schema: T.StructType | None = None) -> DataFrame:
        return self.stored if self.stored is not None else self.spark.createDataFrame([], T.StructType([]))
    def set_mode(self, mode: str) -> None:
        self.mode = mode
    def write_dataframe(self, df: DataFrame, **kwargs: Any) -> None:
        self.written = df


def test_rewrite(spark: SparkSession) -> None:
    def reconcile(previous: DataFrame, new_rows: DataFrame) -> DataFrame:
        return previous.join(new_rows.select("id"), "id", "left_anti").unionByName(new_rows)

    out = FakeOutput(spark, stored=spark.createDataFrame([("a", 5), ("b", 6)], V0))
    prepared = m().prepare(out)
    new_rows = new(spark, ("b", 2.0, "t"))
    previous = prepared.previous(new_rows.schema)
    assert previous.columns == ["id", "amount", "status"]                   # migrated, no version column
    prepared.rewrite(reconcile(previous, new_rows))
    assert out.mode == "replace" and rows(out.written) == [("a", 5.0, None, 3), ("b", 2.0, "t", 3)]

    first_run = m().prepare(FakeOutput(spark))                             # empty, with the new rows' columns
    assert first_run.previous(new_rows.schema).columns == ["id", "amount", "status"]
    plan = first_run.plan_rewrite(new_rows)                                # for a native write
    assert plan.mode == "replace" and rows(plan.df) == [("b", 2.0, "t", 3)]

    @migrated(m(), mode="rewrite")
    def compute(src: FakeInput, out: MigratedOutput) -> DataFrame:
        new_rows = src.dataframe()
        return reconcile(out.previous(new_rows.schema), new_rows)
    assert list(inspect.signature(compute).parameters) == ["src", "out"]   # @transform can still map names

    out = FakeOutput(spark, stored=spark.createDataFrame([("a", 5), ("b", 6)], V0))
    compute(src=FakeInput(new(spark, ("b", 2.0, "t"))), out=out)
    assert out.mode == "replace" and rows(out.written) == [("a", 5.0, None, 3), ("b", 2.0, "t", 3)]
    first = FakeOutput(spark)
    compute(FakeInput(new(spark, ("b", 2.0, "t"))), first)                 # positional, first run
    assert first.mode == "replace" and rows(first.written) == [("b", 2.0, "t", 3)]

    with pytest.raises(UsageError, match="mode must be"):                  # at import
        migrated(m(), mode="replace")  # type: ignore[call-overload]

def test_decorator(spark: SparkSession) -> None:
    calls: list[str] = []

    @migrated(m(), mode="append")
    def compute(src: FakeInput, out: MigratedOutput) -> DataFrame:
        calls.append("business logic")
        return src.dataframe()
    assert list(inspect.signature(compute).parameters) == ["src", "out"]   # @transform can still map names

    mixed = at_version(spark, [("a", 1.0, "s", 2), ("b", 2.0, "s", 3)], "id string, amount double, status string")
    broken = FakeOutput(spark, stored=mixed)
    with pytest.raises(DataStateError):
        compute(src=FakeInput(new(spark)), out=broken)
    assert calls == [] and broken.written is None                 # checked before business logic

    out = FakeOutput(spark, stored=spark.createDataFrame([("a", 5)], V0))
    compute(src=FakeInput(new(spark, ("b", 2.0, "t"))), out=out)
    assert out.mode == "replace" and rows(out.written) == [("a", 5.0, None, 3), ("b", 2.0, "t", 3)]

    @migrated(m(), mode="append")  # type: ignore[arg-type]  # deliberately returns nothing
    def writes_itself(src: FakeInput, out: MigratedOutput) -> None:
        out.write_dataframe(src.dataframe())
    with pytest.raises(UsageError, match="called out.write_dataframe"):
        writes_itself(src=FakeInput(new(spark)), out=FakeOutput(spark))

    @migrated(m(), mode="append")
    def returns_nothing(src: FakeInput, out: MigratedOutput) -> DataFrame:
        return None  # type: ignore[return-value]
    with pytest.raises(UsageError, match="must return its rows as a DataFrame"):
        returns_nothing(src=FakeInput(new(spark)), out=FakeOutput(spark))

    @migrated(m(), mode="append")
    def two_outputs(src: FakeInput, out: FakeOutput, other: FakeOutput) -> DataFrame:
        return src.dataframe()
    with pytest.raises(UsageError, match=r"exactly one output, got \['out', 'other'\]"):
        two_outputs(src=FakeInput(new(spark)), out=FakeOutput(spark), other=FakeOutput(spark))


def test_several_outputs(spark: SparkSession) -> None:
    calls: list[str] = []

    @migrated({"orders": m(), "items": m()}, mode={"orders": "append", "items": "rewrite"})
    def compute(src: FakeInput, orders: MigratedOutput, items: MigratedOutput) -> OrdersAndItems:
        calls.append("business logic")
        return {"orders": src.dataframe(), "items": src.dataframe()}
    assert list(inspect.signature(compute).parameters) == ["src", "orders", "items"]

    orders = FakeOutput(spark, stored=stamped(new(spark, ("a", 1.0, "s"))))     # up to date
    newer = at_version(spark, [("a", 1.0, "s", 4)], "id string, amount double, status string")
    with pytest.raises(DataStateError):                                              # items is broken
        compute(src=FakeInput(new(spark, ("b", 2.0, "t"))), orders=orders, items=FakeOutput(spark, stored=newer))
    assert calls == [] and orders.written is None                     # nothing ran, nothing written

    items = FakeOutput(spark, stored=spark.createDataFrame([("a", 5)], V0))          # legacy
    compute(src=FakeInput(new(spark, ("b", 2.0, "t"))), orders=orders, items=items)
    assert orders.mode == "modify" and rows(orders.written) == [("b", 2.0, "t", 3)]
    assert items.mode == "replace" and rows(items.written) == [("b", 2.0, "t", 3)]  # rewrite: only what's returned

    with pytest.raises(UsageError, match="annotated"):                # at import
        @migrated({"orders": m(), "items": m()}, mode="append")
        def wrong_type(src: FakeInput, orders: FakeOutput, items: FakeOutput) -> OrdersOnly:
            return {"orders": src.dataframe()}

    with pytest.raises(UsageError, match="got modes for"):             # at import
        migrated({"orders": m(), "items": m()}, mode={"orders": "append"})

    @migrated({"orders": m(), "items": m()}, mode="append")
    def forgets_items(src: FakeInput, orders: FakeOutput, items: FakeOutput) -> OrdersAndItems:
        return {"orders": src.dataframe()}  # type: ignore[typeddict-item]
    with pytest.raises(UsageError, match="must return"):              # at run time
        forgets_items(src=FakeInput(new(spark)), orders=FakeOutput(spark), items=FakeOutput(spark))
