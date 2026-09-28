# schema_migrations

Versioned (Alembic-style) schema migrations for incremental Foundry PySpark transforms.
Python 3.12+. Copy the `schema_migrations/` folder into your repository's package.

Each row carries `_schema_version`. If the previous output is behind, the pending
migrations run on it before your transform sees it. The previous output, migrated or not,
must match the new rows' schema, which catches broken migrations and schema changes made
without one.

```python
m = Migrations([
    Migration(1, "rename amt to amount", lambda df: df.withColumnRenamed("amt", "amount")),
    Migration(2, "empty status",         lambda df: df.fillna("", subset=["status"])),
])

@incremental(require_incremental=True)
@transform(out=Output(..., checks=m.checks), src=Input(...))
@migrated(m, mode="append")                    # innermost
def compute(src, out):
    return business_logic(src.dataframe())     # return new rows, don't write
```

`mode` says what the function returns:

- `mode="append"`: **new rows**. They're appended; after a migration (and on the first
  run) the output is replaced with the migrated previous rows plus the new rows.
- `mode="rewrite"`: **all rows**. They replace the output every run. For transforms that
  reconcile the previous output with new rows (upserts, dedup) or recompute everything.

Returning all rows with `mode="append"` duplicates the previous rows, and no check notices.

`m.checks` starts with a version check: it fails the build unless every row has the latest
`_schema_version`, which catches writes that bypass the library (e.g. `out.write_dataframe`)
or come from older code. Add it to every migrated output.

To adopt the library on an existing output you can start with `Migrations([])`: the
existing rows are rewritten once with `_schema_version` 0, then builds append as usual.

## Adding a column

> [!WARNING]
> `withColumn` silently **overwrites** a column that already exists, and the schema check
> can't notice. If the stored data already has the column (legacy data when you adopt the
> library, or an output that lost its version), a plain "add column" migration wipes its
> values.

Preferred way: add the column only if it's missing, so existing values are kept.

```python
def add_currency(df):
    if "currency" in df.columns:      # already there: keep the stored values
        return df
    return df.withColumn("currency", F.lit(None).cast("string"))

Migration(3, "add currency", add_currency)
```

If the existing column has a different type, the build still fails on the schema check.

## Checks

A migration can carry Foundry `Check`s that must hold from its version on. `m.checks` is
the version check plus the checks of all kept migrations, oldest first:

```python
AMOUNT_NOT_NULL = Check(E.col("amount").non_null(), "amount not null", on_error="FAIL")

m = Migrations([
    Migration(1, "rename amt to amount", rename_amt, checks=[AMOUNT_NOT_NULL]),
])

@transform(out=Output(..., checks=m.checks), src=Input(...))
```

Foundry runs every check on every build, against the latest schema only. When a new
migration contradicts an old check (drops or renames its column, changes its type or
meaning), remove the check from the old migration in the same commit and leave a comment
there saying which migration overrides it:

```python
Migration(1, "rename amt to amount", rename_amt,
          checks=[]),  # AMOUNT_NOT_NULL removed: overridden by migration 3 (drop amount)
Migration(2, ...),
Migration(3, "drop amount", lambda df: df.drop("amount")),
```

With `on_error="WARN"` a forgotten contradicted check only warns.

## Use cases

In both modes the function gets a `MigratedOutput` in place of `out`:
`out.previous(schema)` returns the migrated previous rows, without `_schema_version`
(empty with `schema`'s columns on the first run). Calling `out.write_dataframe`,
`out.dataframe` or `out.set_mode` raises `UsageError`: return the rows instead.

Reconcile and replace every run:

```python
@incremental(require_incremental=True)
@transform(out=Output(..., checks=m.checks), src=Input(...))
@migrated(m, mode="rewrite")
def compute(src, out: MigratedOutput) -> DataFrame:
    new_rows = business_logic(src.dataframe())
    return reconcile(out.previous(new_rows.schema), new_rows)
```

Several outputs (or one migrated output among several): pass a dict of output parameter
name → migration list; every output is checked before the function runs. `mode` is one
mode for all outputs or a dict with a mode per output. The return type
must be a TypedDict naming exactly the migrated outputs (checked at import, and by your
type checker):

```python
class Outputs(TypedDict):
    orders: DataFrame
    items: DataFrame

@incremental(require_incremental=True)
@transform(
    orders=Output(..., checks=orders_m.checks),
    items=Output(..., checks=items_m.checks),
    src=Input(...),
)
@migrated({"orders": orders_m, "items": items_m}, mode={"orders": "append", "items": "rewrite"})
def compute(src, orders, items) -> Outputs:
    df = business_logic(src.dataframe())
    return {"orders": to_orders(df), "items": to_items(df)}
```

`write_options=` passes extra arguments to `out.write_dataframe`:
`@migrated(m, mode="append", write_options={"partition_cols": ["date"]})`, or per output
name with the dict form.

`@transform_df` functions: use `@migrated_df(m, ..., mode=...)` instead, with the same
arguments and body. `@transform_df` never passes the output to the function, so
`@migrated` can't wrap it; `@migrated_df` builds the `@transform` for you.

```python
@incremental(require_incremental=True)
@migrated_df(m, Output(..., checks=m.checks), mode="append", src=Input(...))   # was @transform_df(Output(...), src=Input(...))
def compute(src: DataFrame) -> DataFrame:
    return business_logic(src)
```

Writing yourself (no decorator, e.g. native `out.write_dataframe` calls): `prepare` the output first
(it checks the previous output and plans the migrations before any business logic runs),
then ask for the planned write and make it with **both** `set_mode` and `write_dataframe`:

```python
prepared = m.prepare(out)
new_rows = business_logic(src.dataframe())
plan = prepared.plan_write(new_rows)                # like mode="append"; or like mode="rewrite":
# plan = prepared.plan_rewrite(reconcile(prepared.previous(new_rows.schema), new_rows))
out.set_mode(plan.mode)                             # "modify" or "replace"; incremental outputs only
out.write_dataframe(plan.df, ...)                   # plan.df is stamped with _schema_version
```

Write `plan.df`, not a frame of your own: an output without `_schema_version` makes the
next run re-apply every migration (the version check fails that build). Don't skip
`set_mode`: appending a planned `replace` duplicates the previous rows, and no check
notices. `prepared.write(new_rows)` / `prepared.rewrite(all_rows)` do both steps for you.

Unit test without data: `m.dry_run(spark, BASELINE_SCHEMA, expected=LATEST_SCHEMA)`.

Int → struct: `cast()` can't do it; build with `F.struct` and keep nulls as null:
`F.when(F.col("A").isNull(), F.lit(None).cast(A_TYPE)).otherwise(F.struct(...))`.

## Rules

- A new migration gets `latest + 1`. Never change one that already ran, except to
  remove a check a newer migration contradicts (see Checks).
- Change the migration list in the same commit as the business logic.
- Remove old migrations only from the **start** of the list, once all data is past them.
  Data older than the oldest kept version − 1 then fails the build. Move their checks to
  the oldest kept migration, or they stop being run.

## What fails the build

Anything unexpected raises a `MigrationError` before anything is written:
`ConfigError` (bad migration list, at import), `DataStateError` (previous output can't be
migrated), `MigrationFailedError`, `SchemaMismatchError`, `UsageError` (library called the
wrong way).

Not caught: a `cast` that silently turns bad values into null; errors that happen only
while writing (e.g. in a UDF) don't name the migration.

## When it doesn't fit

- Snapshot transforms that are cheap to rebuild.
- Very large outputs: each migration rewrites the output and forces a full recompute
  downstream.
- Changes that need source data: migrations only see the previous output.
- Outputs with other writers, Ontology-backed datasets, consumers that can't tolerate
  the `_schema_version` column.

## Verify in Foundry (tested locally with fakes only)

1. `@migrated(...)` below `@transform` still lets Foundry map `src`/`out` by name.
2. `out.dataframe("previous")` returns the stored (old) schema after a schema change, and
   works without a `schema=` argument (on the first run too).
3. `require_incremental=True` allows the first run of a new dataset.
4. Incremental builds continue after a dataset rollback.
5. With several outputs, a build that fails after one output was written commits none.
6. `spark.sql.parquet.aggregatePushdown=true` shows `PushedAggregation` for the version check.
7. The version check builds with Foundry's `Check` / `E.all(...)` / `E.col(...).non_null()` /
   `.equals(...)` API and fails the build.
8. `@migrated_df`: Foundry maps parameters from the generated `__signature__` (including
   `ctx`), and `@incremental` accepts the transform it builds.
