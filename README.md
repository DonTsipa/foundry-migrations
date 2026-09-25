# schema_migrations

Versioned (Alembic-style) schema migrations for incremental Foundry PySpark transforms.
Python 3.12+. Copy the `schema_migrations/` folder into your repository's package.

Each row carries `_schema_version`. If the previous output is behind, the pending
migrations run on it and the output is rewritten once; otherwise new rows are appended.
The previous output, migrated or not, must match the new rows' schema, which catches
broken migrations and schema changes made without one.

```python
m = Migrations([
    Migration(1, "rename amt to amount", lambda df: df.withColumnRenamed("amt", "amount")),
    Migration(2, "empty status",         lambda df: df.fillna("", subset=["status"])),
])

@incremental(require_incremental=True)
@transform(out=Output(..., checks=[VERSION_CHECK]), src=Input(...))
@m.migrated()                                  # innermost
def compute(src, out):
    return business_logic(src.dataframe())     # return new rows, don't write
```

`VERSION_CHECK` (`from myproject.schema_migrations import VERSION_CHECK`) fails the build
if any row is written without `_schema_version`, e.g. by `out.write_dataframe` instead of
the library. Add it to every migrated output.

## Use cases

Without a decorator, `prepare` every output first: it checks the previous output and
plans the migrations before any business logic runs.

```python
def compute(src, out):
    prepared = m.prepare(out)
    prepared.write(business_logic(src.dataframe()))
```

Several outputs, one migration list each; every output is checked before the function
runs. The return type must be a TypedDict naming exactly the migrated outputs (checked at
import, and by your type checker):

```python
class Outputs(TypedDict):
    orders: DataFrame
    items: DataFrame

@incremental(require_incremental=True)
@transform(orders=Output(...), items=Output(...), src=Input(...))
@migrated({"orders": orders_m, "items": items_m})
def compute(src, orders, items) -> Outputs:
    df = business_logic(src.dataframe())
    return {"orders": to_orders(df), "items": to_items(df)}
```

Reconcile and `replace` every run (upserts, dedup): take the migrated previous rows,
combine them with the new ones, and write everything with `rewrite`.

```python
prepared = m.prepare(out)
new_rows = business_logic(src.dataframe())
previous = prepared.previous(new_rows.schema)      # migrated; empty with these columns on the first run
prepared.rewrite(reconcile(previous, new_rows))    # stamped, written with replace
```

Always write it with `rewrite`, not `out.write_dataframe`: an output without
`_schema_version` makes the next run re-apply every migration.

Unit test without data: `m.dry_run(spark, BASELINE_SCHEMA, expected=LATEST_SCHEMA)`.

Int → struct: `cast()` can't do it; build with `F.struct` and keep nulls as null:
`F.when(F.col("A").isNull(), F.lit(None).cast(A_TYPE)).otherwise(F.struct(...))`.

## Rules

- A new migration gets `latest + 1`. Never change one that already ran.
- Change the migration list in the same commit as the business logic.
- Remove old migrations only from the **start** of the list, once all data is past them.
  Data older than the oldest kept version − 1 then fails the build.

## What fails the build

Anything unexpected raises a `MigrationError` subclass before anything is written.

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

1. `@m.migrated()` below `@transform` still lets Foundry map `src`/`out` by name.
2. `out.dataframe("previous")` returns the stored (old) schema after a schema change.
3. `require_incremental=True` allows the first run of a new dataset.
4. Incremental builds continue after a dataset rollback.
5. With several outputs, a build that fails after one output was written commits none.
6. `spark.sql.parquet.aggregatePushdown=true` shows `PushedAggregation` for the version check.
7. `VERSION_CHECK` builds with Foundry's `Check` / `E.col(...).non_null()` API and fails the build.
