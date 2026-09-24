# schema_migrations — context for Claude Code

Small library for versioned (Alembic-style) schema migrations of **incremental Foundry
PySpark transforms**. See README.md for usage. This file records the design decisions
and open questions so work can continue without the original conversation.

## Files

- `schema_migrations/` — the library package (PySpark only, Python 3.12 syntax). Users
  copy this folder into their Foundry repo.
  - `__init__.py` — re-exports the public names
  - `enums.py` — `CheckedData` (which data failed a schema check), `WriteMode`
  - `exceptions.py` — `MigrationError` and one subclass per failure, each building its
    own message and keeping the relevant values as attributes
  - `core/` — plain Spark
    - `migration.py` — `Migration` (one schema change; runs itself via `_apply`)
    - `migrations.py` — `Migrations` (the chain): public `prepare`, `migrated`, `dry_run`;
      private `_current_version`, `_upgrade`, `_check_previous`, `_stamp`, `_latest`,
      `_baseline`
    - `versions.py` — `VERSION_COL`, `VersionStats` (one aggregation over the previous
      output), `_rows_per_version` (only for the mixed-versions error)
    - `schema.py` — `check_schema` (names + types, nullability ignored)
    - `decorators.py` — `_migrated_one` (body of `Migrations.migrated`) and the
      module-level multi-output `migrated({...})` with its TypedDict return check
  - `foundry/` — everything that touches a Foundry output
    - `protocols.py` — `TransformOutput` Protocol (the parts of Foundry's output the library
      calls; `transforms.api` isn't available locally), `WriteOptions`
    - `prepared.py` — `PreparedOutput` (returned by `prepare`): `write`, `rewrite`
  - `core/migrations.py` imports from `foundry/` (`prepare`/`migrated` are methods);
    `foundry/prepared.py` and `core/decorators.py` import `Migrations` only under
    TYPE_CHECKING, so there's no cycle.
- `tests/test_schema_migrations.py` — pytest suite, runs on local Spark with fake Foundry objects
- `pyproject.toml` — pytest config (repo root on the path, `tests/` as test path)
- `README.md` — user-facing docs

## Run tests

```bash
uv venv -p 3.12 .venv && uv pip install -p .venv/bin/python "pyspark==3.5.3" pytest mypy black   # needs Java 11+
.venv/bin/python -m pytest -q
.venv/bin/python -m mypy --disallow-untyped-defs schema_migrations tests
```

## How it works

- Each output row carries `_schema_version` (int).
- `Migration(version, description, upgrade)` — frozen dataclass; `upgrade` is a plain
  PySpark `df -> df`. Versions explicit, unique, contiguous.
- `Migrations(migrations)`:
  - `_current_version(prev)` → `int | None` (None = empty output). One count/min/max
    aggregation (no shuffle; can use parquet aggregate pushdown). Raises on mixed versions,
    data newer than code, data older than baseline. Per-version breakdown only on error.
  - `prepare(out)` → `PreparedOutput`: reads previous, checks versions, plans migrations
    BEFORE business logic (user asked for fail-fast, incl. multi-output transforms). The
    handle's `write(new_rows)` / `rewrite(merge, expected=)` reuse that check. The decorator
    prepares before calling the function. `prepare` is THE way without a decorator: the
    `m.write`/`m.rewrite` shortcuts were removed because they checked only after the
    business logic ran.
  - module-level `migrated({"orders": m1, "items": m2}, previous_schemas=, write_options=)` —
    several outputs; prepares all before the function, which returns {output: new rows};
    validates the dict (`ReturnedOutputsError`) before writing any output. The function's
    return annotation must be a TypedDict with exactly those fields, all `DataFrame`
    (`ReturnAnnotationError` at import; resolved with `get_type_hints`, so the TypedDict
    must be at module level).
  - `PreparedOutput.rewrite(merge, expected=...)` — for merge-then-replace transforms: `merge` gets the
    migrated previous rows WITHOUT the version column and returns everything to write; the
    library checks, stamps and writes with `replace`. Replaced public `migrate` + `replace`:
    returning unversioned data to users let them write it unstamped, and the next run then
    re-applied every migration (silent corruption, reproduced).
  - `PreparedOutput._resolve(new_rows)` → `(df, WriteMode)`.
  - `PreparedOutput.write(new_rows, write_options=None)` — appends, or replaces after a
    migration.
  - `@m.migrated()` — decorator placed directly above `def`, below `@transform`: prepares,
    calls the function (which RETURNS new rows), then `PreparedOutput.write`.
  - `dry_run(spark, baseline_schema, expected)` — unit-test aid, runs chain on empty frame.

## Design decisions (agreed with the user — keep them)

1. **Keep it simple.** User rejected: folder-per-migration loader, CLI/file generator,
   custom helpers (`rename`, `cast_column`, ...). Migrations use plain PySpark.
2. **Typed, explicit versions**, not list position — so old migrations can be removed.
3. **Removing old migrations**: only from the start of the list. Baseline = oldest kept
   version − 1. Data older than baseline fails the build. Gap in the middle fails at import.
4. **No declared schema required.** The new rows returned by the transform are the
   reference; previous output (migrated or up to date) must match them. This also
   catches "schema changed without adding a migration". The optional `schema=` argument
   was removed as redundant; a consumer contract is a `check_schema(df, CONTRACT)` call.
5. **Schema comparison ignores nullability at every nesting level** (struct/array/map);
   Spark sets nested nullability from expressions (e.g. `F.lit("x")` is non-nullable).
6. **Fail the build, never guess**: mixed versions in previous output (a sign of an
   out-of-band write: API, notebook, old code), data newer than code, data older than
   baseline — all raise a `MigrationError` subclass before anything is written.
7. Every migration rewrites the whole output (`replace`), so all rows share one version.
8. **Few tests.** User found 65, then 18 too many; now 8, one per behaviour (related
   cases are asserted inside the same test). Don't add a test per option/validation case;
   extend the matching test instead.
9. **Version column handling**: no explicit `drop` in the library. Migrations see
   `_schema_version` (constant, since mixed versions are rejected); `check_schema` ignores
   it and returns only the schema's columns; `_stamp` overwrites it with `withColumn`.
   Matched case-insensitively. Unversioned data never leaves the library unstamped.

## Known limitations (documented in README)

- A `cast` that turns bad values into null is not detected (PySpark does it silently).
- Migration errors that happen only at write time (lazy evaluation, e.g. UDFs) aren't
  wrapped in `MigrationFailedError`; the transaction still aborts.
- Migrations only see the previous output; backfilling from source data needs a rebuild.
- Each migration forces a full recompute of downstream incremental transforms.
- `_schema_version` is visible to consumers.

## Not yet verified in Foundry (tested only with fakes)

1. `@m.migrated()` below `@transform` still lets Foundry map `src`/`out` by parameter name
   (uses `functools.wraps`, signature preserved).
2. `out.dataframe("previous")` returns the stored (old) schema after a schema change, and
   how its `schema=` argument behaves when it differs (decorator has `previous_schema=`).
3. `require_incremental=True` allows the first run of a brand-new dataset.
4. Whether Foundry's Spark uses the V2 parquet reader, so
   `spark.sql.parquet.aggregatePushdown=true` shows `PushedAggregation` in the plan.
5. After rolling a dataset back to an earlier transaction (the recovery advised in the
   mixed-versions error), the next incremental build still runs incrementally.

## Background from the conversation

The user started from wanting to fix existing rows (nulls → empty strings) in an
incremental dataset without a multi-hour rebuild and without breaking incrementality.
Conclusion: out-of-band writes (API/notebook transactions) break the incremental chain
(no build history, semantic version resets), so fixes to existing rows must be written by
the transform itself — which led to this library. That fix is simply a migration:
`Migration(n, "empty strings", lambda df: df.fillna("", subset=[...]))`.
