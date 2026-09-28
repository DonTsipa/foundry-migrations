# schema_migrations — context for Claude Code

Small library for versioned (Alembic-style) schema migrations of **incremental Foundry
PySpark transforms**. See README.md for usage. This file records the design decisions
and open questions so work can continue without the original conversation.

## Files

- `schema_migrations/` — the library package (PySpark only, Python 3.12 syntax). Users
  copy this folder into their Foundry repo. Flat on purpose (was `core/` + `foundry/`
  subpackages that imported each other; flattened for readability).
  - `__init__.py` — re-exports the public names
  - `errors.py` — `MigrationError` base + `ConfigError` (import time), `DataStateError`
    (previous output can't be migrated), `MigrationFailedError`, `SchemaMismatchError`,
    `UsageError`. Situations are classmethod constructors (`DataStateError.mixed_versions(...)`)
    that build the message; no per-situation classes (was 14, nobody read their attributes).
  - `schema.py` — `VERSION_COL`, `check_schema` (names + types, nullability and version
    column ignored on both sides; `where=`/`hint=` only label the error)
  - `migrations.py` — `Migration` (one change; runs itself via `_apply`) and `Migrations`
    (the chain): public `prepare`, `dry_run`, `checks`; private `_current_version`
    (the one aggregation, formerly `VersionStats`), `_upgrade`, `_latest`, `_baseline`
  - `prepared.py` — `PreparedOutput` (returned by `prepare`): `write`, `previous`, `rewrite`,
    `plan_write`, `plan_rewrite` (return a `PlannedWrite(df, mode)` for native writes);
    also `TransformOutput` Protocol (`transforms.api` isn't available locally),
    `WriteOptions`, write-mode constants, `_stamp`. Doesn't import `Migrations` (gets `latest`).
  - `decorator.py` — `migrated(m, mode=)` / `migrated({output: Migrations}, mode=)`
    (overloads); `migrated_df(m, Output, mode=, write_options=, **inputs)`: a drop-in for
    `@transform_df` (which never passes the output to the function, so it can't be
    wrapped). Builds `transform(output=..., **inputs)` + `migrated(m, mode=)` around a
    generated `compute` whose `__signature__` is the function's params (inputs, optional
    `ctx`) + `output`; imports `transforms.api` lazily. Ported from the `nativeWrite`
    branch. `MigratedOutput` (the stand-in for each output the function gets);
    `Mode = Literal["append", "rewrite"]`
  - `checks.py` — `version_check(latest)`: Foundry `Check(E.all(E.col(VERSION_COL).non_null(),
    E.col(VERSION_COL).equals(latest)), ..., on_error="FAIL")`, first in `m.checks`. Every
    row must be at the latest version (was only non_null, as `VERSION_CHECK`): catches
    writes that bypass the library AND writes by older code. Per chain, so no module-level
    constant / `VERSION_CHECK` export. Needs Foundry's `transforms`, so imported inside the
    `checks` property. Untested with the real API (fake module).
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
    handle's `write(new_rows)` / `previous(schema)` + `rewrite(all_rows)` reuse that check.
  - **Entry points**: `@migrated({...}, mode=...)`, and `m.prepare(out)` + `write` /
    `previous` + `rewrite` / `plan_*` without a decorator. The former `m.migrated()`
    method (same name as the module function, different shape) was removed.
  - **`mode=` is required, keyword-only, no default** (user chose it; lib not used yet, so
    breaking was fine). It says what the function returns: `"append"` = new rows
    (`PreparedOutput.write`), `"rewrite"` = all rows, replace every run
    (`PreparedOutput.rewrite`). Reason: the user found `@migrated` didn't say "new rows";
    returning all rows in append mode silently duplicates. Replaced a separate
    `@rewrites(m)` decorator. Dict form: one mode, or output name -> mode (keys checked at
    import). Rejected: per-run choice via returning `Append(df)`/`Rewrite(df)` markers
    (breaks the TypedDict return check).
  - The function gets a `MigratedOutput` in place of each migrated output (both modes):
    `previous(schema)`; `write_dataframe`/`dataframe`/`set_mode` raise `UsageError`
    immediately. Returns are still type-checked (DataFrame / TypedDict). Several outputs:
    all are planned (schema-checked) before any is written. Decorated transform is typed
    `Callable[..., None]`: a ParamSpec can't swap one parameter's type.
    Rejected alternatives for reconcile: injecting a `previous` parameter (Foundry maps
    parameters by name; first run needs a schema) and a merge callback.
  - `plan_write(new_rows)` / `plan_rewrite(all_rows)` → `PlannedWrite(df, mode)`: what
    `write`/`rewrite` would do, for users who call `out.write_dataframe` themselves (user
    asked). `write`/`rewrite` are built on them. The library still decides the mode:
    The version check catches an unstamped write but not a wrong mode (appending a planned
    replace duplicates rows silently), which is why writing wasn't removed from the library.
  - `migrated(m, mode=, write_options=)` / `migrated({"orders": m1, "items": m2}, mode=, write_options=)` —
    decorator placed directly above `def`, below `@transform`; prepares the outputs before
    the function. `migrated(m)`: the transform must have exactly one output (found at run
    time as the argument with `write_dataframe`) and the function returns a DataFrame
    (user asked for this shorthand). Dict form (even with one entry): the function returns a
    dict validated (`UsageError`) before writing any output, and its return annotation must
    be a TypedDict with exactly those fields, all `DataFrame` (`UsageError` at import;
    resolved with `get_type_hints`, so the TypedDict must be at module level). User wants
    to keep this annotation check.
  - No `previous_schema` argument (removed): Foundry's `dataframe("previous", schema=)`
    only uses it for the empty frame when there's no previous output, and the library
    handles that case itself. Can't derive it from the input dataset (different schema;
    previous is read before business logic). If Foundry turns out to require it, pass
    `T.StructType([])` internally.
  - `PreparedOutput.previous(schema)` + `rewrite(all_rows)` — for reconcile-then-replace
    transforms: `previous` returns the migrated previous rows checked against `schema`,
    WITHOUT the version column (empty with those columns on the first run); `rewrite`
    stamps and writes with `replace`. User rejected passing a merge function (callback);
    chose to drop the version column from `previous` despite the risk: writing the result
    with `out.write_dataframe` instead of `rewrite` leaves the output unversioned and the
    next run re-applies every migration (silent corruption, reproduced). Documented.
  - `PreparedOutput.write(new_rows, write_options=None)` — appends, or replaces after a
    migration (also on the first run and for snapshot outputs).
  - `Migration(..., checks=[...])` — Foundry `Check`s valid from that version on;
    `Migrations.checks` = `[version_check(latest), *kept migrations' checks oldest first]`
    for `Output(..., checks=m.checks)`. Imports `checks.py` (and so `transforms`) only
    when read. Typed `object` (Check isn't available locally).
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
8. **Few tests.** User found 65, then 18 too many; now 9, one per behaviour (related
   cases are asserted inside the same test). Don't add a test per option/validation case;
   extend the matching test instead.
9. **Version column handling**: no explicit `drop` in the library. Migrations see
   `_schema_version` (constant, since mixed versions are rejected); `check_schema` ignores
   it and returns only the schema's columns; `_stamp` overwrites it with `withColumn`.
   Matched case-insensitively. Unversioned data never leaves the library unstamped:
   `_current_version` returns `(version, stamped)`, and unstamped rows (no column, or all
   null) count as pending even with `Migrations([])`, so the first write rewrites them
   stamped 0 (before, new stamped rows were appended next to unstamped ones and the next
   build failed with mixed versions).

10. **Adding a column (preferred pattern, documented in README):** `withColumn` silently
    overwrites an existing column and the schema check can't see it, so "add column"
    migrations should return `df` unchanged if the column already exists (keeps stored
    values, e.g. legacy data when adopting the library). A different existing type still
    fails the schema check. Verified locally. Not a library helper (decision 1).

11. **Checks per migration (option A)** are plain Foundry `Check`s; Foundry runs all of
    them on every build against the latest schema, never on intermediate versions.
    User rejected a `deprecates=` mechanism and Python `check(before, after)` callables.
    Rule (documented, not enforced): when a new migration contradicts an old check,
    remove it from the old migration in the same commit and leave a comment naming the
    overriding migration. This is the only allowed edit to a migration that already ran.
    Removing old migrations drops their checks; move still-valid ones to the oldest kept.

## Known limitations (documented in README)

- A `cast` that turns bad values into null is not detected (PySpark does it silently).
- Migration errors that happen only at write time (lazy evaluation, e.g. UDFs) aren't
  wrapped in `MigrationFailedError`; the transaction still aborts.
- Migrations only see the previous output; backfilling from source data needs a rebuild.
- Each migration forces a full recompute of downstream incremental transforms.
- `_schema_version` is visible to consumers.

## Not yet verified in Foundry (tested only with fakes)

1. `@migrated(...)` below `@transform` still lets Foundry map `src`/`out` by parameter name
   (uses `functools.wraps`, signature preserved).
2. `out.dataframe("previous")` returns the stored (old) schema after a schema change, and
   works without `schema=` (incl. the first run of a new dataset).
3. `require_incremental=True` allows the first run of a brand-new dataset.
4. Whether Foundry's Spark uses the V2 parquet reader, so
   `spark.sql.parquet.aggregatePushdown=true` shows `PushedAggregation` in the plan.
5. After rolling a dataset back to an earlier transaction (the recovery advised in the
   mixed-versions error), the next incremental build still runs incrementally.
6. The version check: Foundry's `Check` / `E.all` / `E.col(...).non_null()` / `.equals()`
   API names, and that a
   FAIL check aborts the transaction (tested only with a fake `transforms` module).
7. Migration checks: what a Check on a column that no longer exists does (fails the build
   or is skipped), and whether checks on an incremental append see only the new rows.
8. `@migrated_df`: Foundry maps parameters from the generated `__signature__` (incl. `ctx`),
   and `@incremental` accepts the `transform(...)` result it returns.

## Background from the conversation

The user started from wanting to fix existing rows (nulls → empty strings) in an
incremental dataset without a multi-hour rebuild and without breaking incrementality.
Conclusion: out-of-band writes (API/notebook transactions) break the incremental chain
(no build history, semantic version resets), so fixes to existing rows must be written by
the transform itself — which led to this library. That fix is simply a migration:
`Migration(n, "empty strings", lambda df: df.fillna("", subset=[...]))`.
