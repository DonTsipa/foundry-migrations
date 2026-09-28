"""The @migrated decorator (for @transform) and @migrated_df (instead of @transform_df):
prepare outputs before a transform runs and write what it returns, appended or replacing
the output (`mode=`)."""

import functools
import inspect
from collections.abc import Callable, Mapping
from typing import Any, Literal, get_args, get_type_hints, is_typeddict, overload

from pyspark.sql import DataFrame
from pyspark.sql import types as T

from .errors import UsageError
from .migrations import Migrations
from .prepared import PlannedWrite, PreparedOutput, WriteOptions

# "append": the function returns new rows (appended, or written with the migrated previous
#           rows after a migration). "rewrite": it returns all rows (replace every run).
type Mode = Literal["append", "rewrite"]


class MigratedOutput:
    """What a @migrated function gets in place of each migrated output: the migrated
    previous rows, and nothing to write with (the decorator writes what it returns)."""

    def __init__(self, prepared: PreparedOutput, fn_name: str) -> None:
        self._prepared, self._fn_name = prepared, fn_name

    def previous(self, schema: T.StructType) -> DataFrame:
        """See `PreparedOutput.previous`: migrated, checked against `schema`, without the
        version column; empty with `schema`'s columns on the first run."""
        return self._prepared.previous(schema)

    def dataframe(self, *args: object, **kwargs: object) -> DataFrame:
        raise UsageError.writes_itself(self._fn_name, "dataframe")

    def set_mode(self, *args: object, **kwargs: object) -> None:
        raise UsageError.writes_itself(self._fn_name, "set_mode")

    def write_dataframe(self, *args: object, **kwargs: object) -> None:
        raise UsageError.writes_itself(self._fn_name, "write_dataframe")


@overload
def migrated(
    migrations: Migrations,
    *,
    mode: Mode,
    write_options: WriteOptions | None = None,
) -> Callable[[Callable[..., DataFrame]], Callable[..., None]]: ...


@overload
def migrated(
    migrations: Mapping[str, Migrations],
    *,
    mode: Mode | Mapping[str, Mode],
    write_options: Mapping[str, WriteOptions] | None = None,
) -> Callable[[Callable[..., Mapping[str, object]]], Callable[..., None]]: ...


def migrated(
    migrations: Migrations | Mapping[str, Migrations], *, mode: Any, write_options: Any = None
) -> Any:
    """Decorator for a transform whose outputs the library writes. Place it directly above
    `def`, below `@transform`. The migrated outputs are prepared (see `Migrations.prepare`)
    before the function runs, and the function gets a `MigratedOutput` in place of each:
    `out.previous(schema)` returns its migrated previous rows. The function returns rows;
    the decorator stamps and WRITES them. The decorated transform returns None.

    mode="append":  the function returns NEW rows; they're appended, or written together
                    with the migrated previous rows after a migration (`PreparedOutput.write`).
    mode="rewrite": the function returns ALL rows; they replace the output every run
                    (`PreparedOutput.rewrite`).

    @migrated(m, mode=...): the transform has one output; the function returns a DataFrame.
                  write_options: extra keyword arguments for out.write_dataframe.
    @migrated({"orders": m1, "items": m2}, mode=...): output parameter name -> its
                  Migrations; mode is one mode for all or output name -> mode. The function
                  is annotated to return a TypedDict with one DataFrame field per output
                  (checked at import) and returns that dict; all returned rows are checked
                  before any output is written.
                  write_options: output name -> extra keyword arguments."""
    if isinstance(migrations, Migrations):
        _check_modes({"": mode})
        return _one_output(migrations, mode, write_options)
    modes = dict(mode) if isinstance(mode, Mapping) else {name: mode for name in migrations}
    if sorted(modes) != sorted(migrations):
        raise UsageError.bad_mode(f"got modes for {sorted(modes)}, outputs are {sorted(migrations)}")
    _check_modes(modes)
    return _several_outputs(migrations, modes, write_options or {})


def _one_output(
    migrations: Migrations, mode: Mode, write_options: WriteOptions | None
) -> Callable[[Callable[..., DataFrame]], Callable[..., None]]:
    def decorator(fn: Callable[..., DataFrame]) -> Callable[..., None]:
        signature = inspect.signature(fn)

        # keeps the signature, so @transform still maps inputs/outputs
        @functools.wraps(fn)
        def wrapper(*args: Any, **kwargs: Any) -> None:
            bound = signature.bind(*args, **kwargs)
            name = _only_output(fn.__name__, bound.arguments)
            prepared = migrations.prepare(bound.arguments[name])
            bound.arguments[name] = MigratedOutput(prepared, fn.__name__)
            rows = fn(*bound.args, **bound.kwargs)
            if not isinstance(rows, DataFrame):
                raise UsageError.bad_return(fn.__name__, rows, None)
            prepared._write(_plan(prepared, mode, rows), write_options)

        return wrapper

    return decorator


def _several_outputs(
    migrations: Mapping[str, Migrations],
    modes: Mapping[str, Mode],
    write_options: Mapping[str, WriteOptions],
) -> Callable[[Callable[..., Mapping[str, object]]], Callable[..., None]]:
    outputs = sorted(migrations)

    def decorator(fn: Callable[..., Mapping[str, object]]) -> Callable[..., None]:
        signature = inspect.signature(fn)
        if missing := [name for name in outputs if name not in signature.parameters]:
            raise UsageError.output_not_found(fn.__name__, missing, list(signature.parameters))
        _check_return_annotation(fn, outputs)

        # keeps the signature, so @transform still maps inputs/outputs
        @functools.wraps(fn)
        def wrapper(*args: Any, **kwargs: Any) -> None:
            bound = signature.bind(*args, **kwargs)
            prepared = {name: m.prepare(bound.arguments[name]) for name, m in migrations.items()}
            for name, output in prepared.items():
                bound.arguments[name] = MigratedOutput(output, fn.__name__)
            rows = fn(*bound.args, **bound.kwargs)
            if not (isinstance(rows, dict) and sorted(rows) == outputs
                    and all(isinstance(df, DataFrame) for df in rows.values())):
                raise UsageError.bad_return(fn.__name__, rows, outputs)
            # plan (and schema-check) every output before writing any
            plans = {name: _plan(output, modes[name], rows[name]) for name, output in prepared.items()}
            for name, plan in plans.items():
                prepared[name]._write(plan, write_options.get(name))

        return wrapper

    return decorator


def _plan(prepared: PreparedOutput, mode: Mode, rows: DataFrame) -> PlannedWrite:
    return prepared.plan_write(rows) if mode == "append" else prepared.plan_rewrite(rows)


def _check_modes(modes: Mapping[str, object]) -> None:
    allowed = get_args(Mode.__value__)
    if bad := {name: mode for name, mode in modes.items() if mode not in allowed}:
        raise UsageError.bad_mode(f"got {bad}; each must be one of {list(allowed)}")


def _only_output(fn_name: str, arguments: dict[str, object]) -> str:
    """The one argument that is a Foundry output (inputs have no write_dataframe)."""
    outputs = [name for name, value in arguments.items() if hasattr(value, "write_dataframe")]
    if len(outputs) != 1:
        raise UsageError.not_one_output(fn_name, outputs)
    return outputs[0]


_OUTPUT = "output"  # the output's parameter name in the transform migrated_df builds


def migrated_df(
    migrations: Migrations,
    output: object,
    /,
    *,
    mode: Mode,
    write_options: WriteOptions | None = None,
    **inputs: object,
) -> Callable[[Callable[..., DataFrame]], Any]:
    """Use instead of @transform_df, with the same arguments plus `mode=` (see `migrated`).
    The function gets the inputs' DataFrames (and `ctx` if it asks) and returns its rows.
    (@transform_df can't be wrapped: it never passes the output to the function.)"""
    if not isinstance(migrations, Migrations):
        raise UsageError.not_migrations(migrations)
    _check_modes({"": mode})

    def decorator(fn: Callable[..., DataFrame]) -> Any:
        from transforms.api import transform  # type: ignore[import-not-found]  # Foundry only

        params = list(inspect.signature(fn).parameters)
        if _OUTPUT in inputs or set(params) - {"ctx"} != set(inputs):
            raise UsageError.df_parameters(fn.__name__, params, list(inputs), _OUTPUT)

        # Foundry maps inputs, the output and ctx to parameters by name
        signature = inspect.Signature(
            [inspect.Parameter(name, inspect.Parameter.POSITIONAL_OR_KEYWORD)
             for name in [*params, _OUTPUT]])

        def compute(*args: Any, **kwargs: Any) -> DataFrame:
            arguments = signature.bind(*args, **kwargs).arguments
            return fn(**{name: arguments[name].dataframe() if name in inputs else arguments[name]
                         for name in params})

        compute.__signature__ = signature  # type: ignore[attr-defined]
        compute.__name__, compute.__qualname__, compute.__doc__ = (
            fn.__name__, fn.__qualname__, fn.__doc__)
        compute.__module__ = fn.__module__
        wrapped = migrated(migrations, mode=mode, write_options=write_options)(compute)
        return transform(**{_OUTPUT: output}, **inputs)(wrapped)

    return decorator


def _check_return_annotation(fn: Callable[..., object], outputs: list[str]) -> None:
    """Raise unless fn is annotated to return a TypedDict of exactly `outputs`, all DataFrames."""
    try:
        returns = get_type_hints(fn).get("return")
    except NameError as error:  # e.g. a TypedDict defined inside a function
        raise UsageError.return_annotation(fn.__name__, outputs, f"can't resolve it ({error})")
    if not is_typeddict(returns):
        raise UsageError.return_annotation(fn.__name__, outputs, f"got {returns!r}")
    fields = get_type_hints(returns)
    if sorted(fields) != outputs:
        raise UsageError.return_annotation(fn.__name__, outputs, f"got fields {sorted(fields)}")
    if wrong := sorted(name for name, tp in fields.items() if tp is not DataFrame):
        raise UsageError.return_annotation(fn.__name__, outputs, f"{wrong} aren't DataFrame")
