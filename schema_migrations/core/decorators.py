"""Decorators that prepare outputs before a transform runs and write what it returns."""

import functools
import inspect
from collections.abc import Callable, Mapping
from typing import TYPE_CHECKING, get_type_hints, is_typeddict

from pyspark.sql import DataFrame
from pyspark.sql import types as T

from ..exceptions import (
    NotADataFrameError,
    OutputNotFoundError,
    ReturnAnnotationError,
    ReturnedOutputsError,
)
from ..foundry.protocols import WriteOptions

if TYPE_CHECKING:
    from .migrations import Migrations


def _migrated_one[**TransformParams](
    migrations: "Migrations",
    output: str | None,
    previous_schema: T.StructType | None,
    write_options: WriteOptions | None,
) -> Callable[[Callable[TransformParams, DataFrame]], Callable[TransformParams, None]]:
    """Implementation of `Migrations.migrated`."""

    def decorator(
        fn: Callable[TransformParams, DataFrame],
    ) -> Callable[TransformParams, None]:
        signature = inspect.signature(fn)
        if output is not None and output not in signature.parameters:
            raise OutputNotFoundError(fn.__name__, list(signature.parameters), output)

        # keeps the signature, so @transform still maps inputs/outputs
        @functools.wraps(fn)
        def wrapper(
            *args: TransformParams.args, **kwargs: TransformParams.kwargs
        ) -> None:
            arguments = signature.bind(*args, **kwargs).arguments
            out = arguments[output or _only_output(fn, arguments)]
            prepared = migrations.prepare(out, previous_schema)
            new_rows = fn(*args, **kwargs)
            if not isinstance(new_rows, DataFrame):
                raise NotADataFrameError(
                    f"@migrated: the return value of '{fn.__name__}'", new_rows
                )
            prepared.write(new_rows, write_options)

        return wrapper

    return decorator


def migrated[**TransformParams](
    migrations: Mapping[str, "Migrations"],
    previous_schemas: Mapping[str, T.StructType] | None = None,
    write_options: Mapping[str, WriteOptions] | None = None,
) -> Callable[
    [Callable[TransformParams, Mapping[str, object]]], Callable[TransformParams, None]
]:
    """Decorator for a transform with several migrated outputs. WRITES every output in
    `migrations` (see `PreparedOutput.write`): appends the returned rows, or rewrites the
    whole output after a migration. The function must not write to them itself.

    migrations:       output parameter name -> that output's Migrations.
    previous_schemas: output name -> schema for out.dataframe("previous", schema=...).
    write_options:    output name -> extra keyword arguments for out.write_dataframe.

    The function is annotated to return a TypedDict with one DataFrame field per output
    (checked at import) and returns the new rows in it. Every output is prepared before the
    function runs; all returned rows are checked before any is written. The decorated
    transform returns None.
    """
    schemas, options = previous_schemas or {}, write_options or {}

    def decorator(
        fn: Callable[TransformParams, Mapping[str, object]],
    ) -> Callable[TransformParams, None]:
        signature = inspect.signature(fn)
        for name in migrations:
            if name not in signature.parameters:
                raise OutputNotFoundError(fn.__name__, list(signature.parameters), name)
        _check_return_annotation(fn, sorted(migrations))

        # keeps the signature, so @transform still maps inputs/outputs
        @functools.wraps(fn)
        def wrapper(
            *args: TransformParams.args, **kwargs: TransformParams.kwargs
        ) -> None:
            arguments = signature.bind(*args, **kwargs).arguments
            prepared = {
                name: m.prepare(arguments[name], schemas.get(name))
                for name, m in migrations.items()
            }
            new_rows = fn(*args, **kwargs)
            if not isinstance(new_rows, dict) or new_rows.keys() != prepared.keys():
                raise ReturnedOutputsError(fn.__name__, new_rows, sorted(prepared))
            for name, rows in new_rows.items():
                if not isinstance(rows, DataFrame):
                    raise NotADataFrameError(
                        f"@migrated: '{fn.__name__}' output '{name}'", rows
                    )
            for name, output in prepared.items():
                output.write(new_rows[name], options.get(name))

        return wrapper

    return decorator


def _check_return_annotation(fn: Callable[..., object], outputs: list[str]) -> None:
    """Raise unless fn is annotated to return a TypedDict of exactly `outputs`, all DataFrames."""
    try:
        returns = get_type_hints(fn).get("return")
    except NameError as error:  # e.g. a TypedDict defined inside a function
        raise ReturnAnnotationError(fn.__name__, outputs, f"can't resolve it ({error})")
    if not is_typeddict(returns):
        raise ReturnAnnotationError(fn.__name__, outputs, f"got {returns!r}")
    fields = get_type_hints(returns)
    if sorted(fields) != outputs:
        raise ReturnAnnotationError(
            fn.__name__, outputs, f"got fields {sorted(fields)}"
        )
    if wrong := sorted(name for name, tp in fields.items() if tp is not DataFrame):
        raise ReturnAnnotationError(fn.__name__, outputs, f"{wrong} aren't DataFrame")


def _only_output(fn: Callable[..., object], arguments: dict[str, object]) -> str:
    outputs = [
        param for param, value in arguments.items() if hasattr(value, "write_dataframe")
    ]
    if len(outputs) != 1:
        raise OutputNotFoundError(fn.__name__, outputs)
    return outputs[0]
