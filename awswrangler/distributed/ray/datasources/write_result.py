"""Ray write results."""

from __future__ import annotations

import itertools
from dataclasses import dataclass
from typing import Callable, Generic, Iterator, TypeVar

from pandas import DataFrame
from ray.data._internal.execution.interfaces.task_context import TaskContext
from ray.data.block import Block, BlockAccessor
from ray.data.datasource.datasink import Datasink
from ray.data.datasource.datasource import Datasource

WriteReturnType = TypeVar("WriteReturnType")
"""Generic type for the return value of `Datasink.write`."""


@dataclass
class WriteResult(Generic[WriteReturnType]):
    """Aggregated result of the Datasink write operations."""

    # Total number of written rows.
    num_rows: int
    # Total size in bytes of written data.
    size_bytes: int
    # All returned values of `Datasink.write`.
    write_returns: list[WriteReturnType]

    @staticmethod
    def aggregate_write_results(write_results: list["WriteResult"]) -> "WriteResult":
        """Aggregate a list of write results.

        Args:
            write_results: A list of write results.

        Returns
        -------
            A single write result that aggregates the input results.
        """
        total_num_rows = 0
        total_size_bytes = 0
        write_returns = []

        for write_result in write_results:
            total_num_rows += write_result.num_rows
            total_size_bytes += write_result.size_bytes
            if getattr(write_result, "write_returns", None):
                write_returns.append(write_result.write_returns)

        return WriteResult(
            num_rows=total_num_rows,
            size_bytes=total_size_bytes,
            write_returns=write_returns,
        )


def gen_datasink_write_result(
    write_result_blocks: list[Block],
) -> WriteResult:
    """Get datasink write result."""
    assert all(isinstance(block, DataFrame) and len(block) == 1 for block in write_result_blocks)
    total_num_rows = sum(result["num_rows"].sum() for result in write_result_blocks)
    total_size_bytes = sum(result["size_bytes"].sum() for result in write_result_blocks)

    write_returns = [result["write_return"][0] for result in write_result_blocks]
    return WriteResult(total_num_rows, total_size_bytes, write_returns)


def generate_write_fn(
    datasink_or_legacy_datasource: Datasink | Datasource, **write_args
) -> Callable[[Iterator[Block], TaskContext], Iterator[Block]]:
    """Generate write function."""

    def fn(blocks: Iterator[Block], ctx: TaskContext) -> Iterator[Block]:
        """Write the blocks to the given datasink or legacy datasource.

        Outputs the original blocks to be written.
        """
        # Create a copy of the iterator, so we can return the original blocks.
        it1, it2 = itertools.tee(blocks, 2)
        if isinstance(datasink_or_legacy_datasource, Datasink):
            ctx._datasink_write_return = datasink_or_legacy_datasource.write(it1, ctx)
        else:
            datasink_or_legacy_datasource.write(it1, ctx, **write_args)

        return it2

    return fn


def generate_collect_write_stats_fn() -> Callable[[Iterator[Block], TaskContext], Iterator[Block]]:
    """Generate write stats."""

    # If the write op succeeds, the resulting Dataset is a list of
    # one Block which contain stats/metrics about the write.
    # Otherwise, an error will be raised. The Datasource can handle
    # execution outcomes with `on_write_complete()`` and `on_write_failed()``.
    def fn(blocks: Iterator[Block], ctx: TaskContext) -> Iterator[Block]:
        """Handle stats collection for block writes."""
        block_accessors = [BlockAccessor.for_block(block) for block in blocks]
        total_num_rows = sum(ba.num_rows() for ba in block_accessors)
        total_size_bytes = sum(ba.size_bytes() for ba in block_accessors)

        # NOTE: Write tasks can return anything, so we need to wrap it in a valid block
        # type.
        import pandas as pd

        block = pd.DataFrame(
            {
                "num_rows": [total_num_rows],
                "size_bytes": [total_size_bytes],
                "write_result": [ctx._datasink_write_return],
            }
        )
        return iter([block])

    return fn
