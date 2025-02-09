"""
Module for running a pipeline of middleware components in an ML context.

This module provides classes and exceptions for building and running a pipeline where
batches from a data loader are processed sequentially through a series of middleware
components, which receive a shared state and batch (data) as their input.
"""

from collections.abc import Iterable, Sequence
from typing import Any

from .middleware import Middleware, ProcessingStep, StrMapping

__all__ = ["AbortPipeline", "Pipeline", "SkipStep", "ValidationError", "empty_sink"]


class SkipStep(Exception):  # noqa: N818
    """
    Exception to skip processing of the current batch.

    When raised by a middleware, the pipeline skips the current step and
    continues with the next one.
    """


class AbortPipeline(Exception):  # noqa: N818
    """
    Exception to abort the entire pipeline execution.

    When raised by a middleware, the pipeline immediately exits.
    """


class ValidationError(Exception):
    """
    Exception raised when pipeline validation fails due to missing required fields.

    This exception indicates that the state or the batch from the data loader is missing
    fields required by one of the middlewares.
    """


class Pipeline:
    """
    Runs sequentially a series of middleware components to process data.

    The Pipeline builds a chain of middleware by wrapping a final sink callable with
    each middleware in reverse order so that they execute in the provided order.
    """

    def __init__(
        self,
        middlewares: Sequence[Middleware],  # type: ignore[type-arg]
        sink: ProcessingStep,  # type: ignore[type-arg]
        step_name: str = "step",
    ) -> None:
        """
        Initialize the pipeline.

        The sink callable is wrapped by each middleware component (in reverse order) so
        that they execute sequentially in the order provided when processing a batch
        rom the data loader.

        Args:
            middlewares: A sequence of middleware components.
            sink: The final callable that completes processing of the batch.
            step_name: The key used to record the current step index in the
                state. (Default: "step")

        """
        self.middlewares = middlewares
        self.step_name = step_name

        self._run = sink
        for mware in reversed(self.middlewares):
            self._run = mware.wrap(self._run)

    def validate(self, state_fields: set[str], data_fields: set[str]) -> None:
        """
        Validate that the state and the batch from the data loader contain all required
        fields.

        It follows the flow of data through the pipeline by performing two passes:
        1. Forward pass (toward the sink): for each middleware in order, it checks that
           the state contains the middleware's required fields and that the data
           contains the required pre fields. It then accumulates the pre fields that the
           middleware provides.
        2. Reverse pass (away from the sink): for each middleware in reverse order,
           it verifies that the accumulated batch fields include the middleware's
           required post fields, and then accumulates any post fields that the
           middleware provides.

        Raises:
            ValidationError: If any required fields are missing.

        """
        data_fields_acc = set(data_fields)

        for i, mware in enumerate(self.middlewares):
            if not mware.requires_state_fields.issubset(state_fields):
                msg = (
                    "Missing state fields"
                    f" {mware.requires_state_fields.difference(state_fields)}, required"
                    f" by middleware {type(mware).__name__}, at index {i}."
                )
                raise ValidationError(msg)
            if not mware.requires_data_fields_pre.issubset(data_fields_acc):
                msg = (
                    "Missing data pre fields"
                    f" {mware.requires_data_fields_pre.difference(data_fields_acc)},"
                    f" required by middleware {type(mware).__name__}, at index {i}."
                )
                raise ValidationError(msg)

            data_fields_acc.update(mware.provides_data_fields_pre)

        for i, mware in enumerate(reversed(self.middlewares)):
            if not mware.requires_data_fields_post.issubset(data_fields_acc):
                ind = len(self.middlewares) - i - 1
                msg = (
                    "Missing data post fields"
                    f" {mware.requires_data_fields_post.difference(data_fields_acc)},"
                    f" required by middleware {type(mware).__name__}, at index {ind}."
                )
                raise ValidationError(msg)

            data_fields_acc.update(mware.provides_data_fields_post)

    def run(
        self,
        state: dict[str, Any],
        data_loader: Iterable[StrMapping],
        validate: bool = True,
    ) -> None:
        """
        Process batches from the data loader through the middleware chain.

        The pipeline processes batches sequentially. On the first batch, if
        `validate=True`, the pipeline calls `validate` to checks that the state
        and the data contain all required fields.

        The batch is then passed through the middleware chain for processing. The data
        passes through the middlewares sequentially, in the order in which they are
        given in the list. Each middleware "passes" the control on to the next one by
        calling `next_step` in its wrapped function. After the first pass is complete,
        the sink function is ran.

        Then, the flow passes backwards: from the last middleware to the first one, with
        execution in each middleware continuing after the call to `next_step`.

        Here is an illustration of this flow:

        ```
        (data, state)
            ▼
        ┌────────────────┐
        │ Middleware 1   │
        └────────────────┘
            ▼          ▲
        ┌────────────────┐
        │       ...      │
        └────────────────┘
            ▼          ▲
        ┌────────────────┐
        │ Middleware N   │
        └────────────────┘
            ▼          ▲
        ┌────────────────┐
        │      Sink      │
        └────────────────┘
        ```

        If a middleware raises an AbortPipeline exception, processing stops immediately.
        If a middleware raises a SkipStep exception, that batch is skipped and
        processing continues with the next batch.

        On each step, the current step index is recorded in the state using the key
        specified by `step_name`.

        Args:
            state: A dictionary representing the shared pipeline state.
            data_loader: An iterable that yields batches of data (as dictionaries).
            validate: Whether to validate the required fields on the first batch from
                the data loader. (Default: True)

        """
        for step, data in enumerate(data_loader):
            state[self.step_name] = step
            if step == 0 and validate:
                self.validate(set(state.keys()), set(data.keys()))

            try:
                self._run(state=state, data=data)
            except AbortPipeline:
                break
            except SkipStep:
                continue


def empty_sink(state: StrMapping, data: StrMapping) -> None:  # noqa: ARG001
    """An empty processing step (sink)."""
    return
