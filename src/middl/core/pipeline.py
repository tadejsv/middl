"""
Module for running a pipeline of middleware components in an ML context.

This module provides classes and exceptions for building and running a pipeline where
batches from a data loader are processed sequentially through a series of middleware
components, which receive a shared state and batch (data) as their input.
"""

from collections.abc import Sequence, Sized
from typing import Any

from .errors import AbortPipeline, SkipStep, ValidationError
from .loader import Loader
from .middleware import Middleware, StrMapping

__all__ = ["Pipeline"]


class Pipeline:
    """
    Runs sequentially a series of middleware components to process data.

    The Pipeline builds a chain of middlewares by wrapping each middleware in reverse
    order (last middleware wraps an empty processing step) so that they execute in
    the provided order.
    """

    def __init__(
        self,
        middlewares: Sequence[Middleware],  # type: ignore[type-arg]
        step_name: str = "step",
    ) -> None:
        """
        Initialize the pipeline.

        An empty sink callable is wrapped by each middleware component (in reverse
        order), so that they execute sequentially in the order provided when
        processing a batch from the data loader.

        Args:
            middlewares: A sequence of middleware components.
            step_name: The key used to record the current step index in the
                state. (Default: "step")

        """
        self.middlewares = middlewares
        self.step_name = step_name

        self._run = _empty_sink
        for mware in reversed(self.middlewares):
            self._run = mware.wrap(self._run)

    def validate(self, state_fields: set[str], data_fields: set[str]) -> None:
        """
        Validate that the state and the batch from the data loader contain all required
        fields.

        It follows the flow of data through the pipeline by performing two passes:
        1. Forward pass: for each middleware in order, it checks that
           the state contains the middleware's required fields and that the data
           contains the required pre fields. It then accumulates the pre fields that the
           middleware provides.
        2. Reverse pass: for each middleware in reverse order,
           it verifies that the accumulated batch fields include the middleware's
           required post fields, and then accumulates any post fields that the
           middleware provides.

        Raises:
            ValidationError: If any required fields are missing.

        """
        # Create copy of fields sets to avoid modifying the original
        data_fields = set(data_fields)
        state_fields = set(state_fields)

        for i, mware in enumerate(self.middlewares):
            try:
                mware.validate(state_fields, data_fields, pre=True)
            except ValidationError as e:
                msg = (
                    "Validation error by middleware"
                    f" {type(mware).__name__}, at index {i}."
                )
                raise ValidationError(msg) from e

        for i, mware in enumerate(reversed(self.middlewares)):
            try:
                mware.validate(state_fields, data_fields, pre=False)
            except ValidationError as e:
                ind = len(self.middlewares) - i - 1
                msg = (
                    "Validation error by middleware"
                    f" {type(mware).__name__}, at index {ind}."
                )
                raise ValidationError(msg) from e

    def run(
        self,
        state: dict[str, Any],
        data_loader: Loader,
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
        calling `next_step` in its wrapped function. The final middleware does not need
        to call next; in case it does, it simply calls an empty processing step.

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
        ```

        If a middleware raises an AbortPipeline exception, processing stops immediately.
        If a middleware raises a SkipStep exception, that batch is skipped and
        processing continues with the next batch.

        On each step, the current step index is recorded in the state using the key
        specified by `step_name`. If data loader has a length, this is added to state
        under the key `num_{step_name}s` - for example, if `step_name="step"`, this
        would be `num_steps`.

        Before the processing begins, the `on_start` hook of each middleware is
        called, and after all processing finishes (also in the case of `AbortPipeline`
        exception being raised), the `on_finish` hook of each middlewar is called. The
        shared state is provided to each call. Both of these calls will call middlewares
        in the order they are provided (unlike processing, where on the backwards pass
        the order is reversed). Also, the `on_start` call will happen **before** any
        validation takes place.

        Args:
            state: A dictionary representing the shared pipeline state.
            data_loader: An iterable that yields batches of data (as dictionaries).
            validate: Whether to validate the required fields on the first batch from
                the data loader. (Default: True)

        """
        for mware in self.middlewares:
            mware.on_start(state)

        if isinstance(data_loader, Sized):
            state[f"num_{self.step_name}s"] = len(data_loader)

        if validate:
            self.validate(set(state.keys()), data_loader.data_fields)

        for step, data in enumerate(data_loader):
            state[self.step_name] = step

            try:
                self._run(state=state, data=data)
            except AbortPipeline:
                break
            except SkipStep:
                continue

        for mware in self.middlewares:
            mware.on_finish(state)


def _empty_sink(state: StrMapping, data: StrMapping) -> None:  # noqa: ARG001
    """An empty processing step (sink)."""
    return
