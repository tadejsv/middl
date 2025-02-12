"""
Module for running a pipeline of middleware components in an ML context.

This module provides classes and exceptions for building and running a pipeline where
batches from a data loader are processed sequentially through a series of middleware
components, which receive a shared state and batch (data) as their input.
"""

from collections.abc import MutableMapping, Sequence, Sized
from typing import Any

from .errors import AbortPipeline, SkipStep, ValidationError
from .loader import Loader
from .middleware import Middleware, ProcessingStep, StrMapping

__all__ = ["Pipeline", "PipelineWrapper"]


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

    def validate(
        self,
        state_fields: set[str],
        data_fields: set[str],
        sized_data_loader: bool,
    ) -> None:
        """
        Perform a "dry run" validation of the pipeline to check that all required
        fields will be present in the state and data at the correct stages.

        This method verifies that each middleware's required fields are satisfied
        by performing two passes through the list of middlewares:

        1. **Forward Pass (pre=True)**:
            - Iterates over the middlewares in the order they are provided.
            - Each middleware checks whether the current `state_fields` and
              `data_fields` satisfy its requirements for the "pre" phase (input fields).
            - If a middleware contributes new fields to the state or data on the way in,
              those fields are added to the respective sets for subsequent middlewares.
              Typically, middleware should not change the state fields, with the
              exception of `PipelineWrapper` (which adds its own step/num_steps fields).

        2. **Reverse Pass (pre=False)**:
            - Iterates over the middlewares in reverse order.
            - Each middleware checks whether the (now possibly updated) `state_fields`
              and `data_fields` satisfy its requirements for the "post" phase.
            - If a middleware introduces any additional fields on the way out, those
              fields are also accumulated for the previous middlewares. Again,
              middlewares should not add or alter state fields here.

        This method always adds `step_name` to `state_fields`. If `sized_data_loader`
        is True, it also adds `num_{step_name}s`.

        Args:
            state_fields: The set of keys that the pipeline expects in the shared
               `state` object at the start of processing.
            data_fields: The set of keys that each data batch is expected to have
                when produced by the data loader.
            sized_data_loader: Whether or not the data loader has a length, which
                determines if `num_{step_name}s` will be added to the state.

        Raises:
            ValidationError: If any required fields are missing in either the state
                or the data at any stage of validation.

        """
        # Create copy of fields sets to avoid modifying the original
        data_fields = set(data_fields)
        state_fields = set(state_fields)

        state_fields.add(self.step_name)
        if sized_data_loader:
            state_fields.add(f"num_{self.step_name}s")

        for i, mware in enumerate(self.middlewares):
            try:
                mware._validate(state_fields, data_fields, pre=True)  # noqa: SLF001
            except ValidationError as e:
                msg = (
                    "Validation error by middleware"
                    f" {type(mware).__name__}, at index {i}."
                )
                raise ValidationError(msg) from e

        for i, mware in enumerate(reversed(self.middlewares)):
            try:
                mware._validate(state_fields, data_fields, pre=False)  # noqa: SLF001
            except ValidationError as e:
                ind = len(self.middlewares) - i - 1
                msg = (
                    "Validation error by middleware"
                    f" {type(mware).__name__}, at index {ind}."
                )
                raise ValidationError(msg) from e

    def run(
        self,
        state: MutableMapping[str, Any],
        data_loader: Loader,
        validate: bool = True,
    ) -> None:
        """
        Process batches from the data loader through the middleware chain.

        The pipeline processes batches sequentially. Before looping over the
        data loader, if `validate=True`, the pipeline calls `validate` to checks
        that the state and the data contain all fields required by the middlewares.

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
            validate: Whether to validate the required fields before starting the
                main loop. (Default: True)

        """
        for mware in self.middlewares:
            mware.on_start(state)

        if isinstance(data_loader, Sized):
            state[f"num_{self.step_name}s"] = len(data_loader)

        if validate:
            self.validate(
                set(state.keys()),
                data_loader.data_fields,
                isinstance(data_loader, Sized),
            )

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


class PipelineWrapper(Middleware[StrMapping, StrMapping]):
    """
    A middleware that encapsulates an entire `Pipeline` as a single middleware step.

    The wrapped pipeline is run in the "pre" step (forward pass), before the outer
    pipeline calls `next_step`. This means that for each batch passed to this
    middleware, it will first execute the wrapped pipeline by calling
    `pipeline.run` on the provided `data_loader`, and then continue to the next
    component in the outer pipeline.
    """

    def __init__(self, pipeline: Pipeline, data_loader: Loader) -> None:
        """
        Initialize a `PipelineWrapper` instance.

        Args:
            pipeline: The pipeline to be run when this middleware is invoked.
            data_loader: The data loader consumed by the wrapped pipeline.

        """
        super().__init__()
        self.pipeline = pipeline
        self.data_loader = data_loader

    def _validate(
        self,
        state_fields: set[str],
        data_fields: set[str],  # noqa: ARG002
        pre: bool = True,
    ) -> None:
        """
        Validate that the wrapped pipeline has all the fields it needs.

        During the "pre" phase, this method calls the wrapped pipeline's
        validation to ensure that `state_fields` and `data_fields` are sufficient
        for its own middlewares. This is effectively a nested validation pass.

        Args:
            state_fields: Current set of state fields available in the outer pipeline.
            data_fields: Current set of data fields available in the outer pipeline.
            pre: Whether this validation pass is happening before or after the batch
                is processed. The pipeline's validation is only performed during the
                pre phase.

        Raises:
            ValidationError: If the wrapped pipeline determines that required fields
                are missing.

        """
        if pre:
            size_data_loader = isinstance(self.data_loader, Sized)
            self.pipeline.validate(
                state_fields, self.data_loader.data_fields, size_data_loader
            )

    def wrap(
        self, next_step: ProcessingStep[StrMapping, StrMapping]
    ) -> ProcessingStep[StrMapping, StrMapping]:
        """
        Wrap the next step in a function that runs the wrapped pipeline first.

        This method creates and returns a new processing function. When called, the
        function will:
          1. Execute the wrapped pipeline by calling `pipeline.run` on the provided
             `data_loader`, skipping validation (since it's handled separately).
          2. Call `next_step` to continue with the rest of the outer pipeline.

        Args:
            next_step: The subsequent processing step in the outer pipeline.

        Returns:
            A callable that, when invoked with `(state, data)`, runs the wrapped
            pipeline and then calls `next_step`.

        """

        def wrapped(state: StrMapping, data: StrMapping) -> None:
            self.pipeline.run(state, self.data_loader, validate=False)
            next_step(state, data)

        return wrapped
