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
from .middleware import Middleware, StrMapping

__all__ = ["Pipeline", "PipelineWrapper"]


class Pipeline:
    """
    Runs sequentially a series of middleware components to process data.
    """

    def __init__(
        self,
        middlewares: Sequence[Middleware],  # type: ignore[type-arg]
        step_name: str = "step",
    ) -> None:
        """
        Initialize the pipeline.

        Args:
            middlewares: A sequence of middleware components.
            step_name: The key used to record the current step index in the
                state. (Default: "step")

        """
        self.middlewares = middlewares
        self.step_name = step_name

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
        by performing a forward pass through the list of middlewares. At each step,
        the middleware's `_validate` method is called with `state_fields` and
        `data_fields`, in which the middleware checks that all required fields are
        present, and optionally adds any fields that it provides for downstream
        middlewares.

        This method always adds `step_name` to `state_fields`. If `sized_data_loader`
        is True, it also adds `num_{step_name}s`. Middlewares are not expected to
        alter `state_fields`, with the exception of `PipelineWrapper`.

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
                mware._validate(state_fields, data_fields)  # noqa: SLF001
            except ValidationError as e:
                msg = (
                    "Validation error by middleware"
                    f" {type(mware).__name__}, at index {i}."
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
        given in the list.

        If a middleware raises an AbortPipeline exception, processing stops immediately.
        If a middleware raises a SkipStep exception, that step is skipped and
        processing continues with the next step.

        On each step, the current step index is recorded in the state using the key
        specified by `step_name`. If data loader has a length, this is added to state
        under the key `num_{step_name}s` - for example, if `step_name="step"`, this
        would be `num_steps`.

        Before the processing begins, the `on_start` hook of each middleware is
        called, and after all processing finishes (also in the case of `AbortPipeline`
        exception being raised), the `on_finish` hook of each middlewar is called. The
        shared state is provided to each call. Both of these calls will call middlewares
        in the order they are provided. Also, the `on_start` call will happen
        **before** any validation takes place.

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
                for mware in self.middlewares:
                    mware.step(state=state, data=data)
            except AbortPipeline:
                break
            except SkipStep:
                continue

        for mware in self.middlewares:
            mware.on_finish(state)


class PipelineWrapper(Middleware[StrMapping, StrMapping]):
    """
    A middleware that encapsulates an entire `Pipeline` as a single middleware step.
    """

    def __init__(self, pipeline: Pipeline, data_loader: Loader) -> None:
        """
        Initialize a `PipelineWrapper` instance.

        Args:
            pipeline: The pipeline to be run when this middleware's `step`
                function is invoked.
            data_loader: The data loader consumed by the pipeline.

        """
        super().__init__()
        self.pipeline = pipeline
        self.data_loader = data_loader

    def _validate(
        self,
        state_fields: set[str],
        data_fields: set[str],  # noqa: ARG002
    ) -> None:
        """
        Validate that the wrapped pipeline has all the fields it needs.

        This method runs the pipeline's `validate` method, using the provided
        state fields ()

        Args:
            state_fields: Current set of state fields available in the outer pipeline.
            data_fields: Current set of data fields available in the outer pipeline.
                These are ignored when validating the pipeline, instead fields from the
                data loader are used as data fields.

        Raises:
            ValidationError: If the pipeline determines that required fields
                are missing.

        """
        size_data_loader = isinstance(self.data_loader, Sized)
        self.pipeline.validate(
            state_fields, self.data_loader.data_fields, size_data_loader
        )

    def step(self, state: StrMapping, data: StrMapping) -> None:  # noqa: ARG002
        """
        Run the pipeline as a middleware processing step.

        Args:
            state: A dictionary representing the shared pipeline state, will be passed
                to the pipeline.
            data: A dictionary representing step data - will not be used by the
                pipeline.

        """
        self.pipeline.run(state, self.data_loader, validate=False)
