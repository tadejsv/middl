"""
Module defining middleware protocols and base classes.

This module provides the ProcessingStep protocol for the processing step and
the Middleware abstract base class for middleware components.
"""

from abc import ABC, abstractmethod
from collections.abc import MutableMapping
from typing import Any, Generic, Protocol, TypeVar

from .errors import ValidationError

__all__ = ["Middleware", "ProcessingStep", "StrMapping"]


StrMapping = MutableMapping[str, Any]
StateType_contra = TypeVar("StateType_contra", bound=StrMapping, contravariant=True)
DataType_contra = TypeVar("DataType_contra", bound=StrMapping, contravariant=True)


class ProcessingStep(Protocol[StateType_contra, DataType_contra]):
    """
    Protocol for a (callable) processing step of the pipeline.

    Needs to be a callable that accepts a shared state and a data batch,
    both as dictionaries (mappings) with string keys. Implementations should
    use concrete types for the state and data to leverage static type checking.
    """

    def __call__(self, state: StateType_contra, data: DataType_contra) -> None:
        """Perform the processing step."""


class Middleware(ABC, Generic[StateType_contra, DataType_contra]):
    """
    Abstract base class for middleware components.

    This class is generic over two types: one for the state and one for the data.
    Concrete middleware implementations should specify these generic types (typically
    as concrete TypedDict types) to benefit from IDE autocompletion and static type
    checking.

    Concrete implementations need to override the `wrap` method. Optionally, they
    can also override `on_start` and `on_finish` callbacks, in case there is some
    initialization/teardown that needs to be done at the start and at the end of
    pipeline execution.

    Attributes:
        requires_state_fields: The state fields required by this middleware.
        requires_data_fields_pre: The data fields required before processing.
        provides_data_fields_pre: The data fields provided after pre-processing.
        requires_data_fields_post: The data fields required after processing.
        provides_data_fields_post: The data fields provided after post-processing.

    Example:
    ```python
        from typing import TypedDict

        from middle import Middleware, StrMapping

        class MyState(TypedDict):
            step: int

        class MyData(TypedDict):
            loss: int

        class ExampleMiddleware(Middleware[MyState, MyData]):
            def __init__(self) -> None:
                super().__init__()
                self.requires_state_fields = {"step"}
                self.requires_data_fields_post = {"loss"}
                self.provides_data_fields_pre = {"value"}

            def wrap(
                self, next_step: ProcessingStep[StrMapping, StrMapping],
            ) -> ProcessingStep[MyState, MyData]:
                def wrapped(state: MyState, data: MyData) -> None:
                    print(f"Step: {state['step']}")
                    data["value"] = 1
                    next_step(state, data)
                    print(f"Loss: {data['loss']}")
                return wrapped
    ```

    """

    requires_state_fields: set[str]

    requires_data_fields_pre: set[str]
    provides_data_fields_pre: set[str]
    requires_data_fields_post: set[str]
    provides_data_fields_post: set[str]

    def __init__(self) -> None:
        """
        Initialize the middleware with empty sets for required and provided fields.
        """
        self.requires_state_fields = set()

        self.requires_data_fields_pre = set()
        self.provides_data_fields_pre = set()
        self.requires_data_fields_post = set()
        self.provides_data_fields_post = set()

    @abstractmethod
    def wrap(
        self,
        next_step: ProcessingStep[StrMapping, StrMapping],
    ) -> ProcessingStep[StateType_contra, DataType_contra]:
        """
        Wrap the next processing step in the middleware chain with this
        middleware's processing logic.

        Concrete implementations should return a callable that applies
        this middleware's behavior before and/or after invoking the next callable.

        Args:
            next_step: The next callable in the processing chain. This callable accepts
                a state and data and returns None.

        Returns:
            A callable that wraps the next_step with this middleware's behavior.

        Example:
            A simple middleware implementation that prints messages before and after
            invoking the next step:

                def wrap(self, next_step):
                    def wrapped(state, data):
                        print("Before calling next step")
                        next_step(state, data)
                        print("After calling next step")
                    return wrapped

        """
        raise NotImplementedError

    def on_start(self, state: StateType_contra) -> None:
        """
        Callback invoked at the start of the pipeline execution.

        This method is called once before any data is processed by the pipeline.
        Middleware implementations can override this method to any tasks needed
        before the processing begins - for example, a progress bar middleware
        would create (display on screen) a progress bar at this point.

        Args:
            state: A mapping representing the shared state of the pipeline.

        """

    def on_finish(self, state: StateType_contra) -> None:
        """
        Callback invoked at the end of the pipeline execution.

        This method is called once after all data batches have been processed or when
        the pipeline terminates prematurely. Middleware implementations can override
        this method to perform any necessary finalization tasks - for example, a
        progress bar middleware would remove a progress bar at this point.

        Args:
            state: A mapping representing the shared state of the pipeline.

        """

    def _validate(
        self, state_fields: set[str], data_fields: set[str], pre: bool = True
    ) -> None:
        """
        Validate the fields of the middleware, given input fields.

        This method checks that all the fields required by the middleware are
        present in the input fields.

        Arguments:
            state_fields: Fields of the input state mapping.
            data_fields: Fields of the input data mapping.
            pre: If `True`, perform the validation for the forward pass (pre),
                if `False`, perform the validation for the backward pass (post).

        """
        if not self.requires_state_fields.issubset(state_fields):
            msg = (
                "Missing state fields"
                f" {self.requires_state_fields.difference(state_fields)}"
            )
            raise ValidationError(msg)

        if pre:
            if not self.requires_data_fields_pre.issubset(data_fields):
                msg = (
                    "Missing data pre fields"
                    f" {self.requires_data_fields_pre.difference(data_fields)}"
                )
                raise ValidationError(msg)

            data_fields.update(self.provides_data_fields_pre)
        else:
            if not self.requires_data_fields_post.issubset(data_fields):
                msg = (
                    "Missing data post fields"
                    f" {self.requires_data_fields_post.difference(data_fields)}"
                )
                raise ValidationError(msg)

            data_fields.update(self.provides_data_fields_post)
