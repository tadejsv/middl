"""
Module defining middleware protocols and base classes.

This module provides the Middleware abstract base class for middleware components.
"""

from abc import ABC
from collections.abc import MutableMapping
from typing import Any, Generic, TypeVar

from .errors import ValidationError

__all__ = ["Middleware", "StrMapping"]


StrMapping = MutableMapping[str, Any]
StateType_contra = TypeVar("StateType_contra", bound=StrMapping, contravariant=True)
DataType_contra = TypeVar("DataType_contra", bound=StrMapping, contravariant=True)


class Middleware(ABC, Generic[StateType_contra, DataType_contra]):
    """
    Abstract base class for middleware components.

    This class is generic over two types: one for the state and one for the data.
    Concrete middleware implementations should specify these generic types (typically
    as concrete TypedDict types) to benefit from IDE autocompletion and static type
    checking.

    Concrete implementations should usually override the `step` method. Optionally, they
    can also override `on_start` and `on_finish` callbacks, in case there is some
    initialization/teardown that needs to be done at the start and at the end of
    pipeline execution.

    Attributes:
        requires_state_fields: The state fields required.
        requires_data_fields: The data fields required.
        provides_data_fields: The data fields provided.

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
                self.requires_data_fields = {"loss"}
                self.provides_data_fields = {"value"}

            def step(self, state: MyState, data: MyData) -> None:
                print(f"Step: {state['step']}")
                print(f"Loss: {data['loss']}")
                data["value"] = 1
    ```

    """

    requires_state_fields: set[str]

    requires_data_fields: set[str]
    provides_data_fields: set[str]

    def __init__(self) -> None:
        """
        Initialize the middleware with empty sets for required and provided fields.
        """
        self.requires_state_fields = set()
        self.requires_data_fields = set()
        self.provides_data_fields = set()

    def step(self, state: StateType_contra, data: DataType_contra) -> None:
        """
        Run the middleware processing step.

        Args:
            state: A dictionary representing the shared pipeline state.
            data: A dictionary representing step data (batch).

        """

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

    def _validate(self, state_fields: set[str], data_fields: set[str]) -> None:
        """
        Validate the fields of the middleware, given input fields.

        This method checks that all the fields required by the middleware are
        present in the input fields.

        Arguments:
            state_fields: Fields of the input state mapping.
            data_fields: Fields of the input data mapping.

        """
        if not self.requires_state_fields.issubset(state_fields):
            msg = (
                "Missing state fields"
                f" {self.requires_state_fields.difference(state_fields)}"
            )
            raise ValidationError(msg)

        if not self.requires_data_fields.issubset(data_fields):
            msg = (
                "Missing data fields"
                f" {self.requires_data_fields.difference(data_fields)}"
            )
            raise ValidationError(msg)

        data_fields.update(self.provides_data_fields)
