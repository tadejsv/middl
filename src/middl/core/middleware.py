"""
Module defining middleware protocols and base classes.

This module provides the ProcessingStep protocol for the processing step and
the Middleware abstract base class for middleware components.
"""

from abc import ABC, abstractmethod
from collections.abc import Mapping
from typing import Any, Generic, Protocol, TypeVar

__all__ = ["Middleware", "ProcessingStep", "StrMapping"]


StrMapping = Mapping[str, Any]
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
