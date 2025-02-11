from typing import Any
from unittest.mock import MagicMock

import pytest

from middl import (
    AbortPipeline,
    EmptyGenerator,
    Middleware,
    Pipeline,
    ProcessingStep,
    SkipStep,
    ValidationError,
)


class AccMiddleware(Middleware[Any, Any]):
    def wrap(self, next_step: ProcessingStep[Any, Any]) -> ProcessingStep[Any, Any]:
        def wrapped(state: Any, data: Any) -> None:
            next_step(state, data)
            state["acc"].append(state["value"])

        return wrapped


class _SimpleMiddleware(Middleware[Any, Any]):
    def wrap(self, next_step: ProcessingStep[Any, Any]) -> ProcessingStep[Any, Any]:
        def wrapped(state: Any, data: Any) -> None:
            return next_step(state, data)

        return wrapped


def test_validate_missing_pre_field() -> None:
    sm = _SimpleMiddleware()
    sm.requires_data_fields_pre = {"miss"}

    pipe = Pipeline(middlewares=[sm])

    with pytest.raises(
        ValidationError,
        match="Missing data pre fields {'miss'}, required by middleware"
        " _SimpleMiddleware, at index 0.",
    ):
        pipe.validate(set(), set())


def test_validate_missing_post_field() -> None:
    sm = _SimpleMiddleware()
    sm.requires_data_fields_post = {"miss"}

    pipe = Pipeline(middlewares=[sm])

    with pytest.raises(
        ValidationError,
        match="Missing data post fields {'miss'}, required by middleware"
        " _SimpleMiddleware, at index 0.",
    ):
        pipe.validate(set(), set())


def test_validate_missing_state_field() -> None:
    sm = _SimpleMiddleware()
    sm.requires_state_fields = {"miss"}

    pipe = Pipeline(middlewares=[sm])

    with pytest.raises(
        ValidationError,
        match="Missing state fields {'miss'}, required by middleware"
        " _SimpleMiddleware, at index 0.",
    ):
        pipe.validate(set(), set())


def test_validate_ok() -> None:
    sm1 = _SimpleMiddleware()
    sm1.requires_state_fields = {"miss"}
    sm1.provides_data_fields_pre = {"val"}
    sm1.requires_data_fields_post = {"val", "val1"}

    sm2 = _SimpleMiddleware()
    sm2.requires_data_fields_pre = {"val"}
    sm2.provides_data_fields_post = {"val1"}

    pipe = Pipeline(middlewares=[sm1, sm2])

    pipe.validate({"miss"}, set())


def test_run_missing_state_field() -> None:
    sm = _SimpleMiddleware()
    sm.requires_state_fields = {"miss"}

    pipe = Pipeline(middlewares=[sm])

    with pytest.raises(
        ValidationError,
        match="Missing state fields {'miss'}, required by middleware"
        " _SimpleMiddleware, at index 0.",
    ):
        pipe.run({}, [{}, {}, {}])


def test_run_missing_data_field() -> None:
    sm = _SimpleMiddleware()
    sm.requires_data_fields_pre = {"miss"}

    pipe = Pipeline(middlewares=[sm])

    with pytest.raises(
        ValidationError,
        match="Missing data pre fields {'miss'}, required by middleware"
        " _SimpleMiddleware, at index 0.",
    ):
        pipe.run({}, [{}, {}, {}])


def test_run_missing_data_field_no_validate() -> None:
    """
    Check that with validation turned off the pipeline will run despite a
    missing data field.
    """
    sm = _SimpleMiddleware()
    sm.requires_data_fields_pre = {"miss"}

    pipe = Pipeline(middlewares=[sm])

    pipe.run({}, [{}, {}, {}], validate=False)


def test_run_ok() -> None:
    class _Middleware(Middleware[Any, Any]):
        def wrap(self, next_step: ProcessingStep[Any, Any]) -> ProcessingStep[Any, Any]:
            def wrapped(state: Any, data: Any) -> None:
                state["value"] = state["step"] + 1

            return wrapped

    data_loader: Any = [{} for _ in range(3)]
    state: Any = {"acc": []}

    pipeline = Pipeline(middlewares=[AccMiddleware(), _Middleware()])
    pipeline.run(state=state, data_loader=data_loader)

    assert state["acc"] == [1, 2, 3]


def test_run_no_loader_length() -> None:
    class _Middleware(Middleware[Any, Any]):
        def wrap(self, next_step: ProcessingStep[Any, Any]) -> ProcessingStep[Any, Any]:
            def wrapped(state: Any, data: Any) -> None:
                assert set(state.keys()) == {"step"}

            return wrapped

    pipeline = Pipeline(middlewares=[_Middleware()])

    data_loader: Any = ({} for _ in range(3))
    state: Any = {}

    pipeline.run(state, data_loader)


def test_run_dataloader_length() -> None:
    data_loader: Any = [{} for _ in range(3)]

    class _Middleware(Middleware[Any, Any]):
        def wrap(self, next_step: ProcessingStep[Any, Any]) -> ProcessingStep[Any, Any]:
            def wrapped(state: Any, data: Any) -> None:
                assert set(state.keys()) == {"epoch", "num_epochs"}
                assert state["num_epochs"] == len(data_loader)

            return wrapped

    pipeline = Pipeline(middlewares=[_Middleware()], step_name="epoch")
    state: Any = {}

    pipeline.run(state, data_loader)


def test_skip_step() -> None:
    class _Middleware(Middleware[Any, Any]):
        def wrap(self, next_step: ProcessingStep[Any, Any]) -> ProcessingStep[Any, Any]:
            def wrapped(state: Any, data: Any) -> None:
                state["value"] = state["step"] + 1
                if state["step"] == 1:
                    raise SkipStep

            return wrapped

    data_loader: Any = [{} for _ in range(3)]
    state: Any = {"acc": []}

    pipeline = Pipeline(middlewares=[AccMiddleware(), _Middleware()])
    pipeline.run(state=state, data_loader=data_loader)

    assert state["acc"] == [1, 3]


def test_abort_pipeline() -> None:
    class _Middleware(Middleware[Any, Any]):
        def wrap(self, next_step: ProcessingStep[Any, Any]) -> ProcessingStep[Any, Any]:
            def wrapped(state: Any, data: Any) -> None:
                state["value"] = state["step"] + 1
                if state["step"] == 1:
                    raise AbortPipeline

            return wrapped

    data_loader: Any = [{} for _ in range(3)]
    state: Any = {"acc": []}

    pipeline = Pipeline(middlewares=[AccMiddleware(), _Middleware()])
    pipeline.run(state=state, data_loader=data_loader)

    assert state["acc"] == [1]


def test_middleware_callbacks() -> None:
    mware = _SimpleMiddleware()

    mware.on_start = MagicMock(wraps=mware.on_start)  # type: ignore[method-assign]
    mware.on_finish = MagicMock(wraps=mware.on_finish)  # type: ignore[method-assign]

    state: Any = {"hello": 1}
    data_loader: Any = [{} for _ in range(3)]

    pipeline = Pipeline(middlewares=[mware])
    pipeline.run(state=state, data_loader=data_loader)

    # This test is not exactly correct - because the state is mutable, and changes in
    # the course of execution. so the final state will have "step" and "value" keys,
    # which the initial state (that was passed to the on_start call) did not have.
    # Nevertheless, as MagicMock simply stores the reference to the arguments passed
    # in the call (and does not copy them), this check passes.
    mware.on_start.assert_called_once_with(state)

    mware.on_finish.assert_called_once_with(state)


def test_empty_generator() -> None:
    length = 3
    gen = EmptyGenerator(length)

    assert len(gen) == length

    output_1 = list(gen)

    # modify one item to check that new dictionaries are generated
    output_1[0] = {1: 1}  # type: ignore[dict-item]
    output_2 = list(gen)

    assert output_1 == [{1: 1}, {}, {}]  # type: ignore[comparison-overlap]
    assert output_2 == [{}, {}, {}]
