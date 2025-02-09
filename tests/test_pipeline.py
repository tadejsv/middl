from typing import Any

import pytest

from middl import (
    AbortPipeline,
    Middleware,
    Pipeline,
    ProcessingStep,
    SkipStep,
    ValidationError,
    empty_sink,
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

    pipe = Pipeline(middlewares=[sm], sink=empty_sink)

    with pytest.raises(
        ValidationError,
        match="Missing data pre fields {'miss'}, required by middleware"
        " _SimpleMiddleware, at index 0.",
    ):
        pipe.validate(set(), set())


def test_validate_missing_post_field() -> None:
    sm = _SimpleMiddleware()
    sm.requires_data_fields_post = {"miss"}

    pipe = Pipeline(middlewares=[sm], sink=empty_sink)

    with pytest.raises(
        ValidationError,
        match="Missing data post fields {'miss'}, required by middleware"
        " _SimpleMiddleware, at index 0.",
    ):
        pipe.validate(set(), set())


def test_validate_missing_state_field() -> None:
    sm = _SimpleMiddleware()
    sm.requires_state_fields = {"miss"}

    pipe = Pipeline(middlewares=[sm], sink=empty_sink)

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

    pipe = Pipeline(middlewares=[sm1, sm2], sink=empty_sink)

    pipe.validate({"miss"}, set())


def test_run_missing_state_field() -> None:
    sm = _SimpleMiddleware()
    sm.requires_state_fields = {"miss"}

    pipe = Pipeline(middlewares=[sm], sink=empty_sink)

    with pytest.raises(
        ValidationError,
        match="Missing state fields {'miss'}, required by middleware"
        " _SimpleMiddleware, at index 0.",
    ):
        pipe.run({}, [{}, {}, {}])


def test_run_missing_data_field() -> None:
    sm = _SimpleMiddleware()
    sm.requires_data_fields_pre = {"miss"}

    pipe = Pipeline(middlewares=[sm], sink=empty_sink)

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

    pipe = Pipeline(middlewares=[sm], sink=empty_sink)

    pipe.run({}, [{}, {}, {}], validate=False)


def test_run_ok() -> None:
    def sink(state: Any, data: Any) -> None:
        state["value"] = state["step"] + 1

    data_loader: Any = [{} for _ in range(3)]
    state: Any = {"acc": []}

    pipeline = Pipeline(middlewares=[AccMiddleware()], sink=sink)
    pipeline.run(state=state, data_loader=data_loader)

    assert state["acc"] == [1, 2, 3]


def test_skip_step() -> None:
    def sink(state: Any, data: Any) -> None:
        state["value"] = state["step"] + 1
        if state["step"] == 1:
            raise SkipStep

    data_loader: Any = [{} for _ in range(3)]
    state: Any = {"acc": []}

    pipeline = Pipeline(middlewares=[AccMiddleware()], sink=sink)
    pipeline.run(state=state, data_loader=data_loader)

    assert state["acc"] == [1, 3]


def test_abort_pipeline() -> None:
    def sink(state: Any, data: Any) -> None:
        state["value"] = state["step"] + 1
        if state["step"] == 1:
            raise AbortPipeline

    data_loader: Any = [{} for _ in range(3)]
    state: Any = {"acc": []}

    pipeline = Pipeline(middlewares=[AccMiddleware()], sink=sink)
    pipeline.run(state=state, data_loader=data_loader)

    assert state["acc"] == [1]
