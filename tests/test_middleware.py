from typing import Any

import pytest

from middl import Middleware, ProcessingStep, ValidationError


class _TestMiddleware(Middleware[Any, Any]):
    def __init__(self) -> None:
        super().__init__()

    def wrap(self, next_step: ProcessingStep[Any, Any]) -> ProcessingStep[Any, Any]:
        def wrapped(state: Any, data: Any) -> None:
            next_step(state, data)

        return wrapped


def test_validate_success_pre() -> None:
    mw = _TestMiddleware()
    mw.requires_state_fields = {"state_req1"}
    mw.requires_data_fields_pre = {"data_req1"}
    mw.provides_data_fields_pre = {"data_prov1"}
    state_fields = {"state_req1", "some_other_state"}
    data_fields = {"data_req1", "some_other_data"}
    mw._validate(state_fields, data_fields, pre=True)
    assert "data_prov1" in data_fields


def test_validate_success_post() -> None:
    mw = _TestMiddleware()
    mw.requires_state_fields = {"state_req2"}
    mw.requires_data_fields_post = {"data_req2"}
    mw.provides_data_fields_post = {"data_prov2"}
    state_fields = {"state_req2", "another_state"}
    data_fields = {"data_req2", "another_data"}
    mw._validate(state_fields, data_fields, pre=False)
    assert "data_prov2" in data_fields


def test_validate_missing_state_fields_pre() -> None:
    mw = _TestMiddleware()
    mw.requires_state_fields = {"state_req1"}
    mw.requires_data_fields_pre = {"data_req1"}
    mw.provides_data_fields_pre = {"data_prov1"}
    state_fields = {"some_other_state"}
    data_fields = {"data_req1"}
    with pytest.raises(ValidationError, match="Missing state fields {'state_req1'}"):
        mw._validate(state_fields, data_fields, pre=True)


def test_validate_missing_state_fields_post() -> None:
    mw = _TestMiddleware()
    mw.requires_state_fields = {"state_req2"}
    mw.requires_data_fields_post = {"data_req2"}
    mw.provides_data_fields_post = {"data_prov2"}
    state_fields = {"some_other_state"}
    data_fields = {"data_req2"}
    with pytest.raises(ValidationError, match="Missing state fields {'state_req2'}"):
        mw._validate(state_fields, data_fields, pre=False)


def test_validate_missing_data_fields_pre() -> None:
    mw = _TestMiddleware()
    mw.requires_state_fields = {"state_req1"}
    mw.requires_data_fields_pre = {"data_req1", "data_req2"}
    mw.provides_data_fields_pre = {"data_prov1"}
    state_fields = {"state_req1"}
    data_fields = {"data_req1"}
    with pytest.raises(ValidationError, match="Missing data pre fields {'data_req2'}"):
        mw._validate(state_fields, data_fields, pre=True)


def test_validate_missing_data_fields_post() -> None:
    mw = _TestMiddleware()
    mw.requires_state_fields = {"state_req1"}
    mw.requires_data_fields_post = {"data_reqX"}
    mw.provides_data_fields_post = {"data_provX"}
    state_fields = {"state_req1"}
    data_fields = {"some_other_data"}
    with pytest.raises(ValidationError, match="Missing data post fields {'data_reqX'}"):
        mw._validate(state_fields, data_fields, pre=False)
