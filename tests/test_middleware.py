from typing import Any

import pytest

from middl import Middleware, ValidationError


class _TestMiddleware(Middleware[Any, Any]):
    pass


def test_validate_success() -> None:
    mw = _TestMiddleware()
    mw.requires_state_fields = {"state_req1"}
    mw.requires_data_fields = {"data_req1"}
    mw.provides_data_fields = {"data_prov1"}
    state_fields = {"state_req1", "some_other_state"}
    data_fields = {"data_req1", "some_other_data"}
    mw._validate(state_fields, data_fields)
    assert "data_prov1" in data_fields


def test_validate_missing_state_fields() -> None:
    mw = _TestMiddleware()
    mw.requires_state_fields = {"state_req1"}
    mw.requires_data_fields = {"data_req1"}
    mw.provides_data_fields = {"data_prov1"}
    state_fields = {"some_other_state"}
    data_fields = {"data_req1"}
    with pytest.raises(ValidationError, match="Missing state fields {'state_req1'}"):
        mw._validate(state_fields, data_fields)


def test_validate_missing_data_fields() -> None:
    mw = _TestMiddleware()
    mw.requires_state_fields = {"state_req1"}
    mw.requires_data_fields = {"data_req1", "data_req2"}
    mw.provides_data_fields = {"data_prov1"}
    state_fields = {"state_req1"}
    data_fields = {"data_req1"}
    with pytest.raises(ValidationError, match="Missing data fields {'data_req2'}"):
        mw._validate(state_fields, data_fields)
