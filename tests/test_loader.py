from collections.abc import Generator

import pytest

from middl import EmptyLoader, WrappedSizedLoader, WrappedUnsizedLoader, wrap_iterable


def test_empty_loader() -> None:
    length = 3
    gen = EmptyLoader(length)

    assert len(gen) == length

    output_1 = list(gen)

    # modify one item to check that new dictionaries are generated
    output_1[0] = {1: 1}  # type: ignore[dict-item]
    output_2 = list(gen)

    assert output_1 == [{1: 1}, {}, {}]  # type: ignore[comparison-overlap]
    assert output_2 == [{}, {}, {}]


def test_zero_fields() -> None:
    data = [(), ()]
    data_fields: tuple[str, ...] = ()
    wrapped = wrap_iterable(data, data_fields)
    results = list(wrapped)
    assert len(results) == len(data)
    for r in results:
        assert r == {}


def test_one_field() -> None:
    data = [("val1",), ("val2",)]
    data_fields = ("field",)
    wrapped = wrap_iterable(data, data_fields)
    results = list(wrapped)
    assert len(results) == len(data)
    assert results[0] == {"field": "val1"}
    assert results[1] == {"field": "val2"}


def test_three_fields() -> None:
    data = [
        ("a1", "b1", "c1"),
        ("a2", "b2", "c2"),
    ]
    data_fields = ("A", "B", "C")
    wrapped = wrap_iterable(data, data_fields)
    results = list(wrapped)
    assert len(results) == len(data)
    for r in results:
        assert set(r.keys()) == set(data_fields)
        assert len(r.values()) == len(data[0])
        for k, v in r.items():
            # Make sure keys are matched to the item at its
            # corresponding position.
            assert k[0] == v[0].upper()


def test_batch_size_mismatch() -> None:
    data = [("x", "y")]
    data_fields = ("A", "B", "C")
    with pytest.raises(ValueError, match="zip\\(\\) argument"):
        list(wrap_iterable(data, data_fields))


def test_sized_loader_length() -> None:
    data = [
        ("item1",),
        ("item2",),
        ("item3",),
    ]
    data_fields = ("only_key",)
    wrapped = wrap_iterable(data, data_fields)
    assert isinstance(wrapped, WrappedSizedLoader)
    assert len(wrapped) == len(data)


def test_wrap_iterable_sized() -> None:
    data = [("one",), ("two",)]
    data_fields = ("key",)
    wrapped = wrap_iterable(data, data_fields)
    assert isinstance(wrapped, WrappedSizedLoader)


def test_wrap_iterable_unsized() -> None:
    def gen() -> Generator[tuple[str], None, None]:
        yield ("first",)
        yield ("second",)

    data_fields = ("only_key",)
    wrapped = wrap_iterable(gen(), data_fields)
    assert isinstance(wrapped, WrappedUnsizedLoader)
