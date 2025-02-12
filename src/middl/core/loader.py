"""
Module defining data-loading classes and protocols.

This module exports Loader, which is a common protocol for data loaders consumed by
pipelines, and also provides EmptyLoader, a range equivalent for data loaders, and
wrap_iterable, which wraps an iterable that returns tuples as batches, so that it
conforms to the Loader protocol (returns dicts as batches).
"""

from collections.abc import Iterable, Iterator, Sequence, Sized
from typing import Any, Protocol, cast

from .middleware import StrMapping

__all__ = [
    "EmptyLoader",
    "Loader",
    "WrappedSizedLoader",
    "WrappedUnsizedLoader",
    "wrap_iterable",
]


class Loader(Protocol, Iterable[StrMapping]):
    """
    A protocol for all data loaders consumed by pipelines.

    A loader should be an iterable that yields string-key mappings as batches
    of data, and should also have a `data_fields` attribute, which is used for
    validating the pipeline.
    """

    data_fields: set[str]


class EmptyLoader:
    """
    A loader that returns empty batces of data.

    Useful for iteatring over epochs.

    Example usage:
        >>> generator = EmptyGenerator(num_steps=3)
        >>> for batch in generator:
        ...     print(batch)
        {}
        {}
        {}
        >>> print(len(generator))
        3
    """

    def __init__(self, num_steps: int) -> None:
        """
        Initialize an EmptyGenerator instance.

        Args:
            num_steps: The number of steps (or batches) to generate.

        """
        self.num_steps = num_steps
        self.data_fields: set[str] = set()

    def __iter__(self) -> Iterator[StrMapping]:
        """
        Create an iterator that yields empty dictionaries for each step.

        Yields:
            An iterator yielding empty dictionaries.

        """
        for _ in range(self.num_steps):
            yield {}

    def __len__(self) -> int:
        """
        Return the total number of steps.

        Returns:
            The number of steps.

        """
        return self.num_steps


class WrappedUnsizedLoader:
    """
    A wrapper for iterable data loaders that return batches as sequences.

    This class converts each sequence-based batch from the wrapped loader into
    a dictionary using the fields specified in `data_fields`.
    """

    def __init__(
        self, loader: Iterable[Sequence[Any]], data_fields: Sequence[str]
    ) -> None:
        """
        Initialize a WrappedUnsizedLoader.

        Args:
            loader: An iterable that yields sequences (e.g., tuples or lists)
                representing data batches.
            data_fields: A sequence of string keys to use in the output dictionaries.
                The number of keys must match the length of each sequence in the loader,
                each item in batch will be assigned the key at the corresponding
                position.

        """
        self._loader = loader
        self.data_fields = set(data_fields)
        self.data_fields_seq = data_fields

    def __iter__(self) -> Iterator[StrMapping]:
        """
        Create an iterator that converts each sequence batch into a dictionary.

        Yields:
            A dict mapping each item of the sequence to the corresponding key
            in `data_fields`.

        """
        for batch in self._loader:
            batch_dict: dict[str, Any] = dict(
                zip(self.data_fields_seq, batch, strict=True)
            )
            yield batch_dict


class WrappedSizedLoader(WrappedUnsizedLoader):
    """
    A wrapper for iterable data loaders that return sequences and have a length.

    In addition to converting sequence-based batches into dictionaries,
    this class implements the __len__ method, allowing callers to check
    the total number of batches.
    """

    def __len__(self) -> int:
        """
        Return the number of batches in the wrapped loader.

        Returns:
            The length of the underlying loader.

        """
        return len(cast(Sized, self._loader))


def wrap_iterable(
    loader: Iterable[Sequence[Any]], data_fields: Sequence[str]
) -> WrappedSizedLoader | WrappedUnsizedLoader:
    """
    Wrap a sequence-yielding iterable, so that it conforms to Loader protocol.

    This will wrap an iterable, which yields a sequence (tuple, list, ...) as a batch
    in a wrapper class, which simply puts each item of the original data loader batch
    in a dict, with keys specified in `data_fields` argument.

    If the original data loader is a sizable (has length), the wrapped object
    will also be.

    Arguments:
        loader: An iterable, which yields sequences (tuples, lists, ...). Can be sized
            (has length) or not.
        data_fields: Keys for data fields in the dict of the batches of the data loader
            wrapper. The number of keys has to match the number items in a batch of
            `loader`, each item in batch will be assigned the key at the corresponding
            position.

    Returns:
        A wrapped data loader, that yields dictionary batches.

    """
    if isinstance(loader, Sized):
        return WrappedSizedLoader(loader, data_fields)

    return WrappedUnsizedLoader(loader, data_fields)
