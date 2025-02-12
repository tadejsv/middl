from collections.abc import Iterable, Iterator, Sized
from typing import Any, Protocol

from .middleware import StrMapping


class Loader(Protocol, Iterable[StrMapping]):
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
    def __init__(self, loader: Iterable, data_fields: set[str]):
        self._loader = loader
        self.data_fields = data_fields

    def __iter__(self) -> Iterator[StrMapping]:
        for batch in self._loader:
            batch_dict: dict[str, Any] = dict(zip(self.data_fields, batch, strict=True))
            yield batch_dict


class WrappedSizedLoader(WrappedUnsizedLoader):
    def __len__(self) -> int:
        return len(self._loader)


def wrap_iterable(
    loader: Iterable, data_fields: set[str]
) -> WrappedSizedLoader | WrappedUnsizedLoader:
    if isinstance(loader, Sized):
        return WrappedSizedLoader(loader, data_fields)

    return WrappedUnsizedLoader(loader, data_fields)
