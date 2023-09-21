from abc import ABC, abstractmethod
from typing import Any


class Pickler(ABC):
    """Abstract Pickler class which can be used for custom pickling."""

    @abstractmethod
    def dump(self, value: Any, path: str) -> None:
        """Dump an arbitrary Python object into a file.

        Parameters
        ----------
        value : Any
            The value to be dumped.
        path : str
            The path to the file to dump into.
        """
        raise NotImplementedError()

    @abstractmethod
    def load(self, path: str) -> Any:
        """Load an arbitrary Python object from a file.

        Parameters
        ----------
        path : str
            The path to the file to load from.

        Returns
        -------
        Any
            The loaded value.
        """
        raise NotImplementedError()
