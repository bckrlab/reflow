

from abc import ABC, abstractmethod
import time
from typing import Any, Hashable, Iterable, Tuple


from ..recipe import Key, Options


class Cache(ABC):
    """Base class for caches."""

    @abstractmethod
    def get(
            self, 
            step:str,
            options:Options, 
            default:Any=None) -> Any:
        """Get an item from the cache.

        Parameters
        ----------
        step : str
            The step name.
        options : Options
            The options.
        default : Any, optional
            Default value to return if path is not in cache, by default None

        Returns
        -------
        Any
            The item.
        """
        raise NotImplementedError()

    @abstractmethod
    def set(
            self, 
            step:str, 
            options:Options,
            item:Any, 
            cleanup:bool=True) -> None:
        """Set an item in the cache.

        Parameters
        ----------
        step : str
            The step name.
        options : Options
            The options.
        item : Any
            The item to set.
        cleanup : bool, optional
            Whether to internally clean up the cache.
            It may be useful to set this to false if many items are set at once
            and the cleanup operation is performance heavy (e.g., 
            scanning through all files in a large directory or making a database query)
            , by default True
        """
        raise NotImplementedError()

    @abstractmethod
    def contains(
            self, 
            step:str, 
            options:Options) -> bool:
        """Check if the cache contains an item.

        Parameters
        ----------
        step : str
            The step name.
        options : Options
            The options.

        Returns
        -------
        bool
            Whether the cache contains the item.
        """
        raise NotImplementedError()

    @abstractmethod
    def delete(
            self, 
            step:str, 
            options:Options) -> None:
        """Delete an item from the cache.

        Parameters
        ----------
        step : str
            The step name.
        options : Options
            The options.
        """
        raise NotImplementedError()
    
    @abstractmethod
    def delete_all(self, step:str, option:Hashable=None) -> None:
        """Delete a step with a particular option from the cache
        independent of other options settings.

        Parameters
        ----------
        step : str
            The step name.
        option : Hashable, optional
            The option.
            If None, all options for the step are deleted, by default None
        """
        raise not NotImplementedError()
    
    def keys(self) -> Iterable[Key]:
        """Get all keys in the cache.

        Returns
        -------
        Iterable[Path]
            The keys (paths).
        """
        return [path for path, _ in self.items()]
    
    def values(self) -> Iterable[Any]:
        """Get all values in the cache.

        Yields
        ------
        Iterable[Any]
            The values.
        """
        return [item for _, item in self.items()]

    @abstractmethod
    def items(self) -> Iterable[Tuple[Key, Any]]:
        """Get all keys and values in the cache.

        Yields
        ------
        Iterable[Tuple[Key, Any]]
            The paths and items.
        """
        raise NotImplementedError()

    @abstractmethod
    def clear(self) -> None:
        """Clear the cache.
        """
        raise NotImplementedError()

    def is_locked(
            self, 
            step:str, 
            options:Options) -> bool:
        """Check if an item is locked.

        Parameters
        ----------
        step : str
            The step name.
        options : Options
            The options.

        Returns
        -------
        bool
            Whether the item is locked.
        """
        item = self.get(step, options, default=None)
        return isinstance(item, Lock)

    def lock(
            self, 
            step:str, 
            options:Options) -> None:
        """Lock an item.

        Parameters
        ----------
        step : str
            The step name.
        option : Hashable
            The option.
        options : Options
            The options.
        """
        self.set(step, options, Lock(time.time_ns()))

    def clear_locks(self) -> None:
        """Clear all locks."""
        for k, v in self.items():
            if isinstance(v, Lock):
                self.delete(k)


class Lock():
    """A lock for a cache entry."""
    def __init__(self, timestamp:int, source:str|None=None) -> None:
        self.timestamp = timestamp
        self.source = source