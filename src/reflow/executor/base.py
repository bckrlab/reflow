from abc import ABC, abstractmethod
from ast import parse
from tkinter import N
from typing import Any, Hashable, Iterable, Tuple

from ..utils.patterns import ElementPattern, StepOptionPatternList, parse_element_pattern

from ..recipe import Key

from ..cache.base import Options

from ..recipe import Recipe

from ..cache.base import Cache


class Executor(ABC):
    """Abstract base class for executors for executing `Recipe`s.
    """

    @abstractmethod
    def execute(
            self,
            recipe: Recipe,
            input: dict[str, Tuple[Any, ...]]|Tuple[Any, ...],
            output: list[str]|None=None,  # None -> output
            options_include: StepOptionPatternList|None=None,
            options_exclude: StepOptionPatternList|None=None,
            cache_include: ElementPattern|None=None,
            cache_exclude: ElementPattern|None=None,
            **kwargs
        ) -> Iterable[Tuple[Options, dict[str, Any]]]:
        """Execute a `Recipe` with the given input.

        Parameters
        ----------
        recipe : Recipe
            The `Recipe` to execute.
        input : dict[str, Tuple[Any, ...]]
            The input to specific steps of the recipe. 
            Step in the recipe receive a tuple as input (non-keyword arguments).
        output : list[str]|None, optional
            The steps to return as output for.
            If None all output steps are returned, 
            by default None
        options_include : StepOptionPatternList|None, optional
            Patterns to select the options to execute,
            None includes all potential options,
            by default None
        options_exclude : StepOptionPatternList|None, optional
            Patterns to exclude options from execution (works on
            options that are included by `options_include`),
            None excludes no options,
            by default None
        cache_include : ElementPattern|None, optional
            Specifies the steps to include in caching.
            None means no steps are included.
        cache_exclude : ElementPattern|None, optional
            Specifies the steps to exclude from caching.
            None means no steps are excluded.
        kwargs : dict
            Additional keyword arguments used by specific implementations.
            See the corresponding implementation for details.

        Returns
        -------
        Iterable[Tuple[Key, Any]]
            An iterable of tuples of the form (execution key, result)
        """
        raise NotImplementedError()
    
    def cached_steps(self) -> Iterable[Key]|None:
        """Get a list of all cached paths
        
        Returns
        -------
        Iterable[list[Tuple[str, Hashable]]] | None
            A list of all cached paths or None if no cache is used.
        ."""
        return None


    def cache_delete(self, step:str, options:Options=None):
        """Delete a cached item.
        
        Parameters
        ----------
        path : list[Tuple[str, Hashable]]
            The path of the item to delete.
        """
        pass

    def cache_delete_all(self, step:str, option:Hashable=None):
        """Delete a cached step=option independent of other options.
        
        Parameters
        ----------
        step : str
            The step of the item to delete.
        option : Hashable, optional
            The option of the item to delete, by default None
            If None, all options of the step are deleted.
        """
        pass

    def cache_clear(self):
        """Clear the cache."""
        pass

    class Context():
        """Passed to the executed function as a keyword argument `Context.KWARG_NAME`.`
        """

        KWARG_NAME = "context"

        def __init__(self, step:str, options:Options):
            self.step = step
            self.options = options
            self.metrics = {}


class CachedExecutor(Executor):
    """Abstract base class for executors 
    helping to executing `Recipe`s 
    with caching based on `Cache` classes.
    
    See Also
    --------
    reflow.cache.base_dag.Cache: for details on `Cache` classes
    """

    def __init__(self, cache:Cache=None) -> None:
        """Initialize a `CachedExecutor`.

        Parameters
        ----------
        cache : Cache, optional
            The cache, by default None
        """
        self.cache = cache

    @abstractmethod
    def _execute(
            self,
            recipe: Recipe,
            input: dict[str, Tuple[Any, ...]]|Tuple[Any, ...],
            output: list[str]|None=None,  # None -> output
            options_include: StepOptionPatternList|None=None,
            options_exclude: StepOptionPatternList|None=None,
            cache_include: ElementPattern|None=None,
            cache_exclude: ElementPattern|None=None,
            **kwargs
        ) -> Iterable[Tuple[Options, dict[str, Any]]]:
        """Execute a `Recipe` with the given input.
        This method is called by the `execute` method
        and is not meant to be called directly.

        Parameters
        ----------
        recipe : Recipe
            The `Recipe` to execute.
        input : dict[str, Tuple[Any, ...]]
            The input to specific steps of the recipe. 
            Step in the recipe receive a tuple as input (non-keyword arguments).
        output : list[str]|None, optional
            The steps to return as output for.
            If None all output steps are returned, 
            by default None
        options_include : StepOptionPatternList|None, optional
            Patterns to select the options to execute,
            None includes all potential options,
            by default None
        options_exclude : StepOptionPatternList|None, optional
            Patterns to exclude options from execution (works on
            options that are included by `options_include`),
            None excludes no options,
            by default None
        cache_include : ElementPattern|None, optional
            Specifies the steps to include in caching.
            None means no steps are included.
        cache_exclude : ElementPattern|None, optional
            Specifies the steps to exclude from caching.
            None means no steps are excluded.
        kwargs : dict
            Additional keyword arguments used by specific implementations.
            See the corresponding implementation for details.

        Returns
        -------
        Iterable[Tuple[Key, Any]]
            An iterable of tuples of the form (execution key, result)
        """
        raise NotImplementedError()

    def execute(
            self,
            recipe: Recipe,
            input: dict[str, Tuple[Any, ...]]|Tuple[Any, ...],
            output: list[str]|None=None,  # None -> output
            options_include: StepOptionPatternList|None=None,
            options_exclude: StepOptionPatternList|None=None,
            cache_include: ElementPattern|None=None,
            cache_exclude: ElementPattern|None=None,
            on_purge_step:str='raise',
            **kwargs
        ) -> Iterable[Tuple[Options, dict[str, Any]]]:

        result = self._execute(
            recipe=recipe,
            input=input,
            output=output,
            options_include=options_include,
            options_exclude=options_exclude,
            cache_include=cache_include,
            cache_exclude=cache_exclude,
            on_purge_step=on_purge_step,
            **kwargs)

        return result
    
    def _cache_get(
            self, 
            step: str,
            options: Options, 
            default: Any=None, 
            include: ElementPattern|None=None, 
            exclude: ElementPattern|None=None) -> Any:
        """Get an item from the cache.
        Adds filter functionality to the `Cache.get` method.
        If the patterns do not match, the default value is returned.

        This method is not meant to be called directly.

        Parameters
        ----------
        step : str
            The step of the item in the cache.
        options : Options
            The options of the items in the cache.
        default : Any, optional
            Default value if the the path does not exist, by default None
        include : ElementPattern|None, optional
            Steps to include for caching, by default None
        exclude : ElementPattern|None, optional
            Steps to exclude from caching (applied after `include`), by default None

        Returns
        -------
        Any
            The item in the cache at the given path or the default value
        """
        
        if self.cache is None:
            return default
        
        if self._cache_match(
                step, 
                include=include, 
                exclude=exclude):
            return self.cache.get(step, options, default=default)
        else:
            return default

    def _cache_set(
            self, 
            step: str,
            options: Options,
            item: Any, 
            include: ElementPattern|None=None, 
            exclude: ElementPattern|None=None) -> None:
        """Set an item in the cache.
        Adds path filter functionality to the `Cache.set` method.
        If the patterns do not match, the item is not set in the cache.
        
        Parameters
        ----------
        step: str
            The step of the item in the cache.
        options : Options
            The options of the item in the cache.
        item : Any
            The item to set in the cache.
        recipe : Recipe, optional
            The recipe to use for  caching, by default None
        include : ElementPattern|None, optional
            Steps to include for caching, by default None
        exclude : ElementPattern|None, optional
            Steps to exclude from caching (applied after `include`), by default None
        """
        
        if self.cache is None:
            return

        if self._cache_match(
                step,
                include=include, 
                exclude=exclude):
            self.cache.set(step, options, item)

    def _cache_contains(
            self, 
            step:str, 
            options:Options,
            include: ElementPattern|None=None, 
            exclude: ElementPattern|None=None) -> bool:
        """Check if the cache contains an item at the given path.
        Adds path filter functionality to the `Cache.contains` method.
        If the patterns do not match, False is returned.

        Parameters
        ----------
        step : str
            The step of the item in the cache.
        options : Options
            The options of the item in the cache.
        include : ElementPattern|None, optional
            Steps to include for caching, by default None
        exclude : ElementPattern|None, optional
            Steps to exclude from caching (applied after `include`), by default None

        Returns
        -------
        bool
            True if the cache contains an item at the given path, False otherwise.
        """
        
        if self.cache is None:
            return False

        if self._cache_match(
                step,
                include=include, 
                exclude=exclude):
            return self.cache.contains(step, options)
        else:
            return False

    def cached_steps(self) -> Iterable[Options] | None:
        if self.cache is None:
            return None
        else:
            return self.cache.keys()

    def cache_delete(self, step:str, options:Options=None):
        if self.cache is not None:
            self.cache.delete(step, options)

    def cache_delete_all(self, step: str, option: Hashable = None):
        return self.cache.delete_all(step, option)

    def cache_clear(self):
        if self.cache is not None:
            self.cache.clear()

    def _cache_match(
            self,
            step:str, 
            include:ElementPattern|None,
            exclude:ElementPattern|None):
        """Check whether the given step is supposed to use the cache.
        """

        include = parse_element_pattern(include)
        exclude = parse_element_pattern(
            exclude,
            on_empty_element_list=False,
            on_none_element_pattern=False)
        
        return include(step) and not exclude(step)
