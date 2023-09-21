import logging
from typing import Any, Iterable, Tuple

from joblib import Parallel, delayed

from ..cache.base import Cache
from ..cache.dict_cache import DictCache
from ..recipe import Options, Recipe
from ..utils.patterns import ElementPattern, StepOptionPatternList
from ..utils.utils import filter_recipe, filtered_option_combinations
from .base import CachedExecutor

_logger = logging.getLogger(__name__)


class CacheOnlyExecutor(CachedExecutor):
    """This executor does not execute any steps,
    but only retrieves them from the cache."""

    def __init__(
        self,
        cache: Cache,
        local_cache: Cache | str = "dict",
        n_jobs: int | None = None,
        missing_policy: str = "default",  # raise, warn, default
        missing_default_value: Any = None,
    ) -> None:
        """Initializes the executor.
        This executor does not execute any steps,
        but only retrieves them from the cache.

        Parameters
        ----------
        cache : Cache
            The cache to use for retrieving results
        local_cache: Cache, optional
            The local cache to use for keeping results
            after they have been retrieved from the main cache.
        missing_policy : str, optional
            What to do if a path is not found in the cache.
            If "default", the `missing_default_value` is returned.
            By default "default"
        missing_default_value : Any, optional
            The value to return if a path is not found in the cache
            and `missing_policy` is "default", by default None
        """
        super().__init__(cache)
        if type(local_cache) is str:
            if local_cache == "dict":
                self.local_cache = DictCache()
            else:
                raise ValueError(f"Unknown local cache type: {local_cache}")
        else:
            self.local_cache = local_cache
        self.n_jobs = n_jobs
        self.missing_policy = missing_policy
        self.missing_default_value = missing_default_value

    def _execute(
        self,
        recipe: Recipe,
        input: Tuple[Any, ...] | dict[str, Tuple[Any, ...]],
        output: list[str] | None = None,  # None -> output
        options_include: StepOptionPatternList | None = None,
        options_exclude: StepOptionPatternList | None = None,
        cache_include: ElementPattern | None = None,
        cache_exclude: ElementPattern | None = None,
        **kwargs,
    ) -> Iterable[Tuple[Options, dict[str, Any]]]:
        _logger.debug("Executing recipe:")
        # _logger.debug(f"Executing recipe: {recipe}")
        _logger.debug(f"Requested output: {output}")
        _logger.debug(f"Options included: {options_include}")
        _logger.debug(f"Options excluded: {options_exclude}")

        filtered_recipe = filter_recipe(
            recipe, output=output, include=options_include, exclude=options_exclude
        )
        if output is None:
            output_steps = filtered_recipe.output_steps()
        else:
            output_steps = output

        _logger.debug(f"Filtered recipe: {filtered_recipe}")

        # instances with all options
        options_list = filtered_option_combinations(
            filtered_recipe,
            output=output,
            include=options_include,
            exclude=options_exclude,
        )

        results = Parallel(n_jobs=self.n_jobs)(
            delayed(self._execute_instance)(
                options,
                filtered_recipe,
                input,
                output_steps,
                cache_include=cache_include,
                cache_exclude=cache_exclude,
            )
            for options in options_list
        )

        return list(zip(options_list, results))

    def _execute_instance(
        self,
        options: Options,
        recipe: Recipe,
        input: Tuple,
        output_steps: list[str],
        cache_include: ElementPattern | None = None,
        cache_exclude: ElementPattern | None = None,
    ) -> dict[str, Any]:
        _logger.info(f"Executing instance with options: {options}")

        output = {}
        for step in output_steps:
            if self.local_cache is not None and self.local_cache.contains(
                step, options
            ):
                output[step] = self.local_cache.get(step, options)
            else:
                step_output = self._cache_get(
                    step,
                    options,
                    include=cache_include,
                    exclude=cache_exclude,
                    default=None,
                )
                output[step] = step_output
                if self.local_cache is not None and step_output is not None:
                    self.local_cache.set(step, options, step_output)

        # return results
        return output

    def local_cache_clear(self):
        self.local_cache.clear()
