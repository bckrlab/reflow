import inspect
import logging
import random
from copy import deepcopy
from typing import Any, Tuple

from joblib import Parallel, delayed

from ..cache.base import Cache
from ..recipe import Options, Recipe
from ..utils.execution import ensure_dict, ensure_tuple, execution_path_lazy
from ..utils.patterns import ElementPattern, StepOptionPatternList
from ..utils.utils import filter_recipe, filtered_option_combinations
from .base import CachedExecutor, Executor

_logger = logging.getLogger(__name__)


class SimpleDepthFirstExecutor(CachedExecutor):
    """Executes a recipe in a depth-first manner. Does not support caching."""

    def __init__(
        self,
        cache: Cache = None,
        n_jobs: int | None = None,
        n_jobs_shuffle: bool = True,
    ) -> None:
        super().__init__(cache)
        self.n_jobs = n_jobs
        self.n_jobs_shuffle = n_jobs_shuffle

    def _execute(
        self,
        recipe: Recipe,
        input: Tuple[Any, ...] | dict[str, Tuple[Any, ...]],
        output: list[str] | None = None,  # None -> output
        options_include: StepOptionPatternList | None = None,
        options_exclude: StepOptionPatternList | None = None,
        cache_include: ElementPattern | None = None,
        cache_exclude: ElementPattern | None = None,
        on_purge_step: str = "raise",
        **kwargs,
    ) -> list[tuple[Options, dict[str, Any]]]:
        _logger.debug("Executing recipe:")
        # _logger.debug(f"Executing recipe: {recipe}")
        _logger.debug(f"Requested output: {output}")
        _logger.debug(f"Options included: {options_include}")
        _logger.debug(f"Options excluded: {options_exclude}")

        filtered_recipe = filter_recipe(
            recipe,
            output=output,
            include=options_include,
            exclude=options_exclude,
            on_purge_step=on_purge_step,
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

        # shuffle instances for execution to prevent
        # overlapping executions across workers
        if self.n_jobs is not None and self.n_jobs > 0 and self.n_jobs_shuffle:
            random.shuffle(options_list)

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

        # filter recipe according to cache
        _logger.debug("Removing redundant steps")

        steps_to_execute = {Recipe._DAG_ROOT}
        for step in output_steps:
            candidates = [step]
            while len(candidates) > 0:
                new_candidates = []
                for candidate in candidates:
                    _logger.debug(f"Candidate: {candidate}")
                    cache_enabled = self._cache_match(
                        candidate, cache_include, cache_exclude
                    )
                    _logger.debug(f"Caching enabled: {cache_enabled}")

                    if candidate in steps_to_execute:
                        continue
                    if self._cache_contains(
                        candidate, options, cache_include, cache_exclude
                    ):
                        _logger.debug(f"In cache: {candidate}")
                        continue
                    else:
                        _logger.debug(f"Not in cache: {candidate}")
                    steps_to_execute.add(candidate)
                    predecessors = recipe.steps.predecessors(candidate)
                    new_candidates.extend(predecessors)
                candidates = new_candidates
        redundant_steps = set(recipe.steps.nodes) - steps_to_execute

        _logger.debug(f"Removed redundant steps: {redundant_steps}")

        # initialize filtered recipe
        filtered_recipe = deepcopy(recipe)
        filtered_recipe.steps.remove_nodes_from(redundant_steps)
        for step in filtered_recipe.steps.nodes:
            filtered_recipe.steps.nodes[step]["consumed_by"] = set()

        # ensure dict after filtering
        input = ensure_dict(input, filtered_recipe.input_steps())

        # calculate execution path
        execution_path = execution_path_lazy(filtered_recipe)
        print(execution_path)
        execution_path = [
            step
            for layer in execution_path
            for group in layer
            for step in group
            if step != Recipe._DAG_ROOT
        ]

        _logger.debug(f"Derived execution path: {execution_path}")

        # run steps
        print(execution_path)
        results = dict()
        for step in execution_path:
            option = options[step]

            _logger.debug(f"Executing step: {step}={option}")

            # collect inputs
            input_steps = list(recipe.steps.predecessors(step))

            _logger.debug(f"Input steps: {input_steps}")

            step_input = []
            for input_step in input_steps:
                _logger.debug(f"Add input for input step: {input_step}")
                if input_step == Recipe._DAG_ROOT:
                    step_input.append(input[step])
                    _logger.debug("Add original input.")
                elif input_step in results:
                    result_input = results[input_step]
                    step_input.append(result_input)
                    _logger.debug("Add results input.")
                else:
                    cached_input = self._cache_get(
                        input_step,
                        options,
                        include=cache_include,
                        exclude=cache_exclude,
                    )
                    print(input_step, options, cached_input)
                    for k, v in self.cache.dict.items():
                        print(k, v)
                    print(cached_input)
                    step_input.append(cached_input)
                    _logger.debug("Add cached input.")
            # flatten tuples
            step_input = [i for i in step_input for i in ensure_tuple(i)]
            _logger.debug(f"Input: {len(step_input)}")

            # calculate result
            func = recipe.steps.nodes[step]["options"][option]["func"]

            kwargs = {}
            argspec = inspect.getfullargspec(func)
            if argspec.defaults is not None:
                kwarg_names = argspec.args[-len(argspec.defaults) :]
                if "context" in kwarg_names:
                    context = Executor.Context(step=step, options=deepcopy(options))
                    kwargs["context"] = context

            results[step] = func(*step_input, **kwargs)

            # cache result

            cache_enabled = self._cache_match(step, cache_include, cache_exclude)
            _logger.debug(f"Caching enabled: {cache_enabled}")
            self._cache_set(
                step,
                options,
                results[step],
                include=cache_include,
                exclude=cache_exclude,
            )

            # clean up results
            for input_step in input_steps:
                if (
                    input_step != Recipe._DAG_ROOT
                    and input_step in filtered_recipe.steps.nodes
                ):
                    consumed_by = filtered_recipe.steps.nodes[input_step]["consumed_by"]
                    consumed_by.add(step)
                    successors = list(filtered_recipe.steps.successors(input_step))
                    if step in results and len(consumed_by) == len(successors):
                        del results[input_step]

        output = {}
        for step in output_steps:
            if step in results:
                output[step] = results[step]
            else:
                output[step] = self._cache_get(
                    step,
                    options,
                    include=cache_include,
                    exclude=cache_exclude,
                    default=None,
                )

        # return results
        return output
