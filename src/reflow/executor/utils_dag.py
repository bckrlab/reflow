from copy import deepcopy
import logging
from typing import Any, Tuple
import warnings

from ..utils.patterns import ElementPattern, StepOptionPatternList

from .base import Executor
from .depthfirst import SimpleDepthFirstExecutor

from ..cache.base import Cache
from ..cache.dict_cache import DictCache

from ..recipe import Recipe


_logger = logging.getLogger(__name__)


def execute_recipe(
        recipe:Recipe,
        input:Tuple[Any,...]|dict[str, Tuple[Any,...]],
        step:list[str]|str="latest", # latest, output
        # syntactic sugar; can be achieved with include
        option:ElementPattern|None="latest",  
        include:StepOptionPatternList|str="latest",  # latest, all, step
        exclude:StepOptionPatternList|None=None,
        squeeze:bool=True,
        executor:Executor=None,
        # in case no executor is given we initialize one with these parameters
        n_jobs:int|None=None,
        n_jobs_shuffle:bool=True,
        cache:Cache=None,
        # whether to enable cache for these paths 
        cache_include:ElementPattern|str|None="last step", # last step, output
        cache_exclude:ElementPattern|None=None,
        # reset cache; TODO: maybe we should allow include and exclude patterns for this
        cache_reset:str|None=None,  # last step, output, all
        on_purge_step:str='raise',
        # **kwargs:dict
        ) -> Any:
    
    _logger.debug(f"Executing recipe")
    _logger.debug(f"Step: {step}, option: {option}")
    _logger.debug(f"Include: {include}")
    _logger.debug(f"Exclude: {exclude}")

    if type(step) == str:
        if step == "latest":
            steps_parsed = [recipe.latest_step]
        elif step == "output":
            steps_parsed = recipe.output_steps()
        else:
            steps_parsed = [step]
    elif step is None:
        steps_parsed = recipe.output_steps()

    if option is None:
        options_parsed = {}
    elif option == "latest":
        options_parsed = {
            s: recipe.steps.nodes[s]["latest_option"] 
            for s in steps_parsed}
    else:
        options_parsed = {
            s: option
            for s in steps_parsed}
    
    _logger.debug(f"Parsed steps:   {steps_parsed}")
    _logger.debug(f"Parsed options: {options_parsed}")

    if isinstance(include, str):

        if exclude is not None:
            raise ValueError("Cannot set exclude when include is set to a string!")

        if include == "step":
            if option is None:
                option_parsed = recipe.latest_options()[step]
            return recipe.steps.nodes[step]["options"][option_parsed]["func"](*input)
        
        elif include == "latest":
            include_parsed = [recipe.latest_options()]
        
        elif include == "all":
            include_parsed = [{}]
        
        else:
            raise ValueError(f"Unknown include definition: {include}")
    
    elif include is None:
        include_parsed = [{}]
    else:
        include_parsed = deepcopy(include)

    if not isinstance(include_parsed, list):
        include_parsed = [include_parsed]

    for include_dict in include_parsed:
        for s, o in options_parsed.items():
            include_dict[s] = o

    _logger.debug(f"Parsed include: {include_parsed}")

    # get executor
    executor = default_executor(
        executor, 
        cache, 
        n_jobs=n_jobs, 
        n_jobs_shuffle=n_jobs_shuffle,
        no_cache=True)

    # default cache include
    if type(cache_include) is str:
        if cache_include == "last step":
            cache_include = recipe.latest_step
        elif cache_include == "output":
            cache_include = recipe.output_steps()

    # cache reset
    if cache_reset is None:
        pass
    elif cache_reset == "last step":
        _logger.debug("Resetting cache for steps:")
        for s in steps_parsed:
            _logger.debug(f"  - {s}")
            executor.cache_delete_all(s)
    elif cache_reset == "output":
        for s in steps_parsed:
            executor.cache_delete_all(s)
    elif cache_reset == "all":
        executor.cache_clear()
    else:
        raise ValueError(f"Unknown reset value: {cache_reset}")

    # execute
    result = executor.execute(
        recipe=recipe,
        input=input,
        output=steps_parsed,
        options_include=include_parsed,
        options_exclude=exclude,
        cache_include=cache_include,
        cache_exclude=cache_exclude,
        on_purge_step=on_purge_step)
    
    # return
    if squeeze and len(result) <= 1:
        if len(result) == 0:
            return None
        else:
            r = result[0][1]
            if len(r) == 1:
                key = list(r.keys())[0]
                return r[key]
            else:
                return r
    else:
        return result 


def default_executor(
        executor:Executor=None, 
        cache:Cache=None,
        n_jobs:int|None=None,
        n_jobs_shuffle:bool=True,
        no_cache:bool=False) -> Executor:
    
    if executor is not None:
        if cache is not None:
            raise ValueError("Cannot set both executor and cache!")
        if n_jobs is not None:
            warnings.warn("Setting n_jobs has no effect when using a custom executor.")

        return executor

    elif cache is not None:
        return SimpleDepthFirstExecutor(
            cache=cache, n_jobs=n_jobs, n_jobs_shuffle=n_jobs_shuffle)

    else:
        cache = None if no_cache else DictCache()
        return SimpleDepthFirstExecutor(
            cache=cache, n_jobs=n_jobs, n_jobs_shuffle=n_jobs_shuffle)
