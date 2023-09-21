from __future__ import annotations

from typing import Any, Tuple

from .utils.patterns import ElementPattern, StepOptionPatternList

from .cache.base import Cache

from .recipe import Recipe
from .executor.base import Executor
from .executor.utils_dag import default_executor, execute_recipe


class Session():
    """
    session = recipe.process(input)[.with(executor, cache, default_reset...)]
    session.execute(....)

    # maybe this is enough:
    session = Session(recipe).process(X).using(cache=cache)
    session.execute(...)
    """

    def __init__(
            self,
            recipe:Recipe,
            args:Tuple[Any,...]=None,
            kwargs:dict[str,Any]=None,
            executor:Executor=None,
            default_cache_reset:str|None=None
            ) -> None:
        
        self.recipe = recipe
        
        self.args = args
        self.kwargs = kwargs

        self.executor = executor
        self.default_cache_reset = default_cache_reset

    def process(self, *args, **kwargs) -> Session:

        self.args = args
        self.kwargs = kwargs

        # we initialize the executor here, 
        # because we want to be able to skip calling `using`
        self.executor = default_executor()

        return self

    def using(
            self, 
            executor:Executor=None, 
            cache:Cache=None,
            cache_reset:bool=None,
            n_jobs:int|None=None,
            n_jobs_shuffle:bool=True) -> Session:
        
        self.executor = default_executor(
            executor, cache, n_jobs=n_jobs, n_jobs_shuffle=n_jobs_shuffle)
        self.default_cache_reset = cache_reset
        return self
        
    def execute(
            self,
            step:list[str]|str="latest", # latest, output
            # syntactic sugar; can be achieved with include
            option:ElementPattern|None="latest",  
            include:StepOptionPatternList|str="latest",  # latest, all, step
            exclude:StepOptionPatternList|None=None,
            squeeze:bool=True,
            # whether to enable cache for these paths 
            cache_include:ElementPattern|str|None="last step", # latest, output
            cache_exclude:ElementPattern|None=None,
            # reset cache; TODO: maybe we should allow include and exclude patterns for this
            cache_reset:str|None="default",  # last step, output, all, default
            # **kwargs:dict
        ) -> Any:

        if self.args is None:
            raise ValueError("Please call `process()` before `execute()`!")

        if cache_reset == "default":
            cache_reset = self.default_cache_reset

        return execute_recipe(
            recipe=self.recipe,
            input=self.args,
            step=step,
            option=option,
            include=include,
            exclude=exclude,
            squeeze=squeeze,
            executor=self.executor,
            cache_include=cache_include,
            cache_exclude=cache_exclude,
            cache_reset=cache_reset
        )
