
from typing import Any

from .utils.patterns import ElementPattern, StepOptionPatternList

from .executor.base import Executor

from .recipe import Recipe

from .executor.utils_dag import execute_recipe
from .cache.base import Cache


class CallableRecipe(Recipe):
    
    def __init__(self):
        super().__init__()

    def __call__(
            self, 
            *input, 
            step:list[str]|str="latest", # latest, output
            option:ElementPattern|None=None,
            include:StepOptionPatternList|str|None="latest",
            exclude:StepOptionPatternList|str|None=None,
            squeeze:bool=True,
            executor:Executor=None,
            n_jobs:int|None=None,
            n_jobs_shuffle:bool=True,
            cache:Cache=None,
            cache_include:ElementPattern|str|None="latest", # latest, output
            cache_exclude:ElementPattern|None=None,
            cache_reset:str|None=None,
            on_purge_step:str='raise',
            # **kwargs:dict
            ) -> Any:

        return execute_recipe(
            recipe=self,
            input=input,
            step=step,
            option=option,
            include=include,
            exclude=exclude,
            squeeze=squeeze,
            executor=executor,
            n_jobs=n_jobs,
            n_jobs_shuffle=n_jobs_shuffle,
            cache=cache,
            cache_include=cache_include,
            cache_exclude=cache_exclude,
            cache_reset=cache_reset,
            on_purge_step=on_purge_step,
            # **kwargs
        )


def from_recipe(recipe: Recipe, cls: type[Recipe]=None) -> Recipe:
    if cls is None:
        cls = type(recipe)

    new_recipe = cls()
    new_recipe.steps = recipe.steps
    new_recipe.latest_step = recipe.latest_step
    return new_recipe
