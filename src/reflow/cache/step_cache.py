from typing import Any, Hashable

from ..recipe import Options
from .base import Cache


class StepCache(Cache):
    """Define a separate cache for each step of a `Recipe`."""

    def __init__(
        self, cache_dict: dict[str, Cache] | None, default_cache: Cache | None
    ) -> None:
        super().__init__()
        self.cache_dict = cache_dict if cache_dict is not None else {}
        self.default_cache = default_cache

    def set_cache_for_step(self, step: str, cache: Cache):
        self.cache_dict[step] = cache

    def get(self, step: str, options: Options, default: Any = None) -> Any:
        if step in self.cache_dict:
            return self.cache_dict[step].get(step, options, default=default)
        elif self.default_cache is not None:
            return self.default_cache.get(step, options, default=default)

    def set(self, step: str, options: Options, item: Any, cleanup: bool = True) -> None:
        if step in self.cache_dict:
            self.cache_dict[step].set(step, options, item, cleanup=cleanup)
        elif self.default_cache is not None:
            self.default_cache.set(step, options, item, cleanup=cleanup)

    def contains(self, step: str, options: Options) -> bool:
        if step in self.cache_dict:
            return self.cache_dict[step].contains(step, options)
        elif self.default_cache is not None:
            return self.default_cache.contains(step, options)
        else:
            False

    def delete(self, step: str, options: Options) -> None:
        if step in self.cache_dict:
            self.cache_dict[step].delete(step, options)

    def delete_all(self, step: str, option: Hashable = None) -> None:
        if step in self.cache_dict:
            self.cache_dict[step].delete_all(step, option=option)
        elif self.default_cache is not None:
            self.default_cache.delete_all(step, option=option)

    def clear(self):
        for cache in self.cache_dict.values():
            cache.clear()
        if self.default_cache is not None:
            self.default_cache.clear()
