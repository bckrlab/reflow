from ast import parse
from typing import Any, Hashable, Iterable, Tuple

from .utils import format_key, format_options, parse_key, parse_options

from .base import Key, Options
from .base import Cache


class DictCache(Cache):
    """A cache that stores items in a dictionary.
    """

    def __init__(self) -> None:
        super().__init__()
        self.dict = {}

    def set(self, step:str, options: Options, item: Any) -> None:
        key = (step, options)
        self.dict[format_key(step, options)] = (key, item)

    def get(
            self, 
            step:str, 
            options: Options, 
            default:Any=None) -> Any:
        formatted_key = format_key(step, options)
        key, item = self.dict.get(formatted_key, default)
        return item

    def contains(self, step:str, options: Options) -> bool:
        return format_key(step, options) in self.dict
    
    def delete(self, step:str, options: Options) -> None:
        del self.dict[format_key(step, options)]

    def delete_all(
            self, 
            step: str, 
            option: Hashable=None) -> None:
        
        # make sure we match the format of the key (hashable to string)
        if option is not None:
            parsed_option = parse_options(format_options({step: option}))[step]

        for formatted_key in list(self.dict.keys()):
            
            key_step, key_step_option, _ = parse_key(
                formatted_key, return_step_option=True)
            
            if key_step != step:
                continue
            elif option is not None \
                and key_step_option != parsed_option:
                continue
            else:
                del self.dict[formatted_key]

    def keys(self) -> Iterable[Key]:
        return [k for k, _ in self.dict.items()]
    
    def values(self) -> Iterable[Any]:
        return [i for _, i in self.dict.items()]
    
    def items(self) -> Iterable[Tuple[Key, Any]]:
        return self.dict.values()

    def clear(self) -> None:
        self.dict.clear()

