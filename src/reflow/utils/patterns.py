import logging
import re
from typing import Hashable


_logger = logging.getLogger(__name__)


ElementPattern = str|Hashable|list[Hashable]|callable

def parse_element_pattern(
        pattern: None|ElementPattern, 
        on_empty_element_list:bool=True,
        on_none_element_pattern:bool=False):
    
    _logger.debug(f"Parsing element pattern: {pattern}")
    
    if pattern is None:
        return lambda _: on_none_element_pattern

    if type(pattern) is str:
        return lambda x: re.fullmatch(pattern, str(x))
    
    elif callable(pattern):
        return pattern
    
    elif type(pattern) is list:
        if len(pattern) == 0:
            return lambda _: on_empty_element_list
        else:
            return lambda x: x in pattern
    
    else:
        return lambda x: x == pattern


StepOptionPatternDict = dict[str,ElementPattern]

def parse_step_option_pattern(
        pattern_dict: None|StepOptionPatternDict, 
        # for dict
        on_none_dict:bool=True,
        on_missing_step:bool=True,
        # for option pattern
        on_empty_element_list:bool=True,
        on_none_element_pattern:bool=False,
    ):

    _logger.debug(f"Parsing step option pattern dict: {pattern_dict}")

    if pattern_dict is None:
        return lambda step, option: on_none_dict

    pattern_dict = pattern_dict.copy()
    for step, pattern in pattern_dict.items():
        pattern_dict[step] = parse_element_pattern(
            pattern, 
            on_empty_element_list=on_empty_element_list, 
            on_none_element_pattern=on_none_element_pattern)
    
    def match(step, option):
        if step not in pattern_dict:
            return on_missing_step
        else:
            return pattern_dict[step](option)

    return match


StepOptionPatternList = StepOptionPatternDict|list[StepOptionPatternDict]

def parse_step_option_pattern_list(
        patterns: None|StepOptionPatternList,
        mode:str="any",
        # for list
        on_none_list:bool=True,
        on_empty_list:bool=True,
        # for dict
        on_none_option:bool=True,
        on_missing_step:bool=True,
        # for option pattern
        on_empty_element_list:bool=True,
        on_none_element_pattern:bool=False,
    ):

    _logger.debug(f"Parsing step option pattern list: {patterns}")

    if mode not in ["any", "all"]:
        raise ValueError(f"Invalid value for 'mode': '{mode}'")

    if patterns is None:
        return lambda step, option: on_none_list
    elif len(patterns) == 0:
        return lambda step, option: on_empty_list
    elif type(patterns) is not list:
        patterns = [patterns]

    patterns = [
        parse_step_option_pattern(
            p, 
            on_none_dict=on_none_option,
            on_missing_step=on_missing_step,
            on_empty_element_list=on_empty_element_list,
            on_none_element_pattern=on_none_element_pattern) 
        for p in patterns]

    if mode == "any":
        return lambda s, o: any(p(s, o) for p in patterns)
    elif mode == "all":
        return lambda s, o: all(p(s, o) for p in patterns)
