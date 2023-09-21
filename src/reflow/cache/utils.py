from typing import Hashable, Tuple
from ..recipe import Key, Recipe
from .base import Options


def format_key(step:str, options:Options, include_noop=False):
    formatted_step = format_option(step, options[step])
    formatted_options = format_options(options, include_noop=include_noop)
    return f"""{formatted_step}___{formatted_options}"""

def parse_key(bash_key:str, return_step_option:bool=True) \
        -> Tuple[str, Options]|Tuple[str, str, Options]:
    
    elements = bash_key.split("___")
    step, step_option = parse_option(elements[0])
    options = parse_options("___".join(elements[1:]))
    
    if return_step_option:
        return step, step_option, options
    else:
        return step, options 

def format_options(options:Options, include_noop=False):
        """Format options as a string.

        Parameters
        ----------
        options : Options
            The options.
        include_noop : bool, optional
            Whether to include NOOP options, by default False
        """
        steps = list(sorted(options.keys()))
        options = [format_option(step, options[step]) 
            for step in steps 
            if options[step] != Recipe.NOOP or include_noop]
        return "___".join(options)
    
def format_option(step_name, option_name):
    return f"{step_name}={option_name}"

def parse_options(path_string:str) -> Options:
    return [
        parse_option(b) 
        for b in path_string.split("___")]

def parse_option(option:str) -> (str, str):
    return option.split("=")
